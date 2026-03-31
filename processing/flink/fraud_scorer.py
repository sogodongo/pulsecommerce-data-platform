from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Iterator

import boto3
from pyflink.common import Duration, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import CheckpointingMode, StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.typeinfo import Types

logger = logging.getLogger(__name__)

MSK_BROKERS           = os.environ["MSK_BROKERS"]
SOURCE_TOPIC          = "prod.ecommerce.clickstream.v1"
OUTPUT_TOPIC          = "prod.ecommerce.enriched-events.v1"
SNS_FRAUD_ALERT_ARN   = os.environ.get("SNS_FRAUD_ALERT_ARN", "")
AWS_REGION            = os.environ.get("AWS_REGION", "us-east-1")
PARALLELISM           = int(os.environ.get("FLINK_PARALLELISM", "24"))
CHECKPOINT_INTERVAL   = int(os.environ.get("CHECKPOINT_INTERVAL_MS", "60000"))
FRAUD_SCORE_THRESHOLD = float(os.environ.get("FRAUD_SCORE_THRESHOLD", "0.7"))


def local_hour_from_ts(epoch_ms: int, timezone_str: str | None) -> int:
    try:
        if timezone_str:
            import zoneinfo
            tz = zoneinfo.ZoneInfo(timezone_str)
            dt = datetime.fromtimestamp(epoch_ms / 1000, tz=tz)
        else:
            dt = datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)
        return dt.hour
    except Exception:
        return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc).hour


class FraudScoringFunction(KeyedProcessFunction):
    """
    Stateful per-user fraud scoring keyed by user_id.
    State lives in RocksDB and survives restarts via incremental S3 checkpoints.

    State TTL is handled via cleanup timers (15-min inactivity gap) rather than
    Flink's built-in TTL config — gives us more control over when counters reset
    and avoids the NeverReturnExpired visibility quirk in older Flink versions.
    """

    def open(self, runtime_context):
        self.event_count_state = runtime_context.get_state(
            ValueStateDescriptor("event_count_15m", Types.INT())
        )
        self.last_country_state = runtime_context.get_state(
            ValueStateDescriptor("last_country", Types.STRING())
        )
        self.last_event_ts_state = runtime_context.get_state(
            ValueStateDescriptor("last_event_ts", Types.LONG())
        )
        self.session_start_ts_state = runtime_context.get_state(
            ValueStateDescriptor("session_start_ts", Types.LONG())
        )
        self.product_view_ts_state = runtime_context.get_state(
            ValueStateDescriptor("product_view_ts", Types.LONG())
        )
        # Tracks the last registered cleanup timer so we don't spam RocksDB
        self.cleanup_timer_state = runtime_context.get_state(
            ValueStateDescriptor("cleanup_timer_ts", Types.LONG())
        )
        self._sns = None

    def _get_sns(self):
        if self._sns is None:
            self._sns = boto3.client("sns", region_name=AWS_REGION)
        return self._sns

    def process_element(self, raw_json: str, ctx: KeyedProcessFunction.Context) -> Iterator[str]:
        try:
            event = json.loads(raw_json)
        except (json.JSONDecodeError, TypeError):
            return

        current_ts = ctx.timestamp() or int(datetime.now(timezone.utc).timestamp() * 1000)

        current_count   = self.event_count_state.value() or 0
        last_country    = self.last_country_state.value()
        last_ts         = self.last_event_ts_state.value() or 0
        product_view_ts = self.product_view_ts_state.value() or 0

        event_type   = event.get("event_type", "")
        geo          = event.get("geo") or {}
        country      = geo.get("country", "")
        timezone_str = geo.get("timezone")
        time_delta_s = (current_ts - last_ts) / 1000

        fraud_score   = 0.0
        fraud_signals = []

        # Bots and scripted checkout attacks hit this threshold hard
        if current_count > 20 and time_delta_s < 60:
            fraud_score += 0.4
            fraud_signals.append("VELOCITY_SPIKE")

        # Country change mid-session is a strong stolen-credential signal
        if last_country and country and last_country != country:
            fraud_score += 0.5
            fraud_signals.append("GEO_HOP")

        if event_type == "purchase":
            local_hour = local_hour_from_ts(current_ts, timezone_str)
            if 2 <= local_hour < 4:
                fraud_score += 0.2
                fraud_signals.append("UNUSUAL_HOUR")

        # Legitimate users browse before buying; <30s from view to purchase is suspicious
        if event_type == "purchase" and product_view_ts > 0:
            secs_since_view = (current_ts - product_view_ts) / 1000
            if secs_since_view < 30:
                fraud_score += 0.35
                fraud_signals.append("RAPID_PURCHASE")

        product = event.get("product") or {}
        qty = product.get("quantity") or 0
        if event_type == "add_to_cart" and qty > 50:
            fraud_score += 0.25
            fraud_signals.append("CART_OVERFLOW")

        fraud_score = min(fraud_score, 1.0)

        self.event_count_state.update(current_count + 1)
        if country:
            self.last_country_state.update(country)
        self.last_event_ts_state.update(current_ts)

        if last_ts == 0 or (current_ts - last_ts) > 900_000:
            self.session_start_ts_state.update(current_ts)

        if event_type == "product_view":
            self.product_view_ts_state.update(current_ts)

        # Only push a new timer if it would fire later than the existing one
        cleanup_at = current_ts + 900_000
        last_timer = self.cleanup_timer_state.value() or 0
        if cleanup_at > last_timer:
            ctx.timer_service().register_processing_time_timer(cleanup_at)
            self.cleanup_timer_state.update(cleanup_at)

        enriched = {
            **event,
            "fraud_score": round(fraud_score, 4),
            "fraud_signals": fraud_signals,
            "scored_at": datetime.now(timezone.utc).isoformat(),
        }
        yield json.dumps(enriched)

        if fraud_score >= FRAUD_SCORE_THRESHOLD and SNS_FRAUD_ALERT_ARN:
            self._publish_fraud_alert(event, fraud_score, fraud_signals)

    def on_timer(self, timestamp: int, ctx: KeyedProcessFunction.OnTimerContext) -> Iterator[str]:
        self.event_count_state.clear()
        self.session_start_ts_state.clear()
        self.product_view_ts_state.clear()
        self.cleanup_timer_state.clear()
        # last_country and last_event_ts stay — geo-hop detection should survive idle gaps
        return iter([])

    def _publish_fraud_alert(self, event: dict, score: float, signals: list[str]) -> None:
        try:
            alert = {
                "event_id":      event.get("event_id"),
                "user_id":       event.get("user_id"),
                "order_id":      event.get("order_id"),
                "event_type":    event.get("event_type"),
                "fraud_score":   score,
                "fraud_signals": signals,
                "alerted_at":    datetime.now(timezone.utc).isoformat(),
            }
            self._get_sns().publish(
                TopicArn=SNS_FRAUD_ALERT_ARN,
                Message=json.dumps(alert),
                Subject="FRAUD_ALERT",
                MessageAttributes={
                    "fraud_score": {"DataType": "Number", "StringValue": str(score)},
                    "event_type":  {"DataType": "String",  "StringValue": event.get("event_type", "unknown")},
                },
            )
        except Exception as exc:
            # fail-open: log and move on, don't stall the pipeline
            logger.error("SNS publish failed for event_id=%s: %s", event.get("event_id"), exc)


def build_job():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(PARALLELISM)
    env.enable_checkpointing(CHECKPOINT_INTERVAL, CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_min_pause_between_checkpoints(CHECKPOINT_INTERVAL // 2)

    from pyflink.datastream.state_backend import EmbeddedRocksDBStateBackend
    checkpoints_bucket = os.environ.get("FLINK_CHECKPOINTS_BUCKET", "s3://pulsecommerce-flink-checkpoints/")
    env.set_state_backend(EmbeddedRocksDBStateBackend(incremental=True))
    env.get_checkpoint_config().set_checkpoint_storage(f"{checkpoints_bucket}fraud-scorer/")

    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(MSK_BROKERS)
        .set_topics(SOURCE_TOPIC)
        .set_group_id("flink-fraud-scorer")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .set_property("security.protocol", "SSL")
        .build()
    )

    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_hours(4))
        .with_idleness(Duration.of_minutes(5))
    )

    raw_stream = env.from_source(
        source=kafka_source,
        watermark_strategy=watermark_strategy,
        source_name="MSK-clickstream-fraud",
    )

    enriched_stream = (
        raw_stream
        .map(lambda s: s, output_type=Types.STRING())
        .key_by(lambda s: json.loads(s).get("user_id", ""))
        .process(FraudScoringFunction(), output_type=Types.STRING())
    )

    kafka_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(MSK_BROKERS)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(OUTPUT_TOPIC)
            .set_key_serialization_schema(SimpleStringSchema())
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_property("security.protocol", "SSL")
        .set_property("acks", "all")
        .set_property("enable.idempotence", "true")
        .set_property("compression.type", "lz4")
        .build()
    )

    enriched_stream.sink_to(kafka_sink).name("MSK-enriched-events-sink")
    env.execute("pulsecommerce-fraud-scorer")


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    logger.info("Starting fraud scorer (parallelism=%d, threshold=%.2f)", PARALLELISM, FRAUD_SCORE_THRESHOLD)
    build_job()


if __name__ == "__main__":
    main()
