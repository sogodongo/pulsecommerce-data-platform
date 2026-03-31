from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Iterator

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
from pyflink.datastream.state import ListStateDescriptor, ValueStateDescriptor
from pyflink.common.typeinfo import Types
from pyflink.table import StreamTableEnvironment

logger = logging.getLogger(__name__)

MSK_BROKERS         = os.environ["MSK_BROKERS"]
SOURCE_TOPIC        = "prod.ecommerce.enriched-events.v1"
OUTPUT_TOPIC        = SOURCE_TOPIC
WAREHOUSE           = os.environ.get("LAKEHOUSE_BUCKET", "s3://pulsecommerce-lakehouse-123456789012/")
AWS_REGION          = os.environ.get("AWS_REGION", "us-east-1")
PARALLELISM         = int(os.environ.get("FLINK_PARALLELISM", "24"))
CHECKPOINT_INTERVAL = int(os.environ.get("CHECKPOINT_INTERVAL_MS", "60000"))

SESSION_GAP_MS = 15 * 60 * 1000

# Maps event_type to funnel depth. remove_from_cart/wishlist_add share the add_to_cart level
# intentionally — they indicate engagement but not forward progress.
FUNNEL_ORDER = {
    "page_view":         0,
    "search":            1,
    "product_view":      2,
    "add_to_cart":       3,
    "remove_from_cart":  3,
    "wishlist_add":      3,
    "checkout_start":    4,
    "checkout_complete": 5,
    "purchase":          5,
}
_DEPTH_TO_STAGE = {0: "browse", 1: "browse", 2: "product_view", 3: "add_to_cart", 4: "checkout_start", 5: "purchase"}


def compute_session_metrics(events: list[dict]) -> dict:
    if not events:
        return {}

    events_sorted = sorted(events, key=lambda e: e.get("event_ts_ms", 0))
    first = events_sorted[0]
    last  = events_sorted[-1]

    start_ts = first.get("event_ts_ms", 0)
    end_ts   = last.get("event_ts_ms", 0)
    duration = max(0, (end_ts - start_ts) // 1000)

    page_views    = sum(1 for e in events if e.get("event_type") == "page_view")
    product_views = sum(1 for e in events if e.get("event_type") == "product_view")
    cart_adds     = sum(1 for e in events if e.get("event_type") == "add_to_cart")
    cart_removes  = sum(1 for e in events if e.get("event_type") == "remove_from_cart")
    searches      = sum(1 for e in events if e.get("event_type") == "search")
    checkout_att  = sum(1 for e in events if e.get("event_type") == "checkout_start")
    purchases     = sum(1 for e in events if e.get("event_type") == "purchase")

    max_depth  = max((FUNNEL_ORDER.get(e.get("event_type", ""), 0) for e in events), default=0)
    funnel_stage = _DEPTH_TO_STAGE.get(max_depth, "browse")

    revenue = sum(
        float(e.get("product", {}).get("price_usd", 0) or 0)
        for e in events if e.get("event_type") == "purchase"
    )
    cart_value = sum(
        float(e.get("product", {}).get("price_usd", 0) or 0)
        * int(e.get("product", {}).get("quantity", 1) or 1)
        for e in events if e.get("event_type") == "add_to_cart"
    ) - sum(
        float(e.get("product", {}).get("price_usd", 0) or 0)
        for e in events if e.get("event_type") == "remove_from_cart"
    )

    fraud_scores = [float(e.get("fraud_score", 0.0) or 0.0) for e in events]

    first_device = first.get("device") or {}
    first_geo    = first.get("geo") or {}
    first_flags  = first.get("flags") or {}

    return {
        "session_start_ts":       datetime.fromtimestamp(start_ts / 1000, tz=timezone.utc).isoformat(),
        "session_end_ts":         datetime.fromtimestamp(end_ts   / 1000, tz=timezone.utc).isoformat(),
        "session_duration_s":     duration,
        "funnel_stage_reached":   funnel_stage,
        "page_views":             page_views,
        "product_views":          product_views,
        "cart_adds":              cart_adds,
        "cart_removes":           cart_removes,
        "searches":               searches,
        "checkout_attempts":      checkout_att,
        "purchases":              purchases,
        "revenue_attributed_usd": round(revenue, 4),
        "cart_value_usd":         round(max(cart_value, 0.0), 4),
        "cart_abandonment":       cart_adds > 0 and purchases == 0,
        "entry_page_url":         (first.get("page") or {}).get("url"),
        "exit_page_url":          (last.get("page") or {}).get("url"),
        "entry_referrer":         (first.get("page") or {}).get("referrer"),
        "device_type":            first_device.get("type"),
        "device_os":              first_device.get("os"),
        "geo_country":            first_geo.get("country"),
        "ab_cohort":              first_flags.get("ab_cohort"),
        "max_fraud_score":        round(max(fraud_scores) if fraud_scores else 0.0, 4),
        "fraud_flagged":          max(fraud_scores, default=0.0) >= 0.7,
        "event_count":            len(events),
    }


class SessionStitcherFunction(KeyedProcessFunction):
    """
    Accumulates events into per-user session buffers and emits a session summary
    when the inactivity gap timer fires.

    The gap timer slides forward on every new event. If no event arrives within
    SESSION_GAP_MS, the timer fires and we close the session. State is cleared
    immediately after emit so the next event starts a fresh session.
    """

    def open(self, runtime_context):
        self.session_events_state = runtime_context.get_list_state(
            ListStateDescriptor("session_events", Types.STRING())
        )
        self.session_id_state = runtime_context.get_state(
            ValueStateDescriptor("session_id", Types.STRING())
        )
        self.last_event_ts_state = runtime_context.get_state(
            ValueStateDescriptor("last_event_ts", Types.LONG())
        )
        self.gap_timer_ts_state = runtime_context.get_state(
            ValueStateDescriptor("gap_timer_ts", Types.LONG())
        )

    def process_element(self, raw_json: str, ctx: KeyedProcessFunction.Context) -> Iterator[str]:
        try:
            event = json.loads(raw_json)
        except (json.JSONDecodeError, TypeError):
            return

        current_ts = ctx.timestamp() or int(datetime.now(timezone.utc).timestamp() * 1000)
        event["event_ts_ms"] = current_ts

        if not self.session_id_state.value():
            self.session_id_state.update(str(uuid.uuid4()))

        self.session_events_state.add(json.dumps(event))
        self.last_event_ts_state.update(current_ts)

        session_id = self.session_id_state.value()
        yield json.dumps({**event, "session_id": session_id})

        gap_fire_ts = current_ts + SESSION_GAP_MS
        old_timer   = self.gap_timer_ts_state.value() or 0

        if gap_fire_ts > old_timer:
            if old_timer > 0:
                ctx.timer_service().delete_processing_time_timer(old_timer)
            ctx.timer_service().register_processing_time_timer(gap_fire_ts)
            self.gap_timer_ts_state.update(gap_fire_ts)

    def on_timer(self, timestamp: int, ctx: KeyedProcessFunction.OnTimerContext) -> Iterator[str]:
        # Guard against stale timers from events that arrived while a timer was already pending
        if timestamp < (self.gap_timer_ts_state.value() or 0):
            return

        buffered = list(self.session_events_state.get())
        if not buffered:
            self._clear_state()
            return

        events = []
        for raw in buffered:
            try:
                events.append(json.loads(raw))
            except (json.JSONDecodeError, TypeError):
                pass

        if not events:
            self._clear_state()
            return

        session_id     = self.session_id_state.value() or str(uuid.uuid4())
        user_id        = events[0].get("user_id", ctx.get_current_key())
        user_id_hashed = events[0].get("user_id_hashed", user_id)
        metrics        = compute_session_metrics(events)

        logger.info(
            "session closed: id=%s user=%s events=%d funnel=%s revenue=%.2f",
            session_id, user_id, len(events),
            metrics.get("funnel_stage_reached"),
            metrics.get("revenue_attributed_usd", 0.0),
        )

        yield json.dumps({
            "record_type":    "session_summary",
            "session_id":     session_id,
            "user_id":        user_id,
            "user_id_hashed": user_id_hashed,
            "processed_at":   datetime.now(timezone.utc).isoformat(),
            **metrics,
        })

        self._clear_state()

    def _clear_state(self):
        self.session_events_state.clear()
        self.session_id_state.clear()
        self.last_event_ts_state.clear()
        self.gap_timer_ts_state.clear()


def build_job():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(PARALLELISM)
    env.enable_checkpointing(CHECKPOINT_INTERVAL, CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_min_pause_between_checkpoints(CHECKPOINT_INTERVAL // 2)
    env.get_checkpoint_config().set_checkpoint_timeout(300_000)

    from pyflink.datastream.state_backend import EmbeddedRocksDBStateBackend
    checkpoints_bucket = os.environ.get("FLINK_CHECKPOINTS_BUCKET", "s3://pulsecommerce-flink-checkpoints/")
    env.set_state_backend(EmbeddedRocksDBStateBackend(incremental=True))
    env.get_checkpoint_config().set_checkpoint_storage(f"{checkpoints_bucket}session-stitcher/")

    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(MSK_BROKERS)
        .set_topics(SOURCE_TOPIC)
        .set_group_id("flink-session-stitcher")
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
        source_name="MSK-enriched-events",
    )

    stitched_stream = (
        raw_stream
        .key_by(lambda s: json.loads(s).get("user_id", ""))
        .process(SessionStitcherFunction(), output_type=Types.STRING())
    )

    kafka_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(MSK_BROKERS)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(OUTPUT_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_property("security.protocol", "SSL")
        .set_property("acks", "all")
        .set_property("compression.type", "lz4")
        .build()
    )
    stitched_stream.sink_to(kafka_sink).name("MSK-session-enriched-sink")

    t_env = StreamTableEnvironment.create(env)
    t_env.execute_sql(f"""
        CREATE CATALOG glue_catalog WITH (
            'type'         = 'iceberg',
            'catalog-type' = 'glue',
            'warehouse'    = '{WAREHOUSE}',
            'io-impl'      = 'org.apache.iceberg.aws.s3.S3FileIO',
            'glue.region'  = '{AWS_REGION}'
        )
    """)

    session_summary_stream = stitched_stream.filter(
        lambda s: json.loads(s).get("record_type") == "session_summary"
    )
    t_env.from_data_stream(session_summary_stream).execute_insert("glue_catalog.silver.user_sessions")

    env.execute("pulsecommerce-session-stitcher")


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    logger.info("Starting session stitcher (parallelism=%d, gap=%ds)", PARALLELISM, SESSION_GAP_MS // 1000)
    build_job()


if __name__ == "__main__":
    main()
