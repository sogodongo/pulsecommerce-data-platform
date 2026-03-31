"""
processing/flink/bronze_writer.py

Flink Job 1 — Bronze Writer
============================
Reads from MSK topic `prod.ecommerce.clickstream.v1` and writes every event
to the Bronze Iceberg table `glue_catalog.bronze.clickstream` in S3 Tables.

Responsibilities:
  - Validate required fields (event_id, user_id, event_ts)
  - Run lightweight Great Expectations checks via side-output routing
  - Route CRITICAL DQ failures to the Dead Letter Queue topic
  - Write all valid (+ WARNING-flagged) events to Bronze Iceberg
  - Preserve the raw JSON payload for future reprocessing

Architecture decisions:
  - Parallelism=24 matches the MSK topic partition count for 1:1 partition alignment
  - Checkpointing every 60s with RocksDB state backend (incremental checkpoints)
  - At-least-once delivery from Kafka; exactly-once dedup happens in Silver via MERGE
  - Iceberg commit interval tied to checkpoint interval (Flink Iceberg sink commits on checkpoint)
  - Watermark: 4-hour bounded out-of-orderness for late mobile events

Deployment: Amazon Managed Service for Apache Flink (Flink 1.19)
  JAR: flink-jobs.zip (PyFlink application)
  KPUs: 10 baseline, autoscale to 50 on CPU > 75%
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone

from pyflink.common import Duration, Row, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import (
    CheckpointingMode,
    OutputTag,
    StreamExecutionEnvironment,
)
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaSource,
)
from pyflink.datastream.functions import MapFunction, ProcessFunction
from pyflink.table import DataTypes, Schema, StreamTableEnvironment
from pyflink.table.confluent import ConfluentSettings  # type: ignore[import]

logger = logging.getLogger(__name__)

# ── Environment config ────────────────────────────────────────────────────────
MSK_BROKERS        = os.environ["MSK_BROKERS"]
SCHEMA_REGISTRY    = os.environ["SCHEMA_REGISTRY_URL"]
SOURCE_TOPIC       = "prod.ecommerce.clickstream.v1"
DLQ_TOPIC          = "prod.ecommerce.clickstream.dlq.v1"
WAREHOUSE          = os.environ.get("LAKEHOUSE_BUCKET", "s3://pulsecommerce-lakehouse-123456789012/")
AWS_REGION         = os.environ.get("AWS_REGION", "us-east-1")
PARALLELISM        = int(os.environ.get("FLINK_PARALLELISM", "24"))
CHECKPOINT_INTERVAL_MS = int(os.environ.get("CHECKPOINT_INTERVAL_MS", "60000"))

# Output tag for DLQ routing (CRITICAL DQ failures)
DLQ_TAG = OutputTag("dlq", Types.STRING())


# ─────────────────────────────────────────────────────────────────────────────
# Schema validation / DQ — runs inline in Flink (no GX Cloud overhead)
# Full GX suite runs post-Silver via Glue job step
# ─────────────────────────────────────────────────────────────────────────────

class DQResult:
    __slots__ = ("flag", "message")

    def __init__(self, flag: str | None, message: str | None = None):
        self.flag = flag        # None | "WARNING" | "CRITICAL"
        self.message = message


def validate_event(event: dict) -> DQResult:
    """
    Lightweight inline DQ checks. Mirrors the critical subset of the
    Great Expectations suite in processing/quality/bronze_clickstream_expectations.py.
    Returns DQResult with flag=None for clean events.
    """
    # ── CRITICAL: required fields ─────────────────────────────────────────────
    if not event.get("event_id"):
        return DQResult("CRITICAL", "event_id is null or empty")
    if not event.get("user_id"):
        return DQResult("CRITICAL", "user_id is null or empty")
    if not event.get("timestamp"):
        return DQResult("CRITICAL", "timestamp is null or empty")

    # ── CRITICAL: valid event_type ────────────────────────────────────────────
    valid_types = {
        "product_view", "add_to_cart", "remove_from_cart",
        "checkout_start", "checkout_complete", "purchase",
        "search", "page_view", "wishlist_add", "wishlist_remove",
    }
    if event.get("event_type") not in valid_types:
        return DQResult("CRITICAL", f"invalid event_type: {event.get('event_type')!r}")

    # ── WARNING: product price range ──────────────────────────────────────────
    product = event.get("product") or {}
    price = product.get("price_usd")
    if price is not None and not (0.01 <= price <= 50_000.0):
        return DQResult("WARNING", f"product.price_usd out of range: {price}")

    # ── WARNING: geo country present ──────────────────────────────────────────
    geo = event.get("geo") or {}
    if not geo.get("country"):
        return DQResult("WARNING", "geo.country is missing")

    return DQResult(None)


# ─────────────────────────────────────────────────────────────────────────────
# Timestamp parsing
# ─────────────────────────────────────────────────────────────────────────────

def parse_event_ts(ts_str: str | None) -> datetime | None:
    """Parse ISO-8601 timestamp string to datetime. Returns None on failure."""
    if not ts_str:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            dt = datetime.strptime(ts_str.rstrip("Z") + "+00:00", fmt.replace("Z", "+00:00"))
            return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt
        except ValueError:
            continue
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Flink ProcessFunction — validates + routes to Iceberg or DLQ
# ─────────────────────────────────────────────────────────────────────────────

class BronzeValidationFunction(ProcessFunction):
    """
    For each raw Kafka JSON message:
      1. Parse JSON
      2. Run inline DQ checks
      3. Emit to main output (Bronze Iceberg sink) if CRITICAL check passes
      4. Emit to DLQ side-output if CRITICAL check fails
      5. Attach dq_flag + dq_failure_msg columns to every record
    """

    def process_element(self, raw_json: str, ctx: ProcessFunction.Context):
        ingested_at = datetime.now(timezone.utc).isoformat()

        # ── Parse JSON ────────────────────────────────────────────────────────
        try:
            event = json.loads(raw_json)
        except (json.JSONDecodeError, TypeError) as exc:
            dlq_payload = json.dumps({
                "raw": raw_json,
                "error": f"JSON parse error: {exc}",
                "ingested_at": ingested_at,
            })
            ctx.output(DLQ_TAG, dlq_payload)
            return

        # ── DQ validation ────────────────────────────────────────────────────
        dq = validate_event(event)

        if dq.flag == "CRITICAL":
            dlq_payload = json.dumps({
                "raw": raw_json,
                "event_id": event.get("event_id"),
                "dq_failure": dq.message,
                "ingested_at": ingested_at,
            })
            ctx.output(DLQ_TAG, dlq_payload)
            return  # do NOT write to Bronze

        # ── Parse timestamps ──────────────────────────────────────────────────
        event_dt = parse_event_ts(event.get("timestamp"))
        if event_dt is None:
            # Fallback: use Kafka ingestion time (event will have dq_flag=WARNING)
            event_dt = datetime.now(timezone.utc)
            dq = DQResult("WARNING", "could not parse event timestamp — using ingestion time")

        # ── Build flattened Bronze record ─────────────────────────────────────
        device  = event.get("device") or {}
        page    = event.get("page") or {}
        product = event.get("product") or {}
        geo     = event.get("geo") or {}
        flags   = event.get("flags") or {}

        bronze_record = {
            "event_id":           event.get("event_id"),
            "user_id":            event.get("user_id"),
            "session_id":         event.get("session_id"),
            "event_type":         event.get("event_type"),
            "event_ts":           event_dt.isoformat(),
            "ingested_at":        ingested_at,
            "kafka_offset":       ctx.timestamp(),       # Kafka record timestamp used as proxy
            "kafka_partition":    None,                  # filled by KafkaSource metadata
            "device_type":        device.get("type"),
            "device_os":          device.get("os"),
            "app_version":        device.get("app_version"),
            "user_agent":         device.get("user_agent"),
            "page_url":           page.get("url"),
            "page_referrer":      page.get("referrer"),
            "page_title":         page.get("title"),
            "product_sku":        product.get("sku"),
            "product_category":   product.get("category"),
            "product_price_usd":  product.get("price_usd"),
            "product_quantity":   product.get("quantity"),
            "geo_country":        geo.get("country"),
            "geo_city":           geo.get("city"),
            "geo_timezone":       geo.get("timezone"),
            "geo_lat":            geo.get("lat"),
            "geo_lon":            geo.get("lon"),
            "is_bot":             flags.get("is_bot", False),
            "is_internal":        flags.get("is_internal", False),
            "ab_cohort":          flags.get("ab_cohort"),
            "search_query":       event.get("search_query"),
            "order_id":           event.get("order_id"),
            "dq_flag":            dq.flag,
            "dq_failure_msg":     dq.message,
            "raw_payload":        raw_json,
            "event_date":         event_dt.date().isoformat(),
            "event_hour":         event_dt.hour,
        }

        yield json.dumps(bronze_record)


# ─────────────────────────────────────────────────────────────────────────────
# DLQ producer (Kafka sink for CRITICAL failures)
# ─────────────────────────────────────────────────────────────────────────────

def build_dlq_sink():
    from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema

    return (
        KafkaSink.builder()
        .set_bootstrap_servers(MSK_BROKERS)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(DLQ_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_property("security.protocol", "SSL")
        .set_property("acks", "all")
        .set_property("retries", "5")
        .build()
    )


# ─────────────────────────────────────────────────────────────────────────────
# Main Flink job graph
# ─────────────────────────────────────────────────────────────────────────────

def build_job():
    # ── Execution environment ─────────────────────────────────────────────────
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(PARALLELISM)
    env.enable_checkpointing(CHECKPOINT_INTERVAL_MS, CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_min_pause_between_checkpoints(CHECKPOINT_INTERVAL_MS // 2)
    env.get_checkpoint_config().set_checkpoint_timeout(300_000)          # 5-min timeout
    env.get_checkpoint_config().set_max_concurrent_checkpoints(1)
    env.get_checkpoint_config().enable_unaligned_checkpoints()           # reduces checkpoint pause

    # RocksDB state backend — incremental checkpoints to S3
    from pyflink.datastream.state_backend import EmbeddedRocksDBStateBackend
    checkpoints_bucket = os.environ.get(
        "FLINK_CHECKPOINTS_BUCKET", "s3://pulsecommerce-flink-checkpoints/"
    )
    env.set_state_backend(EmbeddedRocksDBStateBackend(incremental=True))
    env.get_checkpoint_config().set_checkpoint_storage(
        f"{checkpoints_bucket}bronze-writer/"
    )

    # ── Kafka source ──────────────────────────────────────────────────────────
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(MSK_BROKERS)
        .set_topics(SOURCE_TOPIC)
        .set_group_id("flink-bronze-writer")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .set_property("security.protocol", "SSL")
        .set_property("max.poll.records", "500")
        .set_property("fetch.max.bytes", "52428800")         # 50 MB per poll
        .set_property("isolation.level", "read_committed")  # only read committed offsets
        .build()
    )

    # 4-hour watermark for late mobile events (offline → reconnect scenarios)
    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_hours(4))
        .with_idleness(Duration.of_minutes(5))               # mark partition idle after 5 min no data
    )

    raw_stream = env.from_source(
        source=kafka_source,
        watermark_strategy=watermark_strategy,
        source_name="MSK-clickstream",
    )

    # ── Validation + routing ──────────────────────────────────────────────────
    validated_stream = raw_stream.process(
        BronzeValidationFunction(),
        output_type=Types.STRING(),
    )

    # DLQ side-output → separate Kafka sink
    dlq_stream = validated_stream.get_side_output(DLQ_TAG)
    dlq_stream.sink_to(build_dlq_sink()).name("DLQ-kafka-sink")

    # ── Table API for Iceberg sink ────────────────────────────────────────────
    t_env = StreamTableEnvironment.create(env)

    # Register Iceberg + Glue Catalog
    t_env.execute_sql(f"""
        CREATE CATALOG glue_catalog WITH (
            'type'          = 'iceberg',
            'catalog-type'  = 'glue',
            'warehouse'     = '{WAREHOUSE}',
            'io-impl'       = 'org.apache.iceberg.aws.s3.S3FileIO',
            'glue.region'   = '{AWS_REGION}'
        )
    """)
    t_env.use_catalog("glue_catalog")

    # Register the validated stream as a temporary view
    bronze_table = t_env.from_data_stream(validated_stream)
    t_env.create_temporary_view("validated_events_raw", bronze_table)

    # Parse JSON strings into typed columns and INSERT into Iceberg
    t_env.execute_sql("""
        INSERT INTO glue_catalog.bronze.clickstream
        SELECT
            JSON_VALUE(f0, '$.event_id')            AS event_id,
            JSON_VALUE(f0, '$.user_id')             AS user_id,
            JSON_VALUE(f0, '$.session_id')          AS session_id,
            JSON_VALUE(f0, '$.event_type')          AS event_type,
            TO_TIMESTAMP(JSON_VALUE(f0, '$.event_ts'), 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS+00:00')
                                                    AS event_ts,
            TO_TIMESTAMP(JSON_VALUE(f0, '$.ingested_at'), 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS+00:00')
                                                    AS ingested_at,
            CAST(NULL AS BIGINT)                    AS kafka_offset,
            CAST(NULL AS INT)                       AS kafka_partition,
            JSON_VALUE(f0, '$.device_type')         AS device_type,
            JSON_VALUE(f0, '$.device_os')           AS device_os,
            JSON_VALUE(f0, '$.app_version')         AS app_version,
            JSON_VALUE(f0, '$.user_agent')          AS user_agent,
            JSON_VALUE(f0, '$.page_url')            AS page_url,
            JSON_VALUE(f0, '$.page_referrer')       AS page_referrer,
            JSON_VALUE(f0, '$.page_title')          AS page_title,
            JSON_VALUE(f0, '$.product_sku')         AS product_sku,
            JSON_VALUE(f0, '$.product_category')    AS product_category,
            CAST(JSON_VALUE(f0, '$.product_price_usd') AS DOUBLE)
                                                    AS product_price_usd,
            CAST(JSON_VALUE(f0, '$.product_quantity') AS INT)
                                                    AS product_quantity,
            JSON_VALUE(f0, '$.geo_country')         AS geo_country,
            JSON_VALUE(f0, '$.geo_city')            AS geo_city,
            JSON_VALUE(f0, '$.geo_timezone')        AS geo_timezone,
            CAST(JSON_VALUE(f0, '$.geo_lat') AS DOUBLE)
                                                    AS geo_lat,
            CAST(JSON_VALUE(f0, '$.geo_lon') AS DOUBLE)
                                                    AS geo_lon,
            CAST(JSON_VALUE(f0, '$.is_bot') AS BOOLEAN)
                                                    AS is_bot,
            CAST(JSON_VALUE(f0, '$.is_internal') AS BOOLEAN)
                                                    AS is_internal,
            JSON_VALUE(f0, '$.ab_cohort')           AS ab_cohort,
            JSON_VALUE(f0, '$.search_query')        AS search_query,
            JSON_VALUE(f0, '$.order_id')            AS order_id,
            JSON_VALUE(f0, '$.dq_flag')             AS dq_flag,
            JSON_VALUE(f0, '$.dq_failure_msg')      AS dq_failure_msg,
            JSON_VALUE(f0, '$.raw_payload')         AS raw_payload,
            CAST(JSON_VALUE(f0, '$.event_date') AS DATE)
                                                    AS event_date,
            CAST(JSON_VALUE(f0, '$.event_hour') AS INT)
                                                    AS event_hour
        FROM validated_events_raw
    """)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    logger.info("Starting Flink Bronze Writer job (parallelism=%d)", PARALLELISM)
    build_job()


if __name__ == "__main__":
    main()
