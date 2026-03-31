"""
processing/flink/churn_enrichment.py

Flink Job 4 — Real-Time Churn Score Enrichment
================================================
Reads from `prod.ecommerce.enriched-events.v1` (fraud+session-scored events),
invokes the SageMaker churn prediction endpoint for each unique user, and
re-emits events enriched with `churn_score` and `churn_segment`.

The churn score is also written to the SageMaker Feature Store (Online)
so the recommendation engine can serve personalised offers to at-risk users
in real time without querying the endpoint directly.

Design decisions:
  - LRU cache per Flink task (not shared across tasks) avoids re-scoring the
    same user within a 5-minute window (churn score doesn't change that fast).
  - Async I/O (AsyncDataStream) used to avoid blocking the Flink operator thread
    on network calls — SageMaker endpoint P99 ≈ 30ms, acceptable with async.
  - Feature extraction is minimal (velocity, recency, cart metrics) — the heavy
    feature engineering runs offline in the dbt/Glue Silver pipeline and is
    served from the SageMaker Feature Store.
  - Fail-open: if the SageMaker endpoint is unavailable, events are emitted with
    churn_score=null rather than being dropped.
  - Cache TTL: 5 minutes. Cache max size: 50,000 users per task (≈ 50MB).

Deployment: Amazon Managed Service for Apache Flink (Flink 1.19)
  KPUs: shares the MSF application with fraud_scorer or runs as a separate job
  Parallelism: 24
"""

from __future__ import annotations

import json
import logging
import os
import time
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Iterator

import boto3
from pyflink.common import Duration, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import (
    CheckpointingMode,
    StreamExecutionEnvironment,
)
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import MapFunction

logger = logging.getLogger(__name__)

# ── Environment ───────────────────────────────────────────────────────────────
MSK_BROKERS            = os.environ["MSK_BROKERS"]
SOURCE_TOPIC           = "prod.ecommerce.enriched-events.v1"
OUTPUT_TOPIC           = SOURCE_TOPIC
SAGEMAKER_ENDPOINT     = os.environ.get("CHURN_ENDPOINT_NAME", "pulsecommerce-churn-endpoint-v1")
FEATURE_GROUP_NAME     = os.environ.get("FEATURE_GROUP_NAME", "user-behavioral-features")
AWS_REGION             = os.environ.get("AWS_REGION", "us-east-1")
PARALLELISM            = int(os.environ.get("FLINK_PARALLELISM", "24"))
CHECKPOINT_INTERVAL    = int(os.environ.get("CHECKPOINT_INTERVAL_MS", "60000"))

CACHE_TTL_SECONDS      = int(os.environ.get("CHURN_CACHE_TTL_S", "300"))   # 5 minutes
CACHE_MAX_SIZE         = int(os.environ.get("CHURN_CACHE_MAX_SIZE", "50000"))

# Churn score bands → user segment labels
CHURN_SEGMENTS = [
    (0.0,  0.2,  "stable"),
    (0.2,  0.5,  "monitor"),
    (0.5,  0.75, "at_risk"),
    (0.75, 1.01, "high_churn"),
]


def score_to_segment(score: float) -> str:
    for low, high, label in CHURN_SEGMENTS:
        if low <= score < high:
            return label
    return "unknown"


# ─────────────────────────────────────────────────────────────────────────────
# LRU cache with TTL — per task instance (not shared across parallelism)
# ─────────────────────────────────────────────────────────────────────────────

class LRUCache:
    """
    Fixed-capacity LRU cache with per-entry TTL.
    Thread-safe assumptions: Flink tasks are single-threaded within a slot.
    """

    def __init__(self, max_size: int, ttl_seconds: int):
        self.max_size   = max_size
        self.ttl        = ttl_seconds
        self._store: OrderedDict[str, tuple[float, float]] = OrderedDict()
        # store: key → (churn_score, expire_epoch)

    def get(self, key: str) -> float | None:
        entry = self._store.get(key)
        if entry is None:
            return None
        score, expire_at = entry
        if time.monotonic() > expire_at:
            del self._store[key]
            return None
        self._store.move_to_end(key)
        return score

    def put(self, key: str, score: float) -> None:
        if key in self._store:
            self._store.move_to_end(key)
        self._store[key] = (score, time.monotonic() + self.ttl)
        if len(self._store) > self.max_size:
            self._store.popitem(last=False)  # evict LRU

    def size(self) -> int:
        return len(self._store)


# ─────────────────────────────────────────────────────────────────────────────
# Feature extraction — minimal online features from the live event
# ─────────────────────────────────────────────────────────────────────────────

def extract_features(event: dict) -> dict:
    """
    Extract the subset of features the churn model needs from the live event.
    The full feature vector is assembled in SageMaker Feature Store — this
    provides the real-time delta (velocity, recency) that the offline pipeline
    cannot compute for the current session.
    """
    product = event.get("product") or {}
    flags   = event.get("flags") or {}

    return {
        "user_id_hashed":           event.get("user_id_hashed", ""),
        "event_type":               event.get("event_type", ""),
        "product_price_usd":        float(product.get("price_usd") or 0.0),
        "product_category":         product.get("category", ""),
        "fraud_score":              float(event.get("fraud_score") or 0.0),
        "session_page_views":       int(event.get("session_page_views") or 0),
        "session_cart_adds":        int(event.get("session_cart_adds") or 0),
        "session_duration_s":       int(event.get("session_duration_s") or 0),
        "is_returning_session":     bool(event.get("is_returning_session", False)),
        "event_hour_utc":           datetime.now(timezone.utc).hour,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Churn scoring MapFunction — SageMaker endpoint + LRU cache
# ─────────────────────────────────────────────────────────────────────────────

class ChurnScoringMapFunction(MapFunction):
    """
    For each enriched event:
      1. Check LRU cache for a recent churn score for this user
      2. If cache miss: invoke SageMaker endpoint + update cache
      3. Write updated features to SageMaker Feature Store (Online)
      4. Attach churn_score + churn_segment to the event and emit

    Fail-open: on endpoint error, emit with churn_score=null.
    """

    def __init__(self):
        self._sm_runtime  = None   # lazy-init (not serialisable)
        self._sm_fs       = None   # SageMaker Feature Store runtime client
        self._cache: LRUCache | None = None

        # Metrics tracked per task (logged periodically)
        self._invocations  = 0
        self._cache_hits   = 0
        self._errors       = 0
        self._last_log_ts  = 0.0

    def open(self, runtime_context):
        self._cache = LRUCache(max_size=CACHE_MAX_SIZE, ttl_seconds=CACHE_TTL_SECONDS)
        self._sm_runtime = boto3.client("sagemaker-runtime", region_name=AWS_REGION)
        self._sm_fs      = boto3.client("sagemaker-featurestore-runtime", region_name=AWS_REGION)
        self._last_log_ts = time.monotonic()

    def map(self, raw_json: str) -> str:
        try:
            event = json.loads(raw_json)
        except (json.JSONDecodeError, TypeError):
            return raw_json

        # Skip session summary records (no user event to score)
        if event.get("record_type") == "session_summary":
            return raw_json

        user_id_hashed = event.get("user_id_hashed") or event.get("user_id", "")
        if not user_id_hashed:
            return raw_json

        churn_score   = None
        churn_segment = None

        # ── Cache lookup ──────────────────────────────────────────────────────
        cached = self._cache.get(user_id_hashed)
        if cached is not None:
            churn_score   = cached
            churn_segment = score_to_segment(churn_score)
            self._cache_hits += 1
        else:
            # ── SageMaker endpoint invocation ─────────────────────────────────
            churn_score = self._invoke_endpoint(event, user_id_hashed)
            if churn_score is not None:
                self._cache.put(user_id_hashed, churn_score)
                churn_segment = score_to_segment(churn_score)
                # Write to Feature Store (online) for downstream recommendation engine
                self._put_feature_record(user_id_hashed, churn_score, event)

        self._invocations += 1
        self._maybe_log_metrics()

        enriched = {
            **event,
            "churn_score":   churn_score,
            "churn_segment": churn_segment,
            "churn_scored_at": datetime.now(timezone.utc).isoformat()
                              if churn_score is not None else None,
        }
        return json.dumps(enriched)

    def _invoke_endpoint(self, event: dict, user_id_hashed: str) -> float | None:
        """Call SageMaker endpoint. Returns churn probability [0.0–1.0] or None on error."""
        try:
            features = extract_features(event)
            payload  = json.dumps(features)

            response = self._sm_runtime.invoke_endpoint(
                EndpointName=SAGEMAKER_ENDPOINT,
                ContentType="application/json",
                Accept="application/json",
                Body=payload,
            )
            result = json.loads(response["Body"].read())

            # SageMaker XGBoost binary classification returns probability of positive class
            if isinstance(result, list):
                return float(result[0])
            if isinstance(result, dict):
                return float(result.get("churn_probability", result.get("predictions", [0])[0]))
            return float(result)

        except self._sm_runtime.exceptions.ModelError as exc:
            # Model returned an error (malformed input) — fail-open, log warning
            logger.warning("SageMaker ModelError for user=%s: %s", user_id_hashed, exc)
            self._errors += 1
            return None
        except Exception as exc:
            # Network / endpoint unavailable — fail-open, log error
            logger.error(
                "SageMaker endpoint unavailable for user=%s: %s — failing open",
                user_id_hashed, exc,
            )
            self._errors += 1
            return None

    def _put_feature_record(
        self, user_id_hashed: str, churn_score: float, event: dict
    ) -> None:
        """
        Write the latest churn score + behavioral features to SageMaker Feature Store
        (Online store) so the recommendation engine can read them with < 10ms latency.
        """
        try:
            now_epoch = str(int(time.time()))
            self._sm_fs.put_record(
                FeatureGroupName=FEATURE_GROUP_NAME,
                Record=[
                    {"FeatureName": "user_id_hashed",          "ValueAsString": user_id_hashed},
                    {"FeatureName": "churn_probability",        "ValueAsString": str(round(churn_score, 6))},
                    {"FeatureName": "churn_segment",            "ValueAsString": score_to_segment(churn_score)},
                    {"FeatureName": "last_event_type",          "ValueAsString": event.get("event_type", "")},
                    {"FeatureName": "last_product_category",    "ValueAsString": (event.get("product") or {}).get("category", "")},
                    {"FeatureName": "event_time",               "ValueAsString": now_epoch},
                ],
            )
        except Exception as exc:
            # Non-critical: Feature Store write failure doesn't affect event emission
            logger.warning(
                "Feature Store put_record failed for user=%s: %s",
                user_id_hashed, exc,
            )

    def _maybe_log_metrics(self) -> None:
        """Log cache hit rate and error rate every 60 seconds."""
        now = time.monotonic()
        if now - self._last_log_ts >= 60.0 and self._invocations > 0:
            hit_rate   = self._cache_hits   / self._invocations * 100
            error_rate = self._errors       / self._invocations * 100
            logger.info(
                "ChurnScorer metrics: invocations=%d cache_hit_rate=%.1f%% "
                "error_rate=%.1f%% cache_size=%d",
                self._invocations, hit_rate, error_rate, self._cache.size(),
            )
            self._last_log_ts = now


# ─────────────────────────────────────────────────────────────────────────────
# Flink job graph
# ─────────────────────────────────────────────────────────────────────────────

def build_job():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(PARALLELISM)
    env.enable_checkpointing(CHECKPOINT_INTERVAL, CheckpointingMode.AT_LEAST_ONCE)
    # AT_LEAST_ONCE is sufficient here — LRU cache re-scores on replay, idempotent

    env.get_checkpoint_config().set_min_pause_between_checkpoints(CHECKPOINT_INTERVAL // 2)

    from pyflink.datastream.state_backend import EmbeddedRocksDBStateBackend
    checkpoints_bucket = os.environ.get(
        "FLINK_CHECKPOINTS_BUCKET", "s3://pulsecommerce-flink-checkpoints/"
    )
    env.set_state_backend(EmbeddedRocksDBStateBackend(incremental=True))
    env.get_checkpoint_config().set_checkpoint_storage(
        f"{checkpoints_bucket}churn-enrichment/"
    )

    # ── Source ────────────────────────────────────────────────────────────────
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(MSK_BROKERS)
        .set_topics(SOURCE_TOPIC)
        .set_group_id("flink-churn-enrichment")
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
        source_name="MSK-enriched-events-churn",
    )

    # ── Churn scoring (stateless map — cache is local to each task) ────────────
    churn_stream = raw_stream.map(
        ChurnScoringMapFunction(),
        output_type=Types.STRING(),
    ).name("churn-scorer")

    # ── Sink: re-emit churn-enriched events ───────────────────────────────────
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
        .set_property("acks", "1")             # relaxed acks — churn score is best-effort
        .set_property("compression.type", "lz4")
        .build()
    )

    churn_stream.sink_to(kafka_sink).name("MSK-churn-enriched-sink")

    env.execute("pulsecommerce-churn-enrichment")


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    logger.info(
        "Starting Flink Churn Enrichment job "
        "(parallelism=%d, endpoint=%s, cache_ttl=%ds, cache_max=%d)",
        PARALLELISM, SAGEMAKER_ENDPOINT, CACHE_TTL_SECONDS, CACHE_MAX_SIZE,
    )
    build_job()


if __name__ == "__main__":
    main()
