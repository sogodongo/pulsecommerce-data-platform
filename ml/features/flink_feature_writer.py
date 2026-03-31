from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from typing import Any

import boto3
from pyflink.common import Types
from pyflink.datastream import MapFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor

logger = logging.getLogger(__name__)

FEATURE_GROUP_NAME = os.environ.get("FEATURE_GROUP_NAME", "pulsecommerce-user-behavioral")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

BATCH_SIZE = 50
MAX_BUFFER_MS = 5_000

# Features to write per event type — keeps PutRecord payload small
ORDER_EVENT_FEATURES = [
    "days_since_last_order",
    "order_count_30d",
    "order_count_90d",
    "order_frequency_30d",
    "avg_order_value_usd",
    "total_ltv_usd",
    "max_order_value_usd",
    "discount_usage_rate",
    "avg_fraud_score",
    "refund_count_90d",
    "churned_30d",
]

SESSION_EVENT_FEATURES = [
    "days_since_last_session",
    "session_count_7d",
    "session_count_30d",
    "cart_abandonment_rate",
    "avg_session_duration_s",
    "avg_pages_per_session",
    "product_view_count_7d",
]

# Written on first encounter — these rarely change between events
STATIC_FEATURES = [
    "preferred_category_encoded",
    "channel_group_encoded",
    "is_gdpr_scope",
]


def build_feature_record(event: dict[str, Any], event_type: str) -> list[dict[str, str]]:
    """
    Build a Feature Store PutRecord payload from a processed event.
    event must have user_id_hashed + relevant counters from upstream Flink operators.
    """
    ts = event.get("event_timestamp") or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    record: list[dict[str, str]] = [
        {"FeatureName": "user_id_hashed", "ValueAsString": event["user_id_hashed"]},
        {"FeatureName": "feature_timestamp", "ValueAsString": str(ts)},
        {"FeatureName": "is_current", "ValueAsString": "1"},
    ]

    if event_type == "order":
        for feat in ORDER_EVENT_FEATURES:
            val = event.get(feat)
            if val is not None:
                record.append({"FeatureName": feat, "ValueAsString": str(val)})
    elif event_type == "session":
        for feat in SESSION_EVENT_FEATURES:
            val = event.get(feat)
            if val is not None:
                record.append({"FeatureName": feat, "ValueAsString": str(val)})

    for feat in STATIC_FEATURES:
        val = event.get(feat)
        if val is not None:
            record.append({"FeatureName": feat, "ValueAsString": str(val)})

    return record


def _record_fingerprint(record: list[dict[str, str]]) -> str:
    sorted_items = sorted((r["FeatureName"], r["ValueAsString"]) for r in record)
    return hashlib.sha256(json.dumps(sorted_items).encode()).hexdigest()


class FeatureStoreWriterFunction(MapFunction):
    """
    Writes enriched events to SageMaker Feature Store Online as a side-effect.
    Keyed by user_id_hashed upstream so deduplication state is per-user per slot.
    Skips writes when the feature fingerprint hasn't changed within MAX_BUFFER_MS.
    Pass-through: returns the input event unchanged.
    """

    def __init__(self, event_type: str = "order") -> None:
        self.event_type = event_type
        self._sm_client: Any = None
        self._last_fingerprint_state: Any = None
        self._last_write_ts_state: Any = None

    def open(self, runtime_context: RuntimeContext) -> None:
        self._sm_client = boto3.client(
            "sagemaker-featurestore-runtime", region_name=AWS_REGION
        )
        self._last_fingerprint_state = runtime_context.get_state(
            ValueStateDescriptor("last_feature_fingerprint", Types.STRING())
        )
        self._last_write_ts_state = runtime_context.get_state(
            ValueStateDescriptor("last_feature_write_ts", Types.LONG())
        )

    def map(self, event: dict[str, Any]) -> dict[str, Any]:
        try:
            self._maybe_write(event)
        except Exception:
            logger.warning(
                "FeatureStore write failed for user %s",
                event.get("user_id_hashed"),
                exc_info=True,
            )
        return event

    def _maybe_write(self, event: dict[str, Any]) -> None:
        record = build_feature_record(event, self.event_type)
        fingerprint = _record_fingerprint(record)

        last_fp = self._last_fingerprint_state.value()
        last_ts = self._last_write_ts_state.value() or 0
        now_ms = int(time.time() * 1000)

        if last_fp == fingerprint and (now_ms - last_ts) < MAX_BUFFER_MS:
            return

        self._sm_client.put_record(
            FeatureGroupName=FEATURE_GROUP_NAME,
            Record=record,
        )
        self._last_fingerprint_state.update(fingerprint)
        self._last_write_ts_state.update(now_ms)

        logger.debug(
            "Feature Store write: user=%s, features=%d",
            event.get("user_id_hashed"),
            len(record),
        )


class BatchFeatureStoreWriter:
    """Synchronous batch writer for Glue backfills and tests."""

    def __init__(
        self,
        feature_group_name: str = FEATURE_GROUP_NAME,
        batch_size: int = BATCH_SIZE,
        sm_client: Any | None = None,
    ) -> None:
        self.feature_group_name = feature_group_name
        self.batch_size = batch_size
        self._client = sm_client or boto3.client(
            "sagemaker-featurestore-runtime", region_name=AWS_REGION
        )
        self._buffer: list[list[dict[str, str]]] = []
        self._success = 0
        self._failed = 0

    def add(self, event: dict[str, Any], event_type: str = "order") -> None:
        record = build_feature_record(event, event_type)
        self._buffer.append(record)
        if len(self._buffer) >= self.batch_size:
            self.flush()

    def flush(self) -> None:
        for record in self._buffer:
            try:
                self._client.put_record(
                    FeatureGroupName=self.feature_group_name,
                    Record=record,
                )
                self._success += 1
            except Exception as exc:
                logger.warning("PutRecord failed: %s", exc)
                self._failed += 1
        self._buffer.clear()

    def close(self) -> dict[str, int]:
        self.flush()
        return {"success": self._success, "failed": self._failed}

    @property
    def stats(self) -> dict[str, int]:
        return {"success": self._success, "failed": self._failed, "buffered": len(self._buffer)}
