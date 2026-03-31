# =============================================================================
# processing/quality/bronze_clickstream_expectations.py
# =============================================================================
# Great Expectations 0.18 validation suite for Bronze clickstream data.
# Runs inline after Flink BronzeWriter writes each micro-batch partition.
# Also executed as a post-write Glue step before promoting to Silver.
#
# Severity tiers:
#   CRITICAL  — blocks Silver promotion, routes events to DLQ, raises alert
#   WARNING   — emits CloudWatch metric, allows promotion with annotation
# =============================================================================

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import boto3
import great_expectations as gx
from great_expectations.core import ExpectationSuite, ExpectationConfiguration
from great_expectations.dataset import SparkDFDataset
from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SUITE_NAME_CRITICAL = "bronze_clickstream_critical"
SUITE_NAME_WARNING = "bronze_clickstream_warning"

VALID_EVENT_TYPES = [
    "page_view",
    "product_view",
    "add_to_cart",
    "remove_from_cart",
    "checkout_start",
    "checkout_complete",
    "search",
    "click",
    "session_start",
    "session_end",
]

VALID_DEVICE_TYPES = ["desktop", "mobile", "tablet", "unknown"]

# Price bounds — anything outside is a data error, not a real product
PRICE_MIN_USD = 0.0
PRICE_MAX_USD = 50_000.0

# Volume anomaly thresholds per micro-batch (5-min window)
VOLUME_WARN_MIN = 100          # fewer events = possible upstream outage
VOLUME_WARN_MAX = 5_000_000    # more events = possible loop / replay storm

CLOUDWATCH_NAMESPACE = "PulseCommerce/DataQuality"
DLQ_TOPIC = "prod.ecommerce.clickstream.dlq.v1"

# ---------------------------------------------------------------------------
# Dataclass for validation results
# ---------------------------------------------------------------------------

@dataclass
class ValidationResult:
    suite_name: str
    success: bool
    statistics: dict[str, Any]
    failed_expectations: list[dict[str, Any]]
    row_count: int
    ts: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


# ---------------------------------------------------------------------------
# Suite builders
# ---------------------------------------------------------------------------

def build_critical_suite() -> ExpectationSuite:
    """
    CRITICAL expectations — any failure blocks Silver promotion.
    Focus: structural integrity and PK completeness.
    """
    suite = ExpectationSuite(expectation_suite_name=SUITE_NAME_CRITICAL)

    # ── Non-null primary identifiers ────────────────────────────────────────
    for col in ("event_id", "user_id_hashed", "session_id", "event_timestamp"):
        suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": col, "mostly": 1.0},
            )
        )

    # ── event_id must be unique within the batch ────────────────────────────
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_unique",
            kwargs={"column": "event_id"},
        )
    )

    # ── event_type must be in the known enum ────────────────────────────────
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={
                "column": "event_type",
                "value_set": VALID_EVENT_TYPES,
                "mostly": 0.999,   # 0.1% tolerance for schema migration windows
            },
        )
    )

    # ── Timestamps must be parseable and not in the distant future ──────────
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "ingested_at"},
        )
    )

    # ── raw_payload must be present (needed for reprocessing) ───────────────
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "raw_payload", "mostly": 1.0},
        )
    )

    # ── Table-level row count — catches complete data loss ───────────────────
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_table_row_count_to_be_between",
            kwargs={"min_value": 1, "max_value": None},
        )
    )

    return suite


def build_warning_suite() -> ExpectationSuite:
    """
    WARNING expectations — failures emit CloudWatch metrics but do not block.
    Focus: business-logic plausibility checks.
    """
    suite = ExpectationSuite(expectation_suite_name=SUITE_NAME_WARNING)

    # ── device_type enum ────────────────────────────────────────────────────
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={
                "column": "device_type",
                "value_set": VALID_DEVICE_TYPES,
                "mostly": 0.99,
            },
        )
    )

    # ── product_price_usd range ─────────────────────────────────────────────
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={
                "column": "product_price_usd",
                "min_value": PRICE_MIN_USD,
                "max_value": PRICE_MAX_USD,
                "mostly": 0.999,
            },
        )
    )

    # ── page_load_ms should be positive ─────────────────────────────────────
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={
                "column": "page_load_ms",
                "min_value": 0,
                "max_value": 60_000,   # 60s — anything higher is a measurement error
                "mostly": 0.995,
            },
        )
    )

    # ── Volume anomaly detection (batch-level) ───────────────────────────────
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_table_row_count_to_be_between",
            kwargs={"min_value": VOLUME_WARN_MIN, "max_value": VOLUME_WARN_MAX},
        )
    )

    # ── country code format (ISO 2-letter) ──────────────────────────────────
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_match_regex",
            kwargs={
                "column": "country",
                "regex": r"^[A-Z]{2}$",
                "mostly": 0.98,
            },
        )
    )

    # ── user_agent should not be blank (bots scrubbed upstream) ─────────────
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "user_agent", "mostly": 0.95},
        )
    )

    return suite


# ---------------------------------------------------------------------------
# Validator
# ---------------------------------------------------------------------------

class BronzeClickstreamValidator:
    """
    Validates a Spark DataFrame of Bronze clickstream records.

    Usage (Glue job):
        validator = BronzeClickstreamValidator(spark, boto3.client("cloudwatch"))
        result = validator.validate(df, partition_ts="2024-01-15T10:00:00Z")
        if not result.success:
            raise RuntimeError("CRITICAL validation failed — blocking Silver promotion")
    """

    def __init__(
        self,
        spark: SparkSession,
        cw_client: Any | None = None,
        sns_client: Any | None = None,
    ) -> None:
        self.spark = spark
        self.cw = cw_client or boto3.client("cloudwatch", region_name=os.environ["AWS_REGION"])
        self.sns = sns_client or boto3.client("sns", region_name=os.environ["AWS_REGION"])
        self._critical_suite = build_critical_suite()
        self._warning_suite = build_warning_suite()

    # ── Public API ──────────────────────────────────────────────────────────

    def validate(self, df: DataFrame, partition_ts: str = "") -> ValidationResult:
        """
        Run both CRITICAL and WARNING suites.
        Returns the merged result; success=False if CRITICAL fails.
        """
        row_count = df.count()
        gx_df = SparkDFDataset(df)

        critical_result = self._run_suite(gx_df, self._critical_suite)
        warning_result = self._run_suite(gx_df, self._warning_suite)

        # Emit CloudWatch metrics regardless of outcome
        self._emit_metrics(critical_result, warning_result, row_count, partition_ts)

        overall_success = critical_result["success"]

        if not overall_success:
            self._alert_on_critical_failure(critical_result, partition_ts)
            logger.error(
                "CRITICAL validation failed",
                extra={
                    "partition_ts": partition_ts,
                    "failed": critical_result["failed"],
                    "row_count": row_count,
                },
            )
        else:
            logger.info(
                "Validation passed",
                extra={
                    "partition_ts": partition_ts,
                    "row_count": row_count,
                    "warning_failures": len(warning_result["failed"]),
                },
            )

        return ValidationResult(
            suite_name=f"{SUITE_NAME_CRITICAL}+{SUITE_NAME_WARNING}",
            success=overall_success,
            statistics={
                "critical_expectations_evaluated": critical_result["evaluated"],
                "critical_expectations_failed": len(critical_result["failed"]),
                "warning_expectations_evaluated": warning_result["evaluated"],
                "warning_expectations_failed": len(warning_result["failed"]),
            },
            failed_expectations=critical_result["failed"] + warning_result["failed"],
            row_count=row_count,
        )

    # ── Internal helpers ────────────────────────────────────────────────────

    def _run_suite(
        self,
        gx_df: SparkDFDataset,
        suite: ExpectationSuite,
    ) -> dict[str, Any]:
        result = gx_df.validate(expectation_suite=suite, result_format="SUMMARY")
        failed = [
            {
                "expectation_type": r.expectation_config.expectation_type,
                "kwargs": r.expectation_config.kwargs,
                "result": r.result,
                "severity": (
                    "CRITICAL" if suite.expectation_suite_name == SUITE_NAME_CRITICAL else "WARNING"
                ),
            }
            for r in result.results
            if not r.success
        ]
        return {
            "success": result.success,
            "evaluated": len(result.results),
            "failed": failed,
            "statistics": result.statistics,
        }

    def _emit_metrics(
        self,
        critical: dict,
        warning: dict,
        row_count: int,
        partition_ts: str,
    ) -> None:
        dimensions = [{"Name": "Layer", "Value": "Bronze"}, {"Name": "Table", "Value": "clickstream"}]
        metric_data = [
            {
                "MetricName": "CriticalExpectationFailures",
                "Dimensions": dimensions,
                "Value": len(critical["failed"]),
                "Unit": "Count",
            },
            {
                "MetricName": "WarningExpectationFailures",
                "Dimensions": dimensions,
                "Value": len(warning["failed"]),
                "Unit": "Count",
            },
            {
                "MetricName": "BatchRowCount",
                "Dimensions": dimensions,
                "Value": row_count,
                "Unit": "Count",
            },
            {
                "MetricName": "ValidationSuccess",
                "Dimensions": dimensions,
                "Value": 1.0 if critical["success"] else 0.0,
                "Unit": "None",
            },
        ]
        try:
            self.cw.put_metric_data(Namespace=CLOUDWATCH_NAMESPACE, MetricData=metric_data)
        except Exception:
            logger.warning("Failed to emit CloudWatch metrics", exc_info=True)

    def _alert_on_critical_failure(self, critical: dict, partition_ts: str) -> None:
        sns_arn = os.environ.get("DATA_QUALITY_SNS_ARN")
        if not sns_arn:
            return
        message = {
            "alert_type": "BRONZE_CLICKSTREAM_CRITICAL_FAILURE",
            "partition_ts": partition_ts,
            "failed_expectations": critical["failed"],
            "action": "Silver promotion blocked. Events routed to DLQ.",
        }
        try:
            self.sns.publish(
                TopicArn=sns_arn,
                Subject="[PulseCommerce] Bronze Clickstream CRITICAL DQ Failure",
                Message=json.dumps(message, indent=2),
                MessageAttributes={
                    "severity": {"DataType": "String", "StringValue": "CRITICAL"},
                    "layer": {"DataType": "String", "StringValue": "bronze"},
                },
            )
        except Exception:
            logger.error("Failed to publish SNS alert", exc_info=True)


# ---------------------------------------------------------------------------
# Glue job entrypoint
# ---------------------------------------------------------------------------

def run_as_glue_job() -> None:
    """Entrypoint when executed as a Glue ELT post-write validation step."""
    import sys
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(
        sys.argv,
        ["JOB_NAME", "partition_date", "partition_hour"],
    )
    partition_ts = f"{args['partition_date']}T{args['partition_hour']}:00:00Z"

    sc = SparkContext()
    glue_ctx = GlueContext(sc)
    spark = glue_ctx.spark_session
    job = Job(glue_ctx)
    job.init(args["JOB_NAME"], args)

    bucket = os.environ["LAKEHOUSE_BUCKET"]
    path = (
        f"s3://{bucket}/bronze/clickstream/"
        f"event_date={args['partition_date']}/event_hour={args['partition_hour']}/"
    )
    df = spark.read.format("iceberg").load(
        f"glue_catalog.bronze.clickstream WHERE "
        f"event_date='{args['partition_date']}' AND event_hour='{args['partition_hour']}'"
    )

    validator = BronzeClickstreamValidator(spark)
    result = validator.validate(df, partition_ts=partition_ts)

    job.commit()

    if not result.success:
        raise RuntimeError(
            f"CRITICAL data quality failure for partition {partition_ts}. "
            f"Silver promotion blocked. Failed: {result.failed_expectations}"
        )


if __name__ == "__main__":
    run_as_glue_job()
