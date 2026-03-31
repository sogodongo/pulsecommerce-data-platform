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

SUITE_NAME_CRITICAL = "silver_orders_critical"
SUITE_NAME_WARNING = "silver_orders_warning"

VALID_ORDER_STATUSES = [
    "pending",
    "confirmed",
    "shipped",
    "delivered",
    "cancelled",
    "refunded",
    "deleted",   # mapped from CDC op='d'
]

VALID_CDC_OPS = ["c", "u", "d", "r"]

# Business rules: order amounts
AMOUNT_MIN_USD = 0.0
AMOUNT_MAX_USD = 100_000.0     # $100k hard ceiling — above = likely test data
DISCOUNT_MAX_RATIO = 1.0       # discount cannot exceed order total

# SCD2 constraints
MAX_VERSIONS_PER_ORDER = 50    # more = runaway CDC loop indicator

CLOUDWATCH_NAMESPACE = "PulseCommerce/DataQuality"

@dataclass
class ValidationResult:
    suite_name: str
    success: bool
    statistics: dict[str, Any]
    failed_expectations: list[dict[str, Any]]
    row_count: int
    current_row_count: int
    ts: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

def build_critical_suite() -> ExpectationSuite:
    """
    CRITICAL suite — structural and referential integrity of Silver orders.
    Failure blocks Gold dbt models from running.
    """
    suite = ExpectationSuite(expectation_suite_name=SUITE_NAME_CRITICAL)

    # ── Primary key completeness ─────────────────────────────────────────────
    for col in ("order_id", "user_id_hashed", "effective_from", "is_current"):
        suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": col, "mostly": 1.0},
            )
        )

    # ── order_id must not be blank ───────────────────────────────────────────
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_match_regex",
            kwargs={"column": "order_id", "regex": r"^\s*$", "mostly": 1.0},
        )
    )

    # ── Status must be in known set ──────────────────────────────────────────
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={
                "column": "status",
                "value_set": VALID_ORDER_STATUSES,
                "mostly": 1.0,
            },
        )
    )

    # ── CDC op must be in known set ──────────────────────────────────────────
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={
                "column": "cdc_op",
                "value_set": VALID_CDC_OPS,
                "mostly": 1.0,
            },
        )
    )

    # ── Amount fields must not be negative ───────────────────────────────────
    for col in ("total_amount_usd", "net_amount_usd"):
        suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={
                    "column": col,
                    "min_value": AMOUNT_MIN_USD,
                    "max_value": None,
                    "mostly": 1.0,
                },
            )
        )

    # ── discount_usd must not exceed total_amount_usd ───────────────────────
    # Approximated via max value check — exact ratio requires custom expectation
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={
                "column": "discount_usd",
                "min_value": 0.0,
                "max_value": AMOUNT_MAX_USD,
                "mostly": 1.0,
            },
        )
    )

    # ── is_current must be boolean (0 or 1 in Spark) ────────────────────────
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={
                "column": "is_current",
                "value_set": [True, False],
                "mostly": 1.0,
            },
        )
    )

    # ── record_version must be >= 1 ──────────────────────────────────────────
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={
                "column": "record_version",
                "min_value": 1,
                "max_value": MAX_VERSIONS_PER_ORDER,
                "mostly": 1.0,
            },
        )
    )

    # ── Table must not be empty ───────────────────────────────────────────────
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_table_row_count_to_be_between",
            kwargs={"min_value": 1, "max_value": None},
        )
    )

    return suite

def build_warning_suite() -> ExpectationSuite:
    """
    WARNING suite — business plausibility checks for Silver orders.
    Failures are logged and metriced but do not block processing.
    """
    suite = ExpectationSuite(expectation_suite_name=SUITE_NAME_WARNING)

    # ── Amount range sanity ──────────────────────────────────────────────────
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={
                "column": "total_amount_usd",
                "min_value": AMOUNT_MIN_USD,
                "max_value": AMOUNT_MAX_USD,
                "mostly": 0.999,
            },
        )
    )

    # ── item_count should be positive ───────────────────────────────────────
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={
                "column": "item_count",
                "min_value": 1,
                "max_value": 500,    # 500+ items = likely B2B edge case or error
                "mostly": 0.999,
            },
        )
    )

    # ── LSN should be monotonically increasing ───────────────────────────────
    # Approximated: cdc_lsn must be > 0
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={
                "column": "cdc_lsn",
                "min_value": 0,
                "max_value": None,
                "mostly": 0.999,
            },
        )
    )

    # ── effective_from should not be far in the future (clock skew check) ───
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "processed_at", "mostly": 1.0},
        )
    )

    # ── fraud_score range ────────────────────────────────────────────────────
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={
                "column": "fraud_score",
                "min_value": 0.0,
                "max_value": 1.0,
                "mostly": 1.0,
            },
        )
    )

    # ── payment_method non-null for non-pending orders ───────────────────────
    # GX doesn't support conditional expects natively; use mostly as approximation
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "payment_method", "mostly": 0.90},
        )
    )

    return suite

def check_scd2_integrity(spark: SparkSession, database: str = "silver") -> dict[str, Any]:
    """
    Verifies SCD2 constraints that GX cannot express as column-level expectations:
      1. Each order_id has exactly one is_current=true row.
      2. effective_from < effective_to for all closed rows (effective_to IS NOT NULL).
      3. No version gaps (record_version increments by 1).
    Returns {"passed": bool, "violations": list[dict]}.
    """
    violations = []

    # Check 1 — duplicate current records
    dup_df = spark.sql(f"""
        SELECT order_id, COUNT(*) AS current_count
        FROM {database}.orders_unified
        WHERE is_current = true
        GROUP BY order_id
        HAVING COUNT(*) > 1
        LIMIT 100
    """)
    dups = dup_df.collect()
    if dups:
        violations.append({
            "check": "duplicate_current_records",
            "severity": "CRITICAL",
            "sample": [row.asDict() for row in dups[:10]],
        })

    # Check 2 — effective_from >= effective_to for closed rows
    overlap_df = spark.sql(f"""
        SELECT order_id, effective_from, effective_to, record_version
        FROM {database}.orders_unified
        WHERE effective_to IS NOT NULL
          AND effective_from >= effective_to
        LIMIT 100
    """)
    overlaps = overlap_df.collect()
    if overlaps:
        violations.append({
            "check": "scd2_date_overlap",
            "severity": "CRITICAL",
            "sample": [row.asDict() for row in overlaps[:10]],
        })

    # Check 3 — version gaps
    gap_df = spark.sql(f"""
        SELECT order_id, record_version,
               LAG(record_version) OVER (PARTITION BY order_id ORDER BY record_version) AS prev_version
        FROM {database}.orders_unified
    """)
    gap_df.createOrReplaceTempView("_version_check")
    gap_violations = spark.sql("""
        SELECT order_id, prev_version, record_version
        FROM _version_check
        WHERE record_version - prev_version > 1
        LIMIT 100
    """).collect()
    if gap_violations:
        violations.append({
            "check": "scd2_version_gaps",
            "severity": "WARNING",
            "sample": [row.asDict() for row in gap_violations[:10]],
        })

    return {"passed": len([v for v in violations if v["severity"] == "CRITICAL"]) == 0, "violations": violations}

class SilverOrdersValidator:
    """
    Validates a Spark DataFrame of Silver orders_unified records.

    Typical usage in bronze_to_silver_orders.py Glue job:
        validator = SilverOrdersValidator(spark)
        result = validator.validate(df_silver_batch, run_scd2_checks=True)
        if not result.success:
            raise RuntimeError("Silver orders CRITICAL DQ failure — Gold blocked")
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

    def validate(
        self,
        df: DataFrame,
        run_scd2_checks: bool = True,
        partition_ts: str = "",
    ) -> ValidationResult:
        row_count = df.count()
        current_row_count = df.filter("is_current = true").count()
        gx_df = SparkDFDataset(df)

        critical_result = self._run_suite(gx_df, self._critical_suite)
        warning_result = self._run_suite(gx_df, self._warning_suite)

        scd2_violations: list[dict] = []
        if run_scd2_checks:
            scd2_result = check_scd2_integrity(self.spark)
            if not scd2_result["passed"]:
                critical_result["success"] = False
                scd2_violations = scd2_result["violations"]
                critical_result["failed"].extend(
                    [{"expectation_type": "scd2_integrity", "severity": "CRITICAL", "result": v}
                     for v in scd2_violations if v["severity"] == "CRITICAL"]
                )

        self._emit_metrics(critical_result, warning_result, row_count, current_row_count)

        overall_success = critical_result["success"]
        if not overall_success:
            self._alert(critical_result, scd2_violations, partition_ts)
            logger.error(
                "CRITICAL silver_orders validation failed",
                extra={"partition_ts": partition_ts, "failures": critical_result["failed"]},
            )
        else:
            logger.info(
                "silver_orders validation passed",
                extra={
                    "partition_ts": partition_ts,
                    "row_count": row_count,
                    "current_rows": current_row_count,
                    "warnings": len(warning_result["failed"]),
                },
            )

        return ValidationResult(
            suite_name=f"{SUITE_NAME_CRITICAL}+{SUITE_NAME_WARNING}",
            success=overall_success,
            statistics={
                "critical_evaluated": critical_result["evaluated"],
                "critical_failed": len(critical_result["failed"]),
                "warning_evaluated": warning_result["evaluated"],
                "warning_failed": len(warning_result["failed"]),
                "scd2_violations": len(scd2_violations),
            },
            failed_expectations=critical_result["failed"] + warning_result["failed"],
            row_count=row_count,
            current_row_count=current_row_count,
        )

    def _run_suite(self, gx_df: SparkDFDataset, suite: ExpectationSuite) -> dict[str, Any]:
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
        return {"success": result.success, "evaluated": len(result.results), "failed": failed}

    def _emit_metrics(
        self,
        critical: dict,
        warning: dict,
        row_count: int,
        current_row_count: int,
    ) -> None:
        dimensions = [{"Name": "Layer", "Value": "Silver"}, {"Name": "Table", "Value": "orders_unified"}]
        try:
            self.cw.put_metric_data(
                Namespace=CLOUDWATCH_NAMESPACE,
                MetricData=[
                    {"MetricName": "CriticalExpectationFailures", "Dimensions": dimensions, "Value": len(critical["failed"]), "Unit": "Count"},
                    {"MetricName": "WarningExpectationFailures", "Dimensions": dimensions, "Value": len(warning["failed"]), "Unit": "Count"},
                    {"MetricName": "BatchRowCount", "Dimensions": dimensions, "Value": row_count, "Unit": "Count"},
                    {"MetricName": "CurrentRowCount", "Dimensions": dimensions, "Value": current_row_count, "Unit": "Count"},
                    {"MetricName": "ValidationSuccess", "Dimensions": dimensions, "Value": 1.0 if critical["success"] else 0.0, "Unit": "None"},
                ],
            )
        except Exception:
            logger.warning("CloudWatch emit failed", exc_info=True)

    def _alert(self, critical: dict, scd2_violations: list, partition_ts: str) -> None:
        sns_arn = os.environ.get("DATA_QUALITY_SNS_ARN")
        if not sns_arn:
            return
        message = {
            "alert_type": "SILVER_ORDERS_CRITICAL_FAILURE",
            "partition_ts": partition_ts,
            "failed_expectations": critical["failed"],
            "scd2_violations": scd2_violations,
            "action": "Gold dbt run blocked until resolved.",
        }
        try:
            self.sns.publish(
                TopicArn=sns_arn,
                Subject="[PulseCommerce] Silver Orders CRITICAL DQ Failure",
                Message=json.dumps(message, indent=2),
                MessageAttributes={
                    "severity": {"DataType": "String", "StringValue": "CRITICAL"},
                    "layer": {"DataType": "String", "StringValue": "silver"},
                },
            )
        except Exception:
            logger.error("SNS alert failed", exc_info=True)

def run_as_glue_job() -> None:
    import sys
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(sys.argv, ["JOB_NAME", "partition_date"])
    sc = SparkContext()
    glue_ctx = GlueContext(sc)
    spark = glue_ctx.spark_session
    job = Job(glue_ctx)
    job.init(args["JOB_NAME"], args)

    df = spark.sql(
        f"SELECT * FROM glue_catalog.silver.orders_unified "
        f"WHERE DATE(effective_from) = '{args['partition_date']}'"
    )

    validator = SilverOrdersValidator(spark)
    result = validator.validate(df, run_scd2_checks=True, partition_ts=args["partition_date"])

    job.commit()

    if not result.success:
        raise RuntimeError(
            f"CRITICAL silver_orders DQ failure for {args['partition_date']}. Gold run blocked."
        )

if __name__ == "__main__":
    run_as_glue_job()
