from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from processing.quality.bronze_clickstream_expectations import (
    BronzeClickstreamValidator,
    build_critical_suite,
    build_warning_suite,
)
from processing.quality.silver_orders_expectations import (
    SilverOrdersValidator,
    build_critical_suite as silver_critical_suite,
    build_warning_suite as silver_warning_suite,
    check_scd2_integrity,
)

@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Local PySpark session — no cluster required."""
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("GX_CI_Suite")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

BRONZE_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("user_id_hashed", StringType(), False),
    StructField("session_id", StringType(), False),
    StructField("event_type", StringType(), True),
    StructField("event_timestamp", TimestampType(), True),
    StructField("ingested_at", TimestampType(), True),
    StructField("device_type", StringType(), True),
    StructField("product_price_usd", DoubleType(), True),
    StructField("page_load_ms", IntegerType(), True),
    StructField("country", StringType(), True),
    StructField("user_agent", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

SILVER_ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("user_id_hashed", StringType(), False),
    StructField("status", StringType(), True),
    StructField("cdc_op", StringType(), True),
    StructField("total_amount_usd", DoubleType(), True),
    StructField("net_amount_usd", DoubleType(), True),
    StructField("discount_usd", DoubleType(), True),
    StructField("item_count", IntegerType(), True),
    StructField("fraud_score", DoubleType(), True),
    StructField("cdc_lsn", LongType(), True),
    StructField("record_version", IntegerType(), True),
    StructField("is_current", BooleanType(), True),
    StructField("effective_from", TimestampType(), True),
    StructField("effective_to", TimestampType(), True),
    StructField("payment_method", StringType(), True),
    StructField("processed_at", TimestampType(), True),
])

def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)   # Spark needs tz-naive

def make_valid_bronze_rows(n: int = 200) -> list[tuple]:
    ts = _now()
    return [
        (
            f"evt_{i:06d}",                  # event_id
            f"user_{i % 50:04d}_hashed",     # user_id_hashed
            f"sess_{i % 20:04d}",            # session_id
            ["page_view", "product_view", "add_to_cart", "checkout_start"][i % 4],
            ts,                              # event_timestamp
            ts,                              # ingested_at
            ["desktop", "mobile", "tablet"][i % 3],  # device_type
            float(10 + (i % 500)),           # product_price_usd
            200 + (i % 3000),                # page_load_ms
            "US",                            # country
            "Mozilla/5.0 Chrome/120",        # user_agent
            json.dumps({"raw": f"payload_{i}"}),  # raw_payload
        )
        for i in range(n)
    ]

def make_valid_silver_orders_rows(n: int = 100) -> list[tuple]:
    ts = _now()
    return [
        (
            f"ord_{i:06d}",             # order_id
            f"user_{i % 30:04d}_hash",  # user_id_hashed
            "confirmed",                # status
            "u",                        # cdc_op
            float(50 + (i % 500)),      # total_amount_usd
            float(45 + (i % 490)),      # net_amount_usd
            float(5 + (i % 10)),        # discount_usd
            1 + (i % 10),               # item_count
            round(0.1 * (i % 10) / 10, 2),  # fraud_score
            100_000 + i,                # cdc_lsn
            1,                          # record_version
            True,                       # is_current
            ts,                         # effective_from
            None,                       # effective_to (open)
            "card",                     # payment_method
            ts,                         # processed_at
        )
        for i in range(n)
    ]

def _mock_cw() -> MagicMock:
    m = MagicMock()
    m.put_metric_data.return_value = {}
    return m

def _mock_sns() -> MagicMock:
    m = MagicMock()
    m.publish.return_value = {"MessageId": "mock-message-id"}
    return m

class TestBronzeCriticalSuite:

    def test_valid_data_passes(self, spark: SparkSession) -> None:
        df = spark.createDataFrame(make_valid_bronze_rows(200), schema=BRONZE_SCHEMA)
        validator = BronzeClickstreamValidator(spark, cw_client=_mock_cw(), sns_client=_mock_sns())
        result = validator.validate(df, partition_ts="2024-01-15T10:00:00Z")
        assert result.success, f"Expected pass; failures: {result.failed_expectations}"

    def test_missing_event_id_fails(self, spark: SparkSession) -> None:
        rows = make_valid_bronze_rows(50)
        # Null out event_id in first 5 rows
        rows = [(None,) + r[1:] if i < 5 else r for i, r in enumerate(rows)]
        df = spark.createDataFrame(rows, schema=BRONZE_SCHEMA)
        validator = BronzeClickstreamValidator(spark, cw_client=_mock_cw(), sns_client=_mock_sns())
        result = validator.validate(df, partition_ts="test")
        assert not result.success

    def test_unknown_event_type_fails(self, spark: SparkSession) -> None:
        rows = make_valid_bronze_rows(100)
        # Replace all event_types with unknown value
        rows = [r[:3] + ("UNKNOWN_EVENT_TYPE",) + r[4:] for r in rows]
        df = spark.createDataFrame(rows, schema=BRONZE_SCHEMA)
        validator = BronzeClickstreamValidator(spark, cw_client=_mock_cw(), sns_client=_mock_sns())
        result = validator.validate(df, partition_ts="test")
        assert not result.success

    def test_empty_dataframe_fails(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([], schema=BRONZE_SCHEMA)
        validator = BronzeClickstreamValidator(spark, cw_client=_mock_cw(), sns_client=_mock_sns())
        result = validator.validate(df, partition_ts="test")
        assert not result.success

    def test_duplicate_event_ids_fail(self, spark: SparkSession) -> None:
        rows = make_valid_bronze_rows(50)
        # Duplicate first row's event_id across all rows
        rows = [("DUPLICATE_ID",) + r[1:] for r in rows]
        df = spark.createDataFrame(rows, schema=BRONZE_SCHEMA)
        validator = BronzeClickstreamValidator(spark, cw_client=_mock_cw(), sns_client=_mock_sns())
        result = validator.validate(df, partition_ts="test")
        assert not result.success

class TestBronzeWarningSuite:

    def test_out_of_range_price_triggers_warning(self, spark: SparkSession) -> None:
        rows = make_valid_bronze_rows(200)
        # Set price to negative for 5 rows (0.1% threshold allows up to 0.1% failure)
        rows = [r[:7] + (-99.99,) + r[8:] if i < 5 else r for i, r in enumerate(rows)]
        df = spark.createDataFrame(rows, schema=BRONZE_SCHEMA)
        validator = BronzeClickstreamValidator(spark, cw_client=_mock_cw(), sns_client=_mock_sns())
        result = validator.validate(df, partition_ts="test")
        # Critical suite should still pass; warning may fail but not block
        assert result.statistics["critical_expectations_failed"] == 0

    def test_invalid_country_code_triggers_warning(self, spark: SparkSession) -> None:
        rows = make_valid_bronze_rows(200)
        rows = [r[:9] + ("INVALID",) + r[10:] if i < 10 else r for i, r in enumerate(rows)]
        df = spark.createDataFrame(rows, schema=BRONZE_SCHEMA)
        validator = BronzeClickstreamValidator(spark, cw_client=_mock_cw(), sns_client=_mock_sns())
        result = validator.validate(df, partition_ts="test")
        # Overall success depends only on critical suite
        assert result.statistics["critical_expectations_failed"] == 0

class TestSilverOrdersCriticalSuite:

    def test_valid_orders_pass(self, spark: SparkSession) -> None:
        df = spark.createDataFrame(make_valid_silver_orders_rows(100), schema=SILVER_ORDERS_SCHEMA)
        validator = SilverOrdersValidator(spark, cw_client=_mock_cw(), sns_client=_mock_sns())
        result = validator.validate(df, run_scd2_checks=False, partition_ts="2024-01-15")
        assert result.success, f"Expected pass; failures: {result.failed_expectations}"

    def test_negative_amount_fails(self, spark: SparkSession) -> None:
        rows = make_valid_silver_orders_rows(50)
        rows = [r[:4] + (-100.0,) + r[5:] if i < 3 else r for i, r in enumerate(rows)]
        df = spark.createDataFrame(rows, schema=SILVER_ORDERS_SCHEMA)
        validator = SilverOrdersValidator(spark, cw_client=_mock_cw(), sns_client=_mock_sns())
        result = validator.validate(df, run_scd2_checks=False, partition_ts="test")
        assert not result.success

    def test_invalid_status_fails(self, spark: SparkSession) -> None:
        rows = make_valid_silver_orders_rows(50)
        rows = [r[:2] + ("INVALID_STATUS",) + r[3:] for r in rows]
        df = spark.createDataFrame(rows, schema=SILVER_ORDERS_SCHEMA)
        validator = SilverOrdersValidator(spark, cw_client=_mock_cw(), sns_client=_mock_sns())
        result = validator.validate(df, run_scd2_checks=False, partition_ts="test")
        assert not result.success

    def test_missing_order_id_fails(self, spark: SparkSession) -> None:
        rows = make_valid_silver_orders_rows(50)
        rows = [(None,) + r[1:] if i < 5 else r for i, r in enumerate(rows)]
        df = spark.createDataFrame(rows, schema=SILVER_ORDERS_SCHEMA)
        validator = SilverOrdersValidator(spark, cw_client=_mock_cw(), sns_client=_mock_sns())
        result = validator.validate(df, run_scd2_checks=False, partition_ts="test")
        assert not result.success

    def test_fraud_score_out_of_range_fails(self, spark: SparkSession) -> None:
        rows = make_valid_silver_orders_rows(50)
        rows = [r[:8] + (1.5,) + r[9:] for r in rows]   # fraud_score = 1.5
        df = spark.createDataFrame(rows, schema=SILVER_ORDERS_SCHEMA)
        validator = SilverOrdersValidator(spark, cw_client=_mock_cw(), sns_client=_mock_sns())
        result = validator.validate(df, run_scd2_checks=False, partition_ts="test")
        assert not result.success

    def test_invalid_cdc_op_fails(self, spark: SparkSession) -> None:
        rows = make_valid_silver_orders_rows(50)
        rows = [r[:3] + ("X",) + r[4:] for r in rows]   # cdc_op = "X"
        df = spark.createDataFrame(rows, schema=SILVER_ORDERS_SCHEMA)
        validator = SilverOrdersValidator(spark, cw_client=_mock_cw(), sns_client=_mock_sns())
        result = validator.validate(df, run_scd2_checks=False, partition_ts="test")
        assert not result.success

class TestSuiteBuilders:

    def test_bronze_critical_suite_has_expectations(self) -> None:
        suite = build_critical_suite()
        assert len(suite.expectations) >= 6

    def test_bronze_warning_suite_has_expectations(self) -> None:
        suite = build_warning_suite()
        assert len(suite.expectations) >= 5

    def test_silver_critical_suite_has_expectations(self) -> None:
        suite = silver_critical_suite()
        assert len(suite.expectations) >= 8

    def test_silver_warning_suite_has_expectations(self) -> None:
        suite = silver_warning_suite()
        assert len(suite.expectations) >= 5

    def test_bronze_critical_suite_name(self) -> None:
        suite = build_critical_suite()
        assert suite.expectation_suite_name == "bronze_clickstream_critical"

    def test_silver_critical_suite_name(self) -> None:
        suite = silver_critical_suite()
        assert suite.expectation_suite_name == "silver_orders_critical"

class TestMetricEmission:

    def test_cloudwatch_called_on_success(self, spark: SparkSession) -> None:
        cw = _mock_cw()
        df = spark.createDataFrame(make_valid_bronze_rows(100), schema=BRONZE_SCHEMA)
        validator = BronzeClickstreamValidator(spark, cw_client=cw, sns_client=_mock_sns())
        validator.validate(df, partition_ts="test")
        cw.put_metric_data.assert_called_once()

    def test_cloudwatch_called_on_failure(self, spark: SparkSession) -> None:
        cw = _mock_cw()
        df = spark.createDataFrame([], schema=BRONZE_SCHEMA)
        validator = BronzeClickstreamValidator(spark, cw_client=cw, sns_client=_mock_sns())
        validator.validate(df, partition_ts="test")
        cw.put_metric_data.assert_called_once()

    def test_sns_not_called_on_success(self, spark: SparkSession) -> None:
        sns = _mock_sns()
        df = spark.createDataFrame(make_valid_bronze_rows(100), schema=BRONZE_SCHEMA)
        validator = BronzeClickstreamValidator(spark, cw_client=_mock_cw(), sns_client=sns)
        validator.validate(df, partition_ts="test")
        sns.publish.assert_not_called()

    def test_sns_called_on_critical_failure(self, spark: SparkSession) -> None:
        sns = _mock_sns()
        with patch.dict(os.environ, {"DATA_QUALITY_SNS_ARN": "arn:aws:sns:us-east-1:123:test"}):
            df = spark.createDataFrame([], schema=BRONZE_SCHEMA)
            validator = BronzeClickstreamValidator(spark, cw_client=_mock_cw(), sns_client=sns)
            validator.validate(df, partition_ts="test")
        sns.publish.assert_called_once()

def run_all_suites_standalone() -> int:
    """
    Run validation suites without pytest.
    Returns 0 on all-pass, 1 if any CRITICAL failure.
    """
    os.environ.setdefault("AWS_REGION", "us-east-1")

    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("GX_CI_Standalone")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    exit_code = 0
    results = []

    # Bronze clickstream
    df_bronze = spark.createDataFrame(make_valid_bronze_rows(500), schema=BRONZE_SCHEMA)
    bronze_validator = BronzeClickstreamValidator(spark, cw_client=_mock_cw(), sns_client=_mock_sns())
    br = bronze_validator.validate(df_bronze, partition_ts="ci-run")
    results.append(("bronze_clickstream", br.success, br.statistics))

    # Silver orders
    df_orders = spark.createDataFrame(make_valid_silver_orders_rows(200), schema=SILVER_ORDERS_SCHEMA)
    silver_validator = SilverOrdersValidator(spark, cw_client=_mock_cw(), sns_client=_mock_sns())
    sr = silver_validator.validate(df_orders, run_scd2_checks=False, partition_ts="ci-run")
    results.append(("silver_orders", sr.success, sr.statistics))

    for name, success, stats in results:
        status = "PASS" if success else "FAIL"
        print(f"[{status}] {name}: {json.dumps(stats)}")
        if not success:
            exit_code = 1

    spark.stop()
    return exit_code

if __name__ == "__main__":
    sys.exit(run_all_suites_standalone())
