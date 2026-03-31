from __future__ import annotations

import hashlib
import hmac
import logging
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, LongType, StringType, TimestampType

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "LAKEHOUSE_BUCKET",
    "PII_SALT",
    "AWS_REGION",
    "LAST_SNAPSHOT_ID",
])

JOB_NAME      = args["JOB_NAME"]
WAREHOUSE     = args["LAKEHOUSE_BUCKET"]
PII_SALT      = args["PII_SALT"]
AWS_REGION    = args["AWS_REGION"]
LAST_SNAPSHOT = int(args.get("LAST_SNAPSHOT_ID", "0"))

sc          = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(JOB_NAME, args)

for key, value in [
    ("spark.sql.extensions",
     "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"),
    ("spark.sql.catalog.glue_catalog",
     "org.apache.iceberg.spark.SparkCatalog"),
    ("spark.sql.catalog.glue_catalog.catalog-impl",
     "org.apache.iceberg.aws.glue.GlueCatalog"),
    ("spark.sql.catalog.glue_catalog.warehouse", WAREHOUSE),
    ("spark.sql.catalog.glue_catalog.io-impl",
     "org.apache.iceberg.aws.s3.S3FileIO"),
    ("spark.sql.adaptive.enabled", "true"),
    ("spark.sql.adaptive.coalescePartitions.enabled", "true"),
]:
    spark.conf.set(key, value)


import boto3

_ddb = boto3.resource("dynamodb", region_name=AWS_REGION)
_bookmark_table = _ddb.Table("pulsecommerce-glue-bookmarks")


def get_bookmark(key: str) -> int:
    try:
        return int(_bookmark_table.get_item(Key={"job_name": key}).get("Item", {}).get("last_snapshot_id", 0))
    except Exception:
        return 0


def save_bookmark(key: str, snapshot_id: int) -> None:
    try:
        import datetime
        _bookmark_table.put_item(Item={
            "job_name": key,
            "last_snapshot_id": snapshot_id,
            "updated_at": datetime.datetime.utcnow().isoformat(),
        })
    except Exception as exc:
        logger.error("Bookmark save failed for %s: %s", key, exc)


@F.udf("string")
def hmac_sha256(value: str) -> str:
    if not value:
        return None
    return hmac.new(PII_SALT.encode(), value.encode(), hashlib.sha256).hexdigest()


def read_bronze_cdc() -> tuple[DataFrame, int]:
    last_snapshot = LAST_SNAPSHOT if LAST_SNAPSHOT > 0 else get_bookmark(JOB_NAME)

    current_snapshot = spark.sql("""
        SELECT snapshot_id FROM glue_catalog.bronze.orders_cdc.snapshots
        ORDER BY committed_at DESC LIMIT 1
    """).collect()[0]["snapshot_id"]

    if last_snapshot == current_snapshot:
        logger.info("CDC snapshot unchanged — nothing to process")
        return spark.createDataFrame([], spark.table("glue_catalog.bronze.orders_cdc").schema), current_snapshot

    if last_snapshot == 0:
        df = spark.table("glue_catalog.bronze.orders_cdc")
    else:
        df = (
            spark.read.format("iceberg")
            .option("start-snapshot-id", str(last_snapshot))
            .option("end-snapshot-id",   str(current_snapshot))
            .table("glue_catalog.bronze.orders_cdc")
        )

    # This CDC topic covers multiple tables — filter to orders rows only
    orders_df = df.filter(F.col("cdc_source_table") == "orders")
    logger.info("CDC records to process (orders): %d", orders_df.count())
    return orders_df, current_snapshot


def deduplicate_cdc(df: DataFrame) -> DataFrame:
    # Multiple CDC events can land for the same order in one batch (rapid updates).
    # LSN is monotonically increasing per Postgres replication slot — highest wins.
    w = Window.partitionBy("order_id").orderBy(F.desc("cdc_lsn"))
    return (
        df.withColumn("_rn", F.row_number().over(w))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )


def build_silver_records(df: DataFrame) -> DataFrame:
    now = F.current_timestamp()

    return (
        df
        .withColumn("user_id_hashed", hmac_sha256(F.col("user_id")))
        .withColumn("effective_from",  F.col("updated_at"))
        .withColumn("effective_to",    F.lit(None).cast(TimestampType()))  # NULL = current
        .withColumn("is_current",      F.lit(True))
        .withColumn("record_version",  F.lit(1))  # computed properly in merge
        .withColumn("processed_at",    now)
        .withColumn("updated_date",    F.to_date(F.col("updated_at")))
        # In Bronze, status holds the after-image for op='u'
        .withColumn("prev_status",
            F.when(F.col("cdc_operation") == "u", F.col("status"))
             .otherwise(F.lit(None).cast(StringType()))
        )
        .withColumn("status_changed_at",
            F.when(F.col("cdc_operation").isin(["c", "u", "r"]), F.col("updated_at"))
             .otherwise(F.lit(None).cast(TimestampType()))
        )
        .withColumn("status",
            F.when(F.col("cdc_operation") == "d", F.lit("deleted"))
             .otherwise(F.col("status"))
        )
        .select(
            "order_id",
            "user_id_hashed",
            "status",
            "prev_status",
            "status_changed_at",
            "currency",
            "total_amount",
            "total_amount_usd",
            "item_count",
            "promo_code",
            F.lit(None).cast(DoubleType()).alias("discount_usd"),   # not in Bronze schema v1
            "shipping_country",
            "payment_method",
            "fraud_flag",
            F.lit(None).cast(DoubleType()).alias("fraud_score"),     # joined separately
            "first_cart_at",
            "created_at",
            "updated_at",
            "effective_from",
            "effective_to",
            "is_current",
            F.lit(1).alias("record_version"),
            "cdc_lsn",
            "processed_at",
            "updated_date",
        )
    )


# Iceberg MERGE doesn't support the two-row SCD2 pattern in a single statement,
# so we close superseded versions first then insert the new current row separately.
def merge_scd2(new_df: DataFrame) -> None:
    new_df.createOrReplaceTempView("cdc_updates")

    spark.sql("""
        MERGE INTO glue_catalog.silver.orders_unified AS target
        USING cdc_updates AS source
        ON  target.order_id   = source.order_id
        AND target.is_current = TRUE
        WHEN MATCHED AND source.cdc_lsn > target.cdc_lsn
            THEN UPDATE SET
                target.is_current    = FALSE,
                target.effective_to  = source.effective_from,
                target.processed_at  = source.processed_at
    """)
    logger.info("SCD2 Step 1: closed superseded versions")

    spark.sql("""
        INSERT INTO glue_catalog.silver.orders_unified
        SELECT
            order_id, user_id_hashed, status, prev_status, status_changed_at,
            currency, total_amount, total_amount_usd, item_count,
            promo_code, discount_usd, shipping_country, payment_method,
            fraud_flag, fraud_score, first_cart_at, created_at, updated_at,
            effective_from, NULL AS effective_to, TRUE AS is_current,
            (
                SELECT COALESCE(MAX(record_version), 0) + 1
                FROM glue_catalog.silver.orders_unified prev
                WHERE prev.order_id = cdc_updates.order_id
            ) AS record_version,
            cdc_lsn, processed_at, updated_date
        FROM cdc_updates
        WHERE cdc_operation != 'd'
          OR order_id IN (
              SELECT order_id FROM glue_catalog.silver.orders_unified
              WHERE is_current = TRUE
          )
    """)
    logger.info("SCD2 Step 2: inserted new current versions")


def main():
    cdc_df, current_snapshot = read_bronze_cdc()

    if cdc_df.rdd.isEmpty():
        logger.info("No new CDC records — exiting cleanly")
        job.commit()
        return

    silver_df = (
        cdc_df
        .transform(deduplicate_cdc)
        .transform(build_silver_records)
    )

    merge_scd2(silver_df)
    save_bookmark(JOB_NAME, current_snapshot)
    logger.info("Orders CDC job complete. Bookmark → snapshot_id=%d", current_snapshot)
    job.commit()


main()
