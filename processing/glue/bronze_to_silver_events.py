from __future__ import annotations

import hashlib
import hmac
import logging
import os
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "LAKEHOUSE_BUCKET",
    "PII_SALT",
    "AWS_REGION",
    "LAST_SNAPSHOT_ID",     # optional — 0 means full load
])

JOB_NAME        = args["JOB_NAME"]
WAREHOUSE       = args["LAKEHOUSE_BUCKET"]
PII_SALT        = args["PII_SALT"]
AWS_REGION      = args["AWS_REGION"]
LAST_SNAPSHOT   = int(args.get("LAST_SNAPSHOT_ID", "0"))

sc          = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(JOB_NAME, args)

spark.conf.set("spark.sql.extensions",
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
spark.conf.set("spark.sql.catalog.glue_catalog",
    "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl",
    "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", WAREHOUSE)
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl",
    "org.apache.iceberg.aws.s3.S3FileIO")
spark.conf.set("spark.sql.catalog.glue_catalog.glue.skip-archive", "true")

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")


import boto3

_ddb = boto3.resource("dynamodb", region_name=AWS_REGION)
_bookmark_table = _ddb.Table("pulsecommerce-glue-bookmarks")


def get_last_snapshot_id(job_name: str) -> int:
    try:
        resp = _bookmark_table.get_item(Key={"job_name": job_name})
        return int(resp.get("Item", {}).get("last_snapshot_id", 0))
    except Exception as exc:
        logger.warning("Could not read bookmark for %s: %s — defaulting to 0 (full load)", job_name, exc)
        return 0


def save_snapshot_id(job_name: str, snapshot_id: int) -> None:
    try:
        _bookmark_table.put_item(Item={
            "job_name": job_name,
            "last_snapshot_id": snapshot_id,
            "updated_at": str(__import__("datetime").datetime.utcnow().isoformat()),
        })
    except Exception as exc:
        logger.error("Could not save bookmark for %s snapshot=%d: %s", job_name, snapshot_id, exc)


GDPR_COUNTRIES = {
    "AT","BE","BG","CY","CZ","DE","DK","EE","ES","FI","FR","GR","HR","HU",
    "IE","IT","LT","LU","LV","MT","NL","PL","PT","RO","SE","SI","SK",   # EU
    "GB","IS","LI","NO",                                                  # UK + EEA
}

# HMAC-SHA256 pseudonymisation — deterministic so joins still work in Silver
@F.udf("string")
def hmac_sha256(value: str) -> str:
    if value is None:
        return None
    return hmac.new(PII_SALT.encode(), value.encode(), hashlib.sha256).hexdigest()


COUNTRY_REGION = {
    **{c: "EMEA" for c in [
        "GB","DE","FR","IT","ES","NL","BE","SE","NO","DK","FI","PL","AT","CH",
        "ZA","NG","KE","EG","MA","GH","TZ","ET","UG","RW","SN",
    ]},
    **{c: "APAC" for c in [
        "IN","CN","JP","AU","SG","ID","TH","MY","PH","VN","KR","NZ","BD","PK",
    ]},
    **{c: "AMER" for c in [
        "US","CA","MX",
    ]},
    **{c: "LATAM" for c in [
        "BR","AR","CO","CL","PE","VE","EC","BO","UY","PY","CR","GT","HN","SV",
    ]},
}

country_region_map = spark.sparkContext.broadcast(COUNTRY_REGION)


@F.udf("string")
def derive_region(country: str) -> str:
    if country is None:
        return "UNKNOWN"
    return country_region_map.value.get(country, "OTHER")


def read_bronze_incremental() -> tuple[DataFrame, int]:
    last_snapshot = LAST_SNAPSHOT if LAST_SNAPSHOT > 0 else get_last_snapshot_id(JOB_NAME)

    current_snapshot = spark.sql("""
        SELECT snapshot_id FROM glue_catalog.bronze.clickstream.snapshots
        ORDER BY committed_at DESC LIMIT 1
    """).collect()[0]["snapshot_id"]

    if last_snapshot == 0:
        logger.info("No bookmark found — performing full Bronze load")
        df = spark.table("glue_catalog.bronze.clickstream")
    elif last_snapshot == current_snapshot:
        logger.info("Bronze snapshot unchanged (id=%d) — nothing to process", current_snapshot)
        return spark.createDataFrame([], spark.table("glue_catalog.bronze.clickstream").schema), current_snapshot
    else:
        logger.info("Incremental read: snapshot %d → %d", last_snapshot, current_snapshot)
        df = (
            spark.read.format("iceberg")
            .option("start-snapshot-id", str(last_snapshot))
            .option("end-snapshot-id", str(current_snapshot))
            .table("glue_catalog.bronze.clickstream")
        )

    count = df.count()
    logger.info("Bronze records to process: %d", count)
    return df, current_snapshot


def deduplicate(df: DataFrame) -> DataFrame:
    # Flink bronze_writer is at-least-once — keep latest ingested_at per event_id
    window = Window.partitionBy("event_id").orderBy(F.desc("ingested_at"))
    deduped = (
        df.withColumn("_rn", F.row_number().over(window))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )
    logger.info(
        "After dedup: %d records (removed %d duplicates)",
        deduped.count(), df.count() - deduped.count(),
    )
    return deduped


def filter_events(df: DataFrame) -> DataFrame:
    filtered = df.filter(
        (F.col("is_bot") == False) &
        (F.col("is_internal") == False) &
        (F.col("dq_flag") != "CRITICAL")
    )
    removed = df.count() - filtered.count()
    logger.info("After filtering: removed %d bot/internal/CRITICAL rows", removed)
    return filtered


def mask_pii(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("user_id_hashed", hmac_sha256(F.col("user_id")))
        # Mask city for GDPR-scope countries — keep country for geo aggregations
        .withColumn("geo_city",
            F.when(F.col("geo_country").isin(GDPR_COUNTRIES), F.lit("MASKED"))
             .otherwise(F.col("geo_city"))
        )
        # raw PII must not persist beyond Bronze
        .drop("user_id", "geo_lat", "geo_lon", "raw_payload", "is_bot", "is_internal")
    )


def enrich_with_product_catalog(df: DataFrame) -> DataFrame:
    catalog = spark.table("glue_catalog.silver.product_catalog").select(
        F.col("sku").alias("cat_sku"),
        F.col("subcategory").alias("product_subcategory"),
        F.col("brand").alias("product_brand"),
        F.col("cost_usd").alias("product_cost_usd"),
        F.col("margin_pct").alias("product_margin_pct"),
    )

    enriched = df.join(
        catalog,
        df["product_sku"] == catalog["cat_sku"],
        how="left",
    ).drop("cat_sku")

    return enriched


def add_derived_columns(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("geo_region", derive_region(F.col("geo_country")))
        .withColumn("processed_at", F.current_timestamp())
        # event_date derived from event_ts for partition pruning
        .withColumn("event_date", F.to_date(F.col("event_ts")))
    )


SILVER_COLUMNS = [
    "event_id", "user_id_hashed", "session_id", "event_type",
    "event_ts", "ingested_at", "processed_at",
    "device_type", "device_os", "app_version",
    "page_url", "page_referrer",
    "product_sku", "product_category", "product_subcategory",
    "product_brand", "product_price_usd", "product_cost_usd",
    "product_margin_pct", "product_quantity",
    "geo_country", "geo_city", "geo_timezone", "geo_region",
    "ab_cohort", "search_query", "order_id",
    "fraud_score", "fraud_signals",
    "dq_flag",
    "event_date",
]


def select_silver_schema(df: DataFrame) -> DataFrame:
    # Fill missing columns with nulls — handles schema evolution without breaking the job
    existing = set(df.columns)
    for col in SILVER_COLUMNS:
        if col not in existing:
            df = df.withColumn(col, F.lit(None).cast("string"))
    return df.select(*SILVER_COLUMNS)


def merge_to_silver(df: DataFrame) -> None:
    df.createOrReplaceTempView("silver_updates")

    spark.sql("""
        MERGE INTO glue_catalog.silver.enriched_events AS target
        USING silver_updates AS source
        ON target.event_id = source.event_id
        WHEN MATCHED AND source.ingested_at > target.ingested_at
            THEN UPDATE SET *
        WHEN NOT MATCHED
            THEN INSERT *
    """)

    logger.info("Merged records into glue_catalog.silver.enriched_events")


def main():
    bronze_df, current_snapshot = read_bronze_incremental()

    if bronze_df.rdd.isEmpty():
        logger.info("No new Bronze records — exiting cleanly")
        job.commit()
        return

    silver_df = (
        bronze_df
        .transform(deduplicate)
        .transform(filter_events)
        .transform(mask_pii)
        .transform(enrich_with_product_catalog)
        .transform(add_derived_columns)
        .transform(select_silver_schema)
    )

    merge_to_silver(silver_df)
    save_snapshot_id(JOB_NAME, current_snapshot)
    logger.info("Job complete. Bookmark updated to snapshot_id=%d", current_snapshot)
    job.commit()


main()
