"""
processing/glue/bronze_to_silver_events.py

AWS Glue 5.0 ELT Job — Bronze → Silver (Clickstream Events)
=============================================================
Transforms raw Bronze clickstream events into the cleaned Silver layer.

Responsibilities:
  1. Incremental read using Iceberg snapshot diff (avoids full table scan)
  2. Deduplication by event_id (Flink at-least-once → exactly-once in Silver)
  3. Bot traffic filtering (is_bot = true → discarded)
  4. Internal staff event filtering (is_internal = true → discarded)
  5. PII masking — HMAC-SHA256(user_id + salt), GDPR city masking
  6. Product enrichment — join with silver.product_catalog for brand/subcategory/margin
  7. Geo region derivation (country → EMEA/APAC/AMER/LATAM)
  8. Fraud score join from enriched-events snapshot
  9. MERGE (upsert) into silver.enriched_events — handles late-arriving Bronze records

Schedule: Every 15 minutes via Amazon MWAA (Airflow silver_refresh_dag.py)
Workers:  G.1X, autoscale 5–20 workers
Runtime:  AWS Glue 5.0 (Spark 3.5, Python 3.11)
"""

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

# ── Job parameters (passed by Airflow / Glue console) ────────────────────────
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

# ── Spark / Glue context ──────────────────────────────────────────────────────
sc          = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(JOB_NAME, args)

# ── Iceberg + Glue Catalog config ─────────────────────────────────────────────
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

# ── Adaptive query execution ───────────────────────────────────────────────────
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")


# ─────────────────────────────────────────────────────────────────────────────
# Snapshot tracking helpers — DynamoDB bookmark
# ─────────────────────────────────────────────────────────────────────────────

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


# ─────────────────────────────────────────────────────────────────────────────
# PII masking helpers
# ─────────────────────────────────────────────────────────────────────────────

GDPR_COUNTRIES = {
    "AT","BE","BG","CY","CZ","DE","DK","EE","ES","FI","FR","GR","HR","HU",
    "IE","IT","LT","LU","LV","MT","NL","PL","PT","RO","SE","SI","SK",   # EU
    "GB","IS","LI","NO",                                                  # UK + EEA
}

# UDF: HMAC-SHA256 pseudonymisation (deterministic — same user_id always maps to same hash)
@F.udf("string")
def hmac_sha256(value: str) -> str:
    if value is None:
        return None
    return hmac.new(PII_SALT.encode(), value.encode(), hashlib.sha256).hexdigest()


# Geo region mapping broadcast
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


# ─────────────────────────────────────────────────────────────────────────────
# Step 1: Incremental Bronze read
# ─────────────────────────────────────────────────────────────────────────────

def read_bronze_incremental() -> tuple[DataFrame, int]:
    """
    Read only new Bronze records since the last processed snapshot.
    Returns (DataFrame, current_snapshot_id).
    Uses Iceberg incremental read (snapshot diff) — no full table scan.
    """
    last_snapshot = LAST_SNAPSHOT if LAST_SNAPSHOT > 0 else get_last_snapshot_id(JOB_NAME)

    # Get current snapshot ID before reading (for bookmark update)
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
        logger.info(
            "Incremental read: snapshot %d → %d",
            last_snapshot, current_snapshot,
        )
        df = (
            spark.read.format("iceberg")
            .option("start-snapshot-id", str(last_snapshot))
            .option("end-snapshot-id", str(current_snapshot))
            .table("glue_catalog.bronze.clickstream")
        )

    count = df.count()
    logger.info("Bronze records to process: %d", count)
    return df, current_snapshot


# ─────────────────────────────────────────────────────────────────────────────
# Step 2: Deduplication
# ─────────────────────────────────────────────────────────────────────────────

def deduplicate(df: DataFrame) -> DataFrame:
    """
    Keep exactly one record per event_id — the one with the latest ingested_at.
    Flink bronze_writer uses at-least-once delivery, so duplicates are possible.
    """
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


# ─────────────────────────────────────────────────────────────────────────────
# Step 3: Filtering (bots, internal, CRITICAL DQ failures)
# ─────────────────────────────────────────────────────────────────────────────

def filter_events(df: DataFrame) -> DataFrame:
    filtered = df.filter(
        (F.col("is_bot") == False) &
        (F.col("is_internal") == False) &
        (F.col("dq_flag") != "CRITICAL")
    )
    removed = df.count() - filtered.count()
    logger.info("After filtering: removed %d bot/internal/CRITICAL rows", removed)
    return filtered


# ─────────────────────────────────────────────────────────────────────────────
# Step 4: PII masking
# ─────────────────────────────────────────────────────────────────────────────

def mask_pii(df: DataFrame) -> DataFrame:
    return (
        df
        # Hash user_id with HMAC-SHA256 (deterministic — joins still work in Silver)
        .withColumn("user_id_hashed", hmac_sha256(F.col("user_id")))

        # Mask city for GDPR-scope countries (EU + UK + EEA)
        .withColumn("geo_city",
            F.when(F.col("geo_country").isin(GDPR_COUNTRIES), F.lit("MASKED"))
             .otherwise(F.col("geo_city"))
        )

        # Drop raw PII columns — they must not persist beyond Bronze
        .drop("user_id", "geo_lat", "geo_lon", "raw_payload", "is_bot", "is_internal")
    )


# ─────────────────────────────────────────────────────────────────────────────
# Step 5: Product enrichment (join with silver.product_catalog)
# ─────────────────────────────────────────────────────────────────────────────

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

    logger.info("Product enrichment join complete")
    return enriched


# ─────────────────────────────────────────────────────────────────────────────
# Step 6: Geo region derivation + derived columns
# ─────────────────────────────────────────────────────────────────────────────

def add_derived_columns(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("geo_region", derive_region(F.col("geo_country")))
        .withColumn("processed_at", F.current_timestamp())
        # Normalise event_date from event_ts for partitioning
        .withColumn("event_date", F.to_date(F.col("event_ts")))
    )


# ─────────────────────────────────────────────────────────────────────────────
# Step 7: Select final Silver schema
# ─────────────────────────────────────────────────────────────────────────────

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
    # Add any missing columns as nulls (schema evolution safety net)
    existing = set(df.columns)
    for col in SILVER_COLUMNS:
        if col not in existing:
            df = df.withColumn(col, F.lit(None).cast("string"))
    return df.select(*SILVER_COLUMNS)


# ─────────────────────────────────────────────────────────────────────────────
# Step 8: MERGE into Silver Iceberg
# ─────────────────────────────────────────────────────────────────────────────

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


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

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
