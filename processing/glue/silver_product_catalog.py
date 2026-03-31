from __future__ import annotations

import json
import logging
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "LAKEHOUSE_BUCKET",
    "AWS_REGION",
])

JOB_NAME  = args["JOB_NAME"]
WAREHOUSE = args["LAKEHOUSE_BUCKET"]
AWS_REGION = args["AWS_REGION"]

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
]:
    spark.conf.set(key, value)


@F.udf(ArrayType(StringType()))
def parse_tags(tags_raw: str) -> list[str]:
    if not tags_raw:
        return []
    try:
        parsed = json.loads(tags_raw)
        if isinstance(parsed, list):
            return [str(t).strip().lower() for t in parsed if t]
        return []
    except (json.JSONDecodeError, TypeError):
        return []


@F.udf("string")
def classify_price_band(price_usd) -> str:
    if price_usd is None:
        return "unknown"
    p = float(price_usd)
    if p < 25:
        return "budget"
    if p < 100:
        return "mid"
    if p < 300:
        return "premium"
    return "luxury"


def read_bronze_catalog() -> DataFrame:
    # Bronze always holds the full catalog state — producer sends full deltas, not incremental
    df = spark.table("glue_catalog.bronze.product_catalog_raw")
    logger.info("Bronze catalog rows: %d", df.count())
    return df


def deduplicate(df: DataFrame) -> DataFrame:
    from pyspark.sql import Window
    w = Window.partitionBy("sku").orderBy(F.desc("ingested_at"))
    return (
        df.withColumn("_rn", F.row_number().over(w))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )


def transform(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("price_usd",
            F.when(F.col("price_usd").cast("double") > 0, F.col("price_usd").cast("double"))
             .otherwise(F.lit(None))
        )
        .withColumn("cost_usd",
            F.col("cost_usd").cast("double")
        )
        .withColumn("stock_quantity",
            F.col("stock_quantity").cast("int")
        )
        .withColumn("weight_kg",
            F.col("weight_kg").cast("double")
        )
        .withColumn("margin_pct",
            F.when(
                F.col("cost_usd").isNotNull() & F.col("price_usd").isNotNull() & (F.col("price_usd") > 0),
                F.round((F.col("price_usd") - F.col("cost_usd")) / F.col("price_usd") * 100, 2)
            ).otherwise(F.col("margin_pct").cast("double"))
        )
        .withColumn("tags", parse_tags(F.col("tags")))
        .withColumn("price_band", classify_price_band(F.col("price_usd")))
        .withColumn("category",    F.lower(F.trim(F.col("category"))))
        .withColumn("subcategory", F.lower(F.trim(F.col("subcategory"))))
        .withColumn("brand",       F.initcap(F.trim(F.col("brand"))))
        .withColumn("name",        F.trim(F.col("name")))
        .withColumn("image_url",   F.trim(F.col("image_url")))
        .withColumn("processed_at", F.current_timestamp())
    )


def validate(df: DataFrame) -> DataFrame:
    # Log anomalies but keep the rows — downstream Gold filters on is_active / price_usd
    null_price_count = df.filter(F.col("price_usd").isNull()).count()
    if null_price_count > 0:
        logger.warning("%d SKUs have null price_usd after casting", null_price_count)

    neg_margin = df.filter(F.col("margin_pct") < 0).count()
    if neg_margin > 0:
        logger.warning("%d SKUs have negative margin_pct (cost > price)", neg_margin)

    inactive = df.filter(F.col("is_active") == False).count()
    logger.info("%d SKUs marked is_active=false (kept in Silver, excluded from Gold dim_products)", inactive)

    return df


SILVER_COLS = [
    "sku", "name", "category", "subcategory", "brand",
    "price_usd", "cost_usd", "margin_pct",
    "stock_quantity", "is_active", "weight_kg",
    "tags", "image_url",
    "created_at", "updated_at", "processed_at", "content_hash",
]


def select_silver_schema(df: DataFrame) -> DataFrame:
    existing = set(df.columns)
    for col in SILVER_COLS:
        if col not in existing:
            df = df.withColumn(col, F.lit(None).cast("string"))
    return df.select(*SILVER_COLS)


def merge_to_silver(df: DataFrame) -> None:
    # Iceberg merge-on-read skips rows where content_hash hasn't changed
    df.createOrReplaceTempView("catalog_updates")

    spark.sql("""
        MERGE INTO glue_catalog.silver.product_catalog AS target
        USING catalog_updates AS source
        ON target.sku = source.sku
        WHEN MATCHED AND source.content_hash != target.content_hash
            THEN UPDATE SET *
        WHEN NOT MATCHED
            THEN INSERT *
    """)

    logger.info("Merged %d SKUs into glue_catalog.silver.product_catalog", df.count())


def deactivate_removed_skus(current_skus_df: DataFrame) -> None:
    # Soft-delete: SKUs absent from the current Bronze snapshot get flagged, not deleted
    current_skus_df.createOrReplaceTempView("current_catalog_skus")

    spark.sql("""
        UPDATE glue_catalog.silver.product_catalog
        SET is_active = false,
            processed_at = current_timestamp()
        WHERE sku NOT IN (SELECT sku FROM current_catalog_skus)
          AND is_active = true
    """)

    logger.info("Deactivated SKUs no longer in Bronze catalog")


def main():
    bronze_df = read_bronze_catalog()

    silver_df = (
        bronze_df
        .transform(deduplicate)
        .transform(transform)
        .transform(validate)
        .transform(select_silver_schema)
    )

    # Cache — used twice (merge + deactivate)
    silver_df.cache()

    merge_to_silver(silver_df)
    deactivate_removed_skus(silver_df.select("sku"))

    silver_df.unpersist()

    logger.info("Product catalog Silver job complete")
    job.commit()


main()
