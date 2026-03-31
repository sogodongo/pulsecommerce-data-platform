from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import boto3
import pandas as pd

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

FEATURE_GROUP_NAME = os.environ.get("FEATURE_GROUP_NAME", "pulsecommerce-user-behavioral")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
SAGEMAKER_ROLE_ARN = os.environ.get("SAGEMAKER_ROLE_ARN", "")
LAKEHOUSE_BUCKET = os.environ.get("LAKEHOUSE_BUCKET", "")

OFFLINE_STORE_PREFIX = f"s3://{LAKEHOUSE_BUCKET}/ml/feature-store/"

# Maps Python type → SageMaker FeatureType
FEATURE_DEFINITIONS = [
    {"FeatureName": "user_id_hashed",          "FeatureType": "String"},
    {"FeatureName": "feature_timestamp",        "FeatureType": "String"},

    {"FeatureName": "days_since_last_order",    "FeatureType": "Fractional"},
    {"FeatureName": "days_since_last_session",  "FeatureType": "Fractional"},
    {"FeatureName": "session_count_7d",         "FeatureType": "Integral"},
    {"FeatureName": "session_count_30d",        "FeatureType": "Integral"},

    {"FeatureName": "order_count_30d",          "FeatureType": "Integral"},
    {"FeatureName": "order_count_90d",          "FeatureType": "Integral"},
    {"FeatureName": "order_frequency_30d",      "FeatureType": "Fractional"},   # orders/week
    {"FeatureName": "avg_order_value_usd",      "FeatureType": "Fractional"},
    {"FeatureName": "total_ltv_usd",            "FeatureType": "Fractional"},
    {"FeatureName": "max_order_value_usd",      "FeatureType": "Fractional"},
    {"FeatureName": "discount_usage_rate",      "FeatureType": "Fractional"},   # % orders with discount

    {"FeatureName": "cart_abandonment_rate",    "FeatureType": "Fractional"},
    {"FeatureName": "avg_session_duration_s",   "FeatureType": "Fractional"},
    {"FeatureName": "avg_pages_per_session",    "FeatureType": "Fractional"},
    {"FeatureName": "product_view_count_7d",    "FeatureType": "Integral"},

    {"FeatureName": "preferred_category_encoded",  "FeatureType": "Integral"},
    {"FeatureName": "channel_group_encoded",       "FeatureType": "Integral"},

    {"FeatureName": "avg_fraud_score",          "FeatureType": "Fractional"},
    {"FeatureName": "refund_count_90d",         "FeatureType": "Integral"},

    {"FeatureName": "is_gdpr_scope",            "FeatureType": "Integral"},   # 0/1

    # label is written to offline store but never served from online
    {"FeatureName": "churned_30d",              "FeatureType": "Integral"},
    {"FeatureName": "is_current",               "FeatureType": "Integral"},
]

# Deterministic encoding — must stay in sync with train.py
CATEGORY_ENCODING = {
    "Electronics": 0, "Clothing": 1, "Home & Garden": 2, "Sports": 3,
    "Books": 4, "Beauty": 5, "Toys": 6, "Food": 7, "Automotive": 8, "Other": 9,
}

CHANNEL_ENCODING = {
    "Paid Search": 0, "Paid Social": 1, "Organic Social": 2, "Email": 3,
    "Direct": 4, "Referral": 5, "Organic Search": 6, "Other": 7,
}


def create_feature_group(sm_client: Any | None = None) -> dict[str, Any]:
    """Idempotent — skips creation if the group already exists."""
    client = sm_client or boto3.client("sagemaker", region_name=AWS_REGION)

    try:
        existing = client.describe_feature_group(FeatureGroupName=FEATURE_GROUP_NAME)
        status = existing["FeatureGroupStatus"]
        logger.info("Feature Group '%s' already exists (status=%s)", FEATURE_GROUP_NAME, status)
        return existing
    except client.exceptions.ResourceNotFound:
        pass

    logger.info("Creating Feature Group '%s'", FEATURE_GROUP_NAME)
    response = client.create_feature_group(
        FeatureGroupName=FEATURE_GROUP_NAME,
        RecordIdentifierFeatureName="user_id_hashed",
        EventTimeFeatureName="feature_timestamp",
        FeatureDefinitions=FEATURE_DEFINITIONS,
        OnlineStoreConfig={"EnableOnlineStore": True},
        OfflineStoreConfig={
            "S3StorageConfig": {
                "S3Uri": OFFLINE_STORE_PREFIX,
                "KmsKeyId": os.environ.get("KMS_KEY_ID", ""),
            },
            "DisableGlueTableCreation": False,   # auto-create Glue table for Athena
        },
        RoleArn=SAGEMAKER_ROLE_ARN,
        Description="User behavioral features for churn prediction — PulseCommerce",
        Tags=[
            {"Key": "Project", "Value": "PulseCommerce"},
            {"Key": "Layer", "Value": "ML-Features"},
        ],
    )

    for _ in range(30):
        status_resp = client.describe_feature_group(FeatureGroupName=FEATURE_GROUP_NAME)
        if status_resp["FeatureGroupStatus"] == "Created":
            break
        if status_resp["FeatureGroupStatus"] == "CreateFailed":
            raise RuntimeError(
                f"Feature Group creation failed: {status_resp.get('FailureReason')}"
            )
        time.sleep(10)

    logger.info("Feature Group '%s' is Created", FEATURE_GROUP_NAME)
    return response


FEATURE_SQL = """
WITH
-- 30-day and 90-day order aggregates
order_agg AS (
    SELECT
        o.user_key,
        du.user_id_hashed,
        COUNT(CASE WHEN o.order_date >= date_sub(current_date(), 30) THEN 1 END) AS order_count_30d,
        COUNT(CASE WHEN o.order_date >= date_sub(current_date(), 90) THEN 1 END) AS order_count_90d,
        AVG(o.total_amount_usd)                                                  AS avg_order_value_usd,
        MAX(o.total_amount_usd)                                                  AS max_order_value_usd,
        SUM(o.total_amount_usd)                                                  AS total_ltv_usd,
        AVG(o.fraud_score)                                                        AS avg_fraud_score,
        SUM(CASE WHEN o.discount_usd > 0 THEN 1 ELSE 0 END)
            / NULLIF(COUNT(*), 0)                                                AS discount_usage_rate,
        COUNT(CASE WHEN o.status = 'refunded'
              AND o.order_date >= date_sub(current_date(), 90) THEN 1 END)       AS refund_count_90d,
        DATEDIFF(current_date(), MAX(o.order_date))                              AS days_since_last_order,
        -- churn label: no order in last 30 days despite being active in prior 90
        CASE WHEN MAX(o.order_date) < date_sub(current_date(), 30)
             AND COUNT(CASE WHEN o.order_date >= date_sub(current_date(), 90) THEN 1 END) > 0
             THEN 1 ELSE 0 END                                                   AS churned_30d
    FROM gold.fct_orders o
    JOIN gold.dim_users du ON o.user_key = du.user_key AND du.is_current = true
    GROUP BY o.user_key, du.user_id_hashed
),
-- Session aggregates
session_agg AS (
    SELECT
        s.user_key,
        COUNT(CASE WHEN s.session_date >= date_sub(current_date(), 7)  THEN 1 END) AS session_count_7d,
        COUNT(CASE WHEN s.session_date >= date_sub(current_date(), 30) THEN 1 END) AS session_count_30d,
        DATEDIFF(current_date(), MAX(s.session_date))                              AS days_since_last_session,
        AVG(s.session_duration_s)                                                  AS avg_session_duration_s,
        AVG(s.page_view_count)                                                     AS avg_pages_per_session,
        SUM(CASE WHEN s.session_date >= date_sub(current_date(), 7) THEN s.product_view_count END)
                                                                                   AS product_view_count_7d,
        -- Cart abandonment: add_to_cart events without purchase in same session
        AVG(CASE WHEN s.funnel_exit_stage IN ('add_to_cart','checkout_start') THEN 1.0 ELSE 0.0 END)
                                                                                   AS cart_abandonment_rate
    FROM gold.fct_sessions s
    GROUP BY s.user_key
),
-- Preferred category (top purchase category per user)
category_pref AS (
    SELECT user_key, primary_product_category AS preferred_category
    FROM gold.dim_users
    WHERE is_current = true
),
-- Channel preference (most common acquisition channel)
channel_pref AS (
    SELECT o.user_key, dc.channel_group
    FROM gold.fct_orders o
    JOIN gold.dim_channels dc ON o.channel_key = dc.channel_key
    GROUP BY o.user_key, dc.channel_group
    QUALIFY ROW_NUMBER() OVER (PARTITION BY o.user_key ORDER BY COUNT(*) DESC) = 1
),
-- Geography (is_gdpr_scope)
geo AS (
    SELECT du.user_key, dg.is_gdpr_scope
    FROM gold.dim_users du
    JOIN gold.dim_geography dg ON du.country = dg.country
    WHERE du.is_current = true
)
SELECT
    oa.user_id_hashed,
    current_timestamp()                                         AS feature_timestamp,
    COALESCE(oa.days_since_last_order, 999)                    AS days_since_last_order,
    COALESCE(sa.days_since_last_session, 999)                  AS days_since_last_session,
    COALESCE(sa.session_count_7d, 0)                           AS session_count_7d,
    COALESCE(sa.session_count_30d, 0)                          AS session_count_30d,
    COALESCE(oa.order_count_30d, 0)                            AS order_count_30d,
    COALESCE(oa.order_count_90d, 0)                            AS order_count_90d,
    COALESCE(oa.order_count_30d / 4.0, 0.0)                   AS order_frequency_30d,
    COALESCE(oa.avg_order_value_usd, 0.0)                      AS avg_order_value_usd,
    COALESCE(oa.total_ltv_usd, 0.0)                            AS total_ltv_usd,
    COALESCE(oa.max_order_value_usd, 0.0)                      AS max_order_value_usd,
    COALESCE(oa.discount_usage_rate, 0.0)                      AS discount_usage_rate,
    COALESCE(sa.cart_abandonment_rate, 0.0)                    AS cart_abandonment_rate,
    COALESCE(sa.avg_session_duration_s, 0.0)                   AS avg_session_duration_s,
    COALESCE(sa.avg_pages_per_session, 0.0)                    AS avg_pages_per_session,
    COALESCE(sa.product_view_count_7d, 0)                      AS product_view_count_7d,
    -- default → "Other" = 9/7
    COALESCE(cp.preferred_category, 'Other')                   AS preferred_category_raw,
    COALESCE(ch.channel_group, 'Other')                        AS channel_group_raw,
    COALESCE(oa.avg_fraud_score, 0.0)                          AS avg_fraud_score,
    COALESCE(oa.refund_count_90d, 0)                           AS refund_count_90d,
    COALESCE(g.is_gdpr_scope, false)                           AS is_gdpr_scope_bool,
    COALESCE(oa.churned_30d, 0)                                AS churned_30d,
    1                                                           AS is_current
FROM order_agg oa
LEFT JOIN session_agg sa ON oa.user_key = sa.user_key
LEFT JOIN category_pref cp ON oa.user_key = cp.user_key
LEFT JOIN channel_pref ch ON oa.user_key = ch.user_key
LEFT JOIN geo g ON oa.user_key = g.user_key
"""


def compute_features(spark: Any) -> "pyspark.sql.DataFrame":
    """Execute feature SQL and apply integer encoding for categorical columns."""
    from pyspark.sql import functions as F

    df = spark.sql(FEATURE_SQL)

    category_expr = _build_map_expr("preferred_category_raw", CATEGORY_ENCODING, default=9)
    channel_expr = _build_map_expr("channel_group_raw", CHANNEL_ENCODING, default=7)

    df = (
        df
        .withColumn("preferred_category_encoded", category_expr)
        .withColumn("channel_group_encoded", channel_expr)
        .withColumn("is_gdpr_scope", F.col("is_gdpr_scope_bool").cast("integer"))
        .withColumn("feature_timestamp", F.date_format(F.col("feature_timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        .drop("preferred_category_raw", "channel_group_raw", "is_gdpr_scope_bool")
    )
    return df


def _build_map_expr(col: str, mapping: dict[str, int], default: int) -> Any:
    """Build a PySpark CASE WHEN column expression from a dict mapping."""
    from pyspark.sql import functions as F

    expr = F.lit(default)
    for k, v in reversed(list(mapping.items())):
        expr = F.when(F.col(col) == k, F.lit(v)).otherwise(expr)
    return expr


def ingest_to_feature_store(
    df_pandas: "pd.DataFrame",
    sm_feature_store_client: Any | None = None,
    batch_size: int = 500,
) -> dict[str, int]:
    """
    Ingest a pandas DataFrame into SageMaker Feature Store (online + offline).
    Uses PutRecord in batches. Returns {"success": n, "failed": n}.
    """
    client = sm_feature_store_client or boto3.client(
        "sagemaker-featurestore-runtime", region_name=AWS_REGION
    )

    success_count = 0
    failed_count = 0

    # Feature Store PutRecord expects list of {"FeatureName": ..., "ValueAsString": ...}
    feature_names = [f["FeatureName"] for f in FEATURE_DEFINITIONS]
    records = df_pandas[feature_names].to_dict(orient="records")

    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        for record in batch:
            feature_record = [
                {"FeatureName": k, "ValueAsString": str(v) if v is not None else ""}
                for k, v in record.items()
            ]
            try:
                client.put_record(
                    FeatureGroupName=FEATURE_GROUP_NAME,
                    Record=feature_record,
                )
                success_count += 1
            except Exception as exc:
                logger.warning("Failed to put record for %s: %s", record.get("user_id_hashed"), exc)
                failed_count += 1

        if i % (batch_size * 10) == 0:
            logger.info("Ingested %d/%d records", i + len(batch), len(records))

    logger.info("Feature Store ingestion complete: success=%d, failed=%d", success_count, failed_count)
    return {"success": success_count, "failed": failed_count}


def run_as_glue_job() -> None:
    """Glue Python Shell / PySpark job entrypoint."""
    import sys
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    sc = SparkContext()
    glue_ctx = GlueContext(sc)
    spark = glue_ctx.spark_session
    job = Job(glue_ctx)
    job.init(args["JOB_NAME"], args)

    create_feature_group()

    df_spark = compute_features(spark)
    df_pandas = df_spark.toPandas()
    logger.info("Computed %d user feature records", len(df_pandas))

    result = ingest_to_feature_store(df_pandas)
    job.commit()

    if result["failed"] > result["success"] * 0.05:   # >5% failure rate
        raise RuntimeError(f"Feature Store ingestion failure rate too high: {result}")


if __name__ == "__main__":
    run_as_glue_job()
