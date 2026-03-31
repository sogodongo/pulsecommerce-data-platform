from __future__ import annotations

import os
from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel, Field

from analytics.api.main import (
    _async_sleep,
    _parse_redshift_result,
    get_redshift_client,
    get_sm_featurestore_client,
    get_sm_runtime_client,
    limiter,
)

router = APIRouter()

REDSHIFT_WORKGROUP = os.environ.get("REDSHIFT_WORKGROUP", "pulsecommerce")
REDSHIFT_DATABASE = os.environ.get("REDSHIFT_DATABASE", "analytics")
FEATURE_GROUP_NAME = os.environ.get("FEATURE_GROUP_NAME", "pulsecommerce-user-behavioral")
CHURN_ENDPOINT_NAME = os.environ.get("CHURN_ENDPOINT_NAME", "pulsecommerce-churn-v1")
FRAUD_SCORE_THRESHOLD = float(os.environ.get("FRAUD_SCORE_THRESHOLD", "0.7"))


class UserProfile(BaseModel):
    user_id_hashed: str
    ltv_band: Literal["bronze", "silver", "gold", "platinum"]
    preferred_category: str | None
    country: str | None
    is_gdpr_scope: bool
    is_current: bool
    churn_score: float | None = Field(None, ge=0.0, le=1.0, description="Real-time churn probability")
    total_ltv_usd: float | None
    order_count_90d: int | None


class FeatureRecord(BaseModel):
    user_id_hashed: str
    feature_timestamp: str
    days_since_last_order: float
    days_since_last_session: float
    session_count_7d: int
    session_count_30d: int
    order_count_30d: int
    order_count_90d: int
    order_frequency_30d: float
    avg_order_value_usd: float
    total_ltv_usd: float
    cart_abandonment_rate: float
    avg_fraud_score: float
    churned_30d: int


class OrderSummary(BaseModel):
    order_id: str
    order_date: str
    status: str
    total_amount_usd: float
    net_amount_usd: float
    item_count: int
    fraud_score: float
    fraud_flagged: bool


class OrdersResponse(BaseModel):
    user_id_hashed: str
    orders: list[OrderSummary]
    count: int
    page: int
    page_size: int


class LtvSegmentRow(BaseModel):
    ltv_band: str
    user_count: int
    avg_ltv_usd: float
    pct_of_total: float


async def _run_redshift_query(client: Any, sql: str) -> list[dict[str, Any]]:
    try:
        stmt = client.execute_statement(
            WorkgroupName=REDSHIFT_WORKGROUP,
            Database=REDSHIFT_DATABASE,
            Sql=sql,
        )
        stmt_id = stmt["Id"]
        for _ in range(60):
            desc = client.describe_statement(Id=stmt_id)
            if desc["Status"] in ("FINISHED", "FAILED", "ABORTED"):
                break
            await _async_sleep(0.5)
        if desc["Status"] != "FINISHED":
            raise RuntimeError(f"Redshift query failed: {desc.get('Error')}")
        result = client.get_statement_result(Id=stmt_id)
        return _parse_redshift_result(result)
    except RuntimeError:
        raise
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Redshift error: {exc}") from exc


@router.get("/{user_id_hashed}", response_model=UserProfile)
@limiter.limit("60/minute")
async def get_user_profile(
    request: Request,
    user_id_hashed: str,
    include_churn_score: bool = Query(True, description="Invoke SageMaker endpoint for real-time churn score"),
    redshift: Any = Depends(get_redshift_client),
    sm_runtime: Any = Depends(get_sm_runtime_client),
    sm_fs: Any = Depends(get_sm_featurestore_client),
) -> UserProfile:
    # set include_churn_score=false for lower-latency reads when churn isn't needed
    sql = f"""
        SELECT
            du.user_id_hashed,
            du.ltv_band,
            du.primary_product_category  AS preferred_category,
            du.country,
            dg.is_gdpr_scope,
            du.is_current,
            SUM(fo.total_amount_usd)     AS total_ltv_usd,
            COUNT(fo.order_id)           AS order_count_90d
        FROM gold.dim_users du
        LEFT JOIN gold.fct_orders fo
            ON du.user_key = fo.user_key
            AND fo.order_date >= DATEADD(day, -90, CURRENT_DATE)
        LEFT JOIN gold.dim_geography dg ON du.country = dg.country
        WHERE du.user_id_hashed = '{user_id_hashed}'
          AND du.is_current = true
        GROUP BY
            du.user_id_hashed, du.ltv_band, du.primary_product_category,
            du.country, dg.is_gdpr_scope, du.is_current
    """

    rows = await _run_redshift_query(redshift, sql)
    if not rows:
        raise HTTPException(status_code=404, detail=f"User not found: {user_id_hashed}")

    row = rows[0]
    churn_score = None

    if include_churn_score:
        churn_score = await _get_churn_score(user_id_hashed, sm_fs, sm_runtime)

    return UserProfile(
        user_id_hashed=row["user_id_hashed"],
        ltv_band=row["ltv_band"] or "bronze",
        preferred_category=row.get("preferred_category"),
        country=row.get("country"),
        is_gdpr_scope=bool(row.get("is_gdpr_scope", False)),
        is_current=bool(row.get("is_current", True)),
        churn_score=churn_score,
        total_ltv_usd=float(row["total_ltv_usd"]) if row.get("total_ltv_usd") else None,
        order_count_90d=int(row["order_count_90d"]) if row.get("order_count_90d") else None,
    )


@router.get("/{user_id_hashed}/features", response_model=FeatureRecord)
@limiter.limit("120/minute")
async def get_user_features(
    request: Request,
    user_id_hashed: str,
    sm_fs: Any = Depends(get_sm_featurestore_client),
) -> FeatureRecord:
    feature_names = [
        "user_id_hashed", "feature_timestamp",
        "days_since_last_order", "days_since_last_session",
        "session_count_7d", "session_count_30d",
        "order_count_30d", "order_count_90d", "order_frequency_30d",
        "avg_order_value_usd", "total_ltv_usd", "cart_abandonment_rate",
        "avg_fraud_score", "churned_30d",
    ]

    try:
        resp = sm_fs.get_record(
            FeatureGroupName=FEATURE_GROUP_NAME,
            RecordIdentifierValueAsString=user_id_hashed,
            FeatureNames=feature_names,
        )
    except sm_fs.exceptions.ResourceNotFound:
        raise HTTPException(status_code=404, detail=f"No feature record for user: {user_id_hashed}")
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Feature Store error: {exc}") from exc

    record_dict = {
        r["FeatureName"]: r["ValueAsString"]
        for r in resp.get("Record", [])
    }

    def _f(key: str, default: float = 0.0) -> float:
        return float(record_dict.get(key, default))

    def _i(key: str, default: int = 0) -> int:
        return int(float(record_dict.get(key, default)))

    return FeatureRecord(
        user_id_hashed=record_dict.get("user_id_hashed", user_id_hashed),
        feature_timestamp=record_dict.get("feature_timestamp", ""),
        days_since_last_order=_f("days_since_last_order", 999),
        days_since_last_session=_f("days_since_last_session", 999),
        session_count_7d=_i("session_count_7d"),
        session_count_30d=_i("session_count_30d"),
        order_count_30d=_i("order_count_30d"),
        order_count_90d=_i("order_count_90d"),
        order_frequency_30d=_f("order_frequency_30d"),
        avg_order_value_usd=_f("avg_order_value_usd"),
        total_ltv_usd=_f("total_ltv_usd"),
        cart_abandonment_rate=_f("cart_abandonment_rate"),
        avg_fraud_score=_f("avg_fraud_score"),
        churned_30d=_i("churned_30d"),
    )


@router.get("/{user_id_hashed}/orders", response_model=OrdersResponse)
@limiter.limit("60/minute")
async def get_user_orders(
    request: Request,
    user_id_hashed: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    status_filter: str | None = Query(None, description="Filter by order status"),
    redshift: Any = Depends(get_redshift_client),
) -> OrdersResponse:
    offset = (page - 1) * page_size
    status_clause = f"AND fo.status = '{status_filter}'" if status_filter else ""

    sql = f"""
        SELECT
            fo.order_id,
            fo.order_date::VARCHAR       AS order_date,
            fo.status,
            fo.total_amount_usd,
            fo.net_amount_usd,
            fo.item_count,
            fo.fraud_score,
            fo.fraud_flagged
        FROM gold.fct_orders fo
        JOIN gold.dim_users du
            ON fo.user_key = du.user_key
            AND du.is_current = true
        WHERE du.user_id_hashed = '{user_id_hashed}'
          {status_clause}
        ORDER BY fo.order_date DESC
        LIMIT {page_size}
        OFFSET {offset}
    """

    rows = await _run_redshift_query(redshift, sql)

    orders = [
        OrderSummary(
            order_id=r["order_id"],
            order_date=r["order_date"],
            status=r["status"],
            total_amount_usd=float(r["total_amount_usd"] or 0),
            net_amount_usd=float(r["net_amount_usd"] or 0),
            item_count=int(r["item_count"] or 0),
            fraud_score=float(r["fraud_score"] or 0),
            fraud_flagged=bool(r["fraud_flagged"]),
        )
        for r in rows
    ]

    return OrdersResponse(
        user_id_hashed=user_id_hashed,
        orders=orders,
        count=len(orders),
        page=page,
        page_size=page_size,
    )


@router.get("/ltv-segments", response_model=list[LtvSegmentRow])
@limiter.limit("10/minute")
async def get_ltv_segments(
    request: Request,
    redshift: Any = Depends(get_redshift_client),
) -> list[LtvSegmentRow]:
    sql = """
        SELECT
            ltv_band,
            COUNT(*)                              AS user_count,
            AVG(total_spend_usd)                  AS avg_ltv_usd,
            COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS pct_of_total
        FROM gold.dim_users
        WHERE is_current = true
        GROUP BY ltv_band
        ORDER BY
            CASE ltv_band
                WHEN 'platinum' THEN 1
                WHEN 'gold'     THEN 2
                WHEN 'silver'   THEN 3
                ELSE 4
            END
    """

    rows = await _run_redshift_query(redshift, sql)
    return [
        LtvSegmentRow(
            ltv_band=r["ltv_band"],
            user_count=int(r["user_count"] or 0),
            avg_ltv_usd=float(r["avg_ltv_usd"] or 0),
            pct_of_total=float(r["pct_of_total"] or 0),
        )
        for r in rows
    ]


async def _get_churn_score(
    user_id_hashed: str,
    sm_fs: Any,
    sm_runtime: Any,
) -> float | None:
    # feature order must match train.py FEATURE_NAMES
    inference_features = [
        "days_since_last_order", "days_since_last_session",
        "session_count_7d", "session_count_30d",
        "order_count_30d", "order_count_90d", "order_frequency_30d",
        "avg_order_value_usd", "total_ltv_usd", "max_order_value_usd",
        "discount_usage_rate", "cart_abandonment_rate",
        "avg_session_duration_s", "avg_pages_per_session", "product_view_count_7d",
        "preferred_category_encoded", "channel_group_encoded",
        "avg_fraud_score", "refund_count_90d", "is_gdpr_scope",
    ]

    try:
        resp = sm_fs.get_record(
            FeatureGroupName=FEATURE_GROUP_NAME,
            RecordIdentifierValueAsString=user_id_hashed,
            FeatureNames=inference_features,
        )
    except Exception:
        return None

    record_map = {r["FeatureName"]: r["ValueAsString"] for r in resp.get("Record", [])}
    if not record_map:
        return None

    csv_payload = ",".join(record_map.get(f, "0") for f in inference_features)

    try:
        invoke_resp = sm_runtime.invoke_endpoint(
            EndpointName=CHURN_ENDPOINT_NAME,
            ContentType="text/csv",
            Body=csv_payload,
        )
        score = float(invoke_resp["Body"].read().decode("utf-8").strip())
        return round(max(0.0, min(1.0, score)), 4)
    except Exception:
        return None
