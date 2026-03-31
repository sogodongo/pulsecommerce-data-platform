from __future__ import annotations

import os
from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field

from analytics.api.main import (
    _async_sleep,
    _parse_redshift_result,
    get_redshift_client,
    limiter,
)

router = APIRouter()

REDSHIFT_WORKGROUP = os.environ.get("REDSHIFT_WORKGROUP", "pulsecommerce")
REDSHIFT_DATABASE = os.environ.get("REDSHIFT_DATABASE", "analytics")


class ProductDetail(BaseModel):
    product_key: str
    sku: str
    product_name: str
    brand: str | None
    category: str | None
    price_usd: float
    price_band: Literal["budget", "mid", "premium", "luxury"]
    tags: str | None             # pipe-delimited (Redshift-safe)
    is_active: bool
    # last 30 days
    orders_30d: int | None
    revenue_30d_usd: float | None
    unique_buyers_30d: int | None


class TopProductRow(BaseModel):
    rank: int
    sku: str
    product_name: str
    category: str | None
    price_band: str
    total_revenue_usd: float
    order_count: int
    unique_buyers: int


class PriceBandRow(BaseModel):
    price_band: str
    total_revenue_usd: float
    order_count: int
    avg_order_value_usd: float
    pct_of_revenue: float


class CategoryTrendRow(BaseModel):
    metric_date: str
    category: str
    revenue_usd: float
    order_count: int


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


@router.get("/{sku}", response_model=ProductDetail)
@limiter.limit("120/minute")
async def get_product(
    request: Request,
    sku: str,
    redshift: Any = Depends(get_redshift_client),
) -> ProductDetail:
    sql = f"""
        SELECT
            dp.product_key::VARCHAR     AS product_key,
            dp.sku,
            dp.product_name,
            dp.brand,
            dp.category,
            dp.price_usd,
            dp.price_band,
            dp.tags,
            dp.is_active,
            COUNT(fo.order_id)          AS orders_30d,
            SUM(fo.total_amount_usd)    AS revenue_30d_usd,
            COUNT(DISTINCT fo.user_key) AS unique_buyers_30d
        FROM gold.dim_products dp
        LEFT JOIN gold.fct_orders fo
            ON dp.product_key = fo.primary_product_key
            AND fo.order_date >= DATEADD(day, -30, CURRENT_DATE)
        WHERE dp.sku = '{sku}'
        GROUP BY
            dp.product_key, dp.sku, dp.product_name, dp.brand,
            dp.category, dp.price_usd, dp.price_band, dp.tags, dp.is_active
    """

    rows = await _run_redshift_query(redshift, sql)
    if not rows:
        raise HTTPException(status_code=404, detail=f"Product not found: {sku}")

    r = rows[0]
    return ProductDetail(
        product_key=r["product_key"],
        sku=r["sku"],
        product_name=r["product_name"],
        brand=r.get("brand"),
        category=r.get("category"),
        price_usd=float(r["price_usd"] or 0),
        price_band=r["price_band"] or "mid",
        tags=r.get("tags"),
        is_active=bool(r.get("is_active", True)),
        orders_30d=int(r["orders_30d"]) if r.get("orders_30d") else None,
        revenue_30d_usd=float(r["revenue_30d_usd"]) if r.get("revenue_30d_usd") else None,
        unique_buyers_30d=int(r["unique_buyers_30d"]) if r.get("unique_buyers_30d") else None,
    )


@router.get("/top-performers", response_model=list[TopProductRow])
@limiter.limit("30/minute")
async def get_top_performers(
    request: Request,
    start_date: str = Query("2024-01-01", description="Inclusive start date (YYYY-MM-DD)"),
    end_date: str = Query("2024-01-31", description="Inclusive end date (YYYY-MM-DD)"),
    limit: int = Query(10, ge=1, le=100, description="Number of top products to return"),
    metric: Literal["revenue", "orders", "buyers"] = Query(
        "revenue", description="Ranking metric"
    ),
    redshift: Any = Depends(get_redshift_client),
) -> list[TopProductRow]:
    rank_col = {
        "revenue": "SUM(fo.total_amount_usd)",
        "orders": "COUNT(fo.order_id)",
        "buyers": "COUNT(DISTINCT fo.user_key)",
    }[metric]

    sql = f"""
        SELECT
            ROW_NUMBER() OVER (ORDER BY {rank_col} DESC) AS rank,
            dp.sku,
            dp.product_name,
            dp.category,
            dp.price_band,
            SUM(fo.total_amount_usd)    AS total_revenue_usd,
            COUNT(fo.order_id)          AS order_count,
            COUNT(DISTINCT fo.user_key) AS unique_buyers
        FROM gold.fct_orders fo
        JOIN gold.dim_products dp ON fo.primary_product_key = dp.product_key
        WHERE fo.order_date BETWEEN '{start_date}' AND '{end_date}'
          AND fo.status NOT IN ('cancelled', 'refunded')
        GROUP BY dp.sku, dp.product_name, dp.category, dp.price_band
        ORDER BY {rank_col} DESC
        LIMIT {limit}
    """

    rows = await _run_redshift_query(redshift, sql)
    return [
        TopProductRow(
            rank=int(r["rank"]),
            sku=r["sku"],
            product_name=r["product_name"],
            category=r.get("category"),
            price_band=r["price_band"] or "mid",
            total_revenue_usd=float(r["total_revenue_usd"] or 0),
            order_count=int(r["order_count"] or 0),
            unique_buyers=int(r["unique_buyers"] or 0),
        )
        for r in rows
    ]


@router.get("/price-bands", response_model=list[PriceBandRow])
@limiter.limit("20/minute")
async def get_price_band_breakdown(
    request: Request,
    start_date: str = Query("2024-01-01"),
    end_date: str = Query("2024-01-31"),
    redshift: Any = Depends(get_redshift_client),
) -> list[PriceBandRow]:
    sql = f"""
        SELECT
            dp.price_band,
            SUM(fo.total_amount_usd)                               AS total_revenue_usd,
            COUNT(fo.order_id)                                     AS order_count,
            AVG(fo.total_amount_usd)                               AS avg_order_value_usd,
            SUM(fo.total_amount_usd) * 100.0
                / SUM(SUM(fo.total_amount_usd)) OVER ()            AS pct_of_revenue
        FROM gold.fct_orders fo
        JOIN gold.dim_products dp ON fo.primary_product_key = dp.product_key
        WHERE fo.order_date BETWEEN '{start_date}' AND '{end_date}'
          AND fo.status NOT IN ('cancelled', 'refunded')
        GROUP BY dp.price_band
        ORDER BY total_revenue_usd DESC
    """

    rows = await _run_redshift_query(redshift, sql)
    return [
        PriceBandRow(
            price_band=r["price_band"] or "mid",
            total_revenue_usd=float(r["total_revenue_usd"] or 0),
            order_count=int(r["order_count"] or 0),
            avg_order_value_usd=float(r["avg_order_value_usd"] or 0),
            pct_of_revenue=float(r["pct_of_revenue"] or 0),
        )
        for r in rows
    ]


@router.get("/category-trends", response_model=list[CategoryTrendRow])
@limiter.limit("20/minute")
async def get_category_trends(
    request: Request,
    start_date: str = Query("2024-01-01"),
    end_date: str = Query("2024-01-31"),
    category: str | None = Query(None, description="Filter to a specific category"),
    redshift: Any = Depends(get_redshift_client),
) -> list[CategoryTrendRow]:
    category_clause = f"AND dp.category = '{category}'" if category else ""

    sql = f"""
        SELECT
            fo.order_date::VARCHAR   AS metric_date,
            dp.category,
            SUM(fo.total_amount_usd) AS revenue_usd,
            COUNT(fo.order_id)       AS order_count
        FROM gold.fct_orders fo
        JOIN gold.dim_products dp ON fo.primary_product_key = dp.product_key
        WHERE fo.order_date BETWEEN '{start_date}' AND '{end_date}'
          AND fo.status NOT IN ('cancelled', 'refunded')
          AND dp.category IS NOT NULL
          {category_clause}
        GROUP BY fo.order_date, dp.category
        ORDER BY fo.order_date, dp.category
    """

    rows = await _run_redshift_query(redshift, sql)
    return [
        CategoryTrendRow(
            metric_date=r["metric_date"],
            category=r["category"],
            revenue_usd=float(r["revenue_usd"] or 0),
            order_count=int(r["order_count"] or 0),
        )
        for r in rows
    ]
