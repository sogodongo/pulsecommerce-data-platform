# =============================================================================
# analytics/api/main.py
# =============================================================================
# FastAPI 0.111 application — PulseCommerce Analytics Serving Layer.
#
# Exposes read-only endpoints backed by:
#   - Redshift Serverless (batch KPIs, aggregated metrics, product data)
#   - SageMaker Feature Store Online (real-time user behavioral features)
#   - SageMaker Endpoint (real-time churn score inference)
#
# Auth: API key via X-API-Key header (validated against Secrets Manager).
# Observability: structlog JSON logging, /metrics (Prometheus), /health.
# Rate limiting: slowapi (Redis token bucket) — 100 req/s per API key.
# =============================================================================

from __future__ import annotations

import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator

import boto3
import structlog
from fastapi import Depends, FastAPI, HTTPException, Request, Security, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.security.api_key import APIKeyHeader
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address
from starlette.responses import Response

from analytics.api.routers import products, users

# ---------------------------------------------------------------------------
# Logging setup (structlog JSON)
# ---------------------------------------------------------------------------

structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
)
log = structlog.get_logger()

# ---------------------------------------------------------------------------
# Prometheus metrics
# ---------------------------------------------------------------------------

REQUEST_COUNT = Counter(
    "api_requests_total",
    "Total HTTP requests",
    ["method", "endpoint", "status_code"],
)
REQUEST_LATENCY = Histogram(
    "api_request_latency_seconds",
    "HTTP request latency",
    ["method", "endpoint"],
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
)

# ---------------------------------------------------------------------------
# Rate limiter (slowapi)
# ---------------------------------------------------------------------------

limiter = Limiter(key_func=get_remote_address, default_limits=["100/second"])

# ---------------------------------------------------------------------------
# API Key authentication
# ---------------------------------------------------------------------------

API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=True)
_API_KEY_CACHE: dict[str, bool] = {}    # in-process cache; refreshed on 401


def _get_valid_api_keys() -> set[str]:
    """Fetch valid API keys from AWS Secrets Manager (cached at module level)."""
    secret_id = os.environ.get("API_KEYS_SECRET_ID", "pulsecommerce/api-keys")
    try:
        client = boto3.client("secretsmanager", region_name=os.environ.get("AWS_REGION", "us-east-1"))
        resp = client.get_secret_value(SecretId=secret_id)
        import json
        keys = json.loads(resp["SecretString"])
        return set(keys.values()) if isinstance(keys, dict) else set(keys)
    except Exception:
        # Fall back to env var for local dev / CI
        env_key = os.environ.get("API_KEY")
        return {env_key} if env_key else set()


_VALID_KEYS: set[str] = set()


async def verify_api_key(api_key: str = Security(API_KEY_HEADER)) -> str:
    global _VALID_KEYS
    if not _VALID_KEYS:
        _VALID_KEYS = _get_valid_api_keys()
    if api_key not in _VALID_KEYS:
        # Refresh once and retry (handles key rotation)
        _VALID_KEYS = _get_valid_api_keys()
        if api_key not in _VALID_KEYS:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid API key",
                headers={"WWW-Authenticate": "ApiKey"},
            )
    return api_key


# ---------------------------------------------------------------------------
# Shared AWS clients (initialised once at startup)
# ---------------------------------------------------------------------------

class AppState:
    redshift_client: Any = None
    sm_runtime_client: Any = None
    sm_featurestore_client: Any = None


app_state = AppState()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Initialise AWS clients once at startup; clean up on shutdown."""
    region = os.environ.get("AWS_REGION", "us-east-1")
    app_state.redshift_client = boto3.client("redshift-data", region_name=region)
    app_state.sm_runtime_client = boto3.client("sagemaker-runtime", region_name=region)
    app_state.sm_featurestore_client = boto3.client(
        "sagemaker-featurestore-runtime", region_name=region
    )
    log.info("api_startup", region=region)
    yield
    log.info("api_shutdown")


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(
    title="PulseCommerce Analytics API",
    description=(
        "Read-only analytics serving layer backed by Redshift Serverless, "
        "SageMaker Feature Store, and SageMaker churn inference endpoint."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# ── Middleware ───────────────────────────────────────────────────────────────

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.environ.get("CORS_ORIGINS", "*").split(","),
    allow_methods=["GET"],
    allow_headers=["X-API-Key", "Content-Type"],
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


@app.middleware("http")
async def observability_middleware(request: Request, call_next: Any) -> Any:
    """Emit Prometheus metrics and structlog entry for every request."""
    start = time.perf_counter()
    response = await call_next(request)
    latency = time.perf_counter() - start

    endpoint = request.url.path
    REQUEST_COUNT.labels(
        method=request.method,
        endpoint=endpoint,
        status_code=response.status_code,
    ).inc()
    REQUEST_LATENCY.labels(method=request.method, endpoint=endpoint).observe(latency)

    log.info(
        "http_request",
        method=request.method,
        path=endpoint,
        status=response.status_code,
        latency_ms=round(latency * 1000, 2),
    )
    return response


# ── Routers ──────────────────────────────────────────────────────────────────

app.include_router(
    users.router,
    prefix="/v1/users",
    tags=["Users"],
    dependencies=[Depends(verify_api_key)],
)
app.include_router(
    products.router,
    prefix="/v1/products",
    tags=["Products"],
    dependencies=[Depends(verify_api_key)],
)

# ── Core endpoints ────────────────────────────────────────────────────────────


@app.get("/health", tags=["Ops"], include_in_schema=False)
async def health() -> dict[str, str]:
    """Kubernetes / ALB health check."""
    return {"status": "ok", "version": app.version}


@app.get("/ready", tags=["Ops"], include_in_schema=False)
async def ready() -> dict[str, Any]:
    """Readiness check — verifies AWS client connectivity."""
    checks: dict[str, str] = {}
    try:
        app_state.redshift_client.list_statements(MaxResults=1)
        checks["redshift"] = "ok"
    except Exception as exc:
        checks["redshift"] = f"error: {exc}"

    all_ok = all(v == "ok" for v in checks.values())
    return JSONResponse(
        content={"status": "ready" if all_ok else "degraded", "checks": checks},
        status_code=200 if all_ok else 503,
    )


@app.get("/metrics", tags=["Ops"], include_in_schema=False)
async def metrics() -> Response:
    """Prometheus metrics scrape endpoint."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/v1/daily-metrics", tags=["KPIs"], dependencies=[Depends(verify_api_key)])
@limiter.limit("30/minute")
async def daily_metrics(
    request: Request,
    start_date: str = "2024-01-01",
    end_date: str = "2024-01-31",
) -> dict[str, Any]:
    """
    Return pre-aggregated daily KPIs from the Gold `agg_daily_metrics` table.

    Query executed against Redshift Serverless via the Data API.
    Results cached for 60s (Cache-Control header).
    """
    sql = f"""
        SELECT
            metric_date,
            gross_revenue_usd,
            net_revenue_usd,
            order_count,
            unique_buyers,
            new_buyers,
            returning_buyers,
            session_count,
            overall_conversion_rate,
            avg_order_value_usd,
            median_order_value_usd,
            cart_abandon_rate,
            high_fraud_order_count,
            refund_count
        FROM gold.agg_daily_metrics
        WHERE metric_date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY metric_date
    """

    try:
        stmt = app_state.redshift_client.execute_statement(
            WorkgroupName=os.environ.get("REDSHIFT_WORKGROUP", "pulsecommerce"),
            Database=os.environ.get("REDSHIFT_DATABASE", "analytics"),
            Sql=sql,
        )
        stmt_id = stmt["Id"]

        # Poll until done (async Data API)
        for _ in range(30):
            desc = app_state.redshift_client.describe_statement(Id=stmt_id)
            if desc["Status"] in ("FINISHED", "FAILED", "ABORTED"):
                break
            await _async_sleep(1)

        if desc["Status"] != "FINISHED":
            raise RuntimeError(f"Redshift query failed: {desc.get('Error')}")

        result = app_state.redshift_client.get_statement_result(Id=stmt_id)
        rows = _parse_redshift_result(result)

    except Exception as exc:
        log.error("daily_metrics_query_failed", error=str(exc))
        raise HTTPException(status_code=503, detail="Upstream query failed")

    response = JSONResponse(content={"data": rows, "count": len(rows)})
    response.headers["Cache-Control"] = "public, max-age=60"
    return response


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _async_sleep(seconds: float) -> None:
    import asyncio
    await asyncio.sleep(seconds)


def _parse_redshift_result(result: dict[str, Any]) -> list[dict[str, Any]]:
    """Convert Redshift Data API result to list of dicts."""
    columns = [col["name"] for col in result.get("ColumnMetadata", [])]
    rows = []
    for record in result.get("Records", []):
        row = {}
        for col, cell in zip(columns, record):
            # Each cell is {"stringValue": ..., "longValue": ..., etc.}
            value = next(iter(cell.values()), None)
            row[col] = value
        rows.append(row)
    return rows


# ---------------------------------------------------------------------------
# Dependency injection helpers (exported for routers)
# ---------------------------------------------------------------------------

def get_redshift_client() -> Any:
    return app_state.redshift_client


def get_sm_runtime_client() -> Any:
    return app_state.sm_runtime_client


def get_sm_featurestore_client() -> Any:
    return app_state.sm_featurestore_client


# ---------------------------------------------------------------------------
# Dev entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "analytics.api.main:app",
        host=os.environ.get("API_HOST", "0.0.0.0"),
        port=int(os.environ.get("API_PORT", "8000")),
        reload=True,
        log_level="info",
    )
