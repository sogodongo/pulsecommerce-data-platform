# =============================================================================
# analytics/api/Dockerfile
# =============================================================================
# Multi-stage build for the PulseCommerce Analytics API (FastAPI 0.111).
#
# Stages:
#   builder  — install dependencies into a virtual environment
#   runtime  — minimal image with only the venv + app code
#
# Security:
#   - Runs as non-root user (uid=1000)
#   - No dev dependencies in runtime image
#   - AWS credentials injected at runtime via IAM role (ECS task role)
#     — no credentials in image
#
# Usage:
#   docker build -t pulsecommerce-api -f analytics/api/Dockerfile .
#   docker run -p 8000:8000 \
#     -e AWS_REGION=us-east-1 \
#     -e REDSHIFT_WORKGROUP=pulsecommerce \
#     -e FEATURE_GROUP_NAME=pulsecommerce-user-behavioral \
#     -e CHURN_ENDPOINT_NAME=pulsecommerce-churn-v1 \
#     pulsecommerce-api
# =============================================================================

# ── Stage 1: Builder ─────────────────────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /build

# System deps for building Python packages with C extensions
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create isolated virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy only dependency files first (layer cache optimisation)
COPY requirements.txt ./

# Install runtime dependencies only (no dev extras)
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
        fastapi==0.111.0 \
        uvicorn[standard]==0.29.0 \
        pydantic==2.7.1 \
        boto3==1.34.0 \
        structlog==24.1.0 \
        prometheus-client==0.20.0 \
        slowapi==0.1.9 \
        httpx==0.27.0


# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

LABEL org.opencontainers.image.title="PulseCommerce Analytics API"
LABEL org.opencontainers.image.description="FastAPI serving layer — Redshift + SageMaker"
LABEL org.opencontainers.image.version="1.0.0"

# Non-root user for container security
RUN groupadd --gid 1000 appuser && \
    useradd --uid 1000 --gid appuser --shell /bin/bash --create-home appuser

WORKDIR /app

# Copy virtual environment from builder
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy application source
# Build context is the project root (pulsecommerce-data-platform/)
COPY analytics/api/ ./analytics/api/

# Create __init__.py files to make packages importable
RUN touch analytics/__init__.py analytics/api/__init__.py analytics/api/routers/__init__.py

# Switch to non-root user
USER appuser

# Environment defaults (overridden at runtime via ECS task definition / env vars)
ENV API_HOST="0.0.0.0" \
    API_PORT="8000" \
    AWS_REGION="us-east-1" \
    REDSHIFT_WORKGROUP="pulsecommerce" \
    REDSHIFT_DATABASE="analytics" \
    FEATURE_GROUP_NAME="pulsecommerce-user-behavioral" \
    CHURN_ENDPOINT_NAME="pulsecommerce-churn-v1" \
    FRAUD_SCORE_THRESHOLD="0.7" \
    CORS_ORIGINS="*" \
    PYTHONUNBUFFERED="1" \
    PYTHONDONTWRITEBYTECODE="1"

EXPOSE 8000

# Health check — ALB integration
HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"

# Uvicorn with 2 workers per CPU core (adjusted via UVICORN_WORKERS env var)
CMD ["sh", "-c", \
    "uvicorn analytics.api.main:app \
     --host ${API_HOST} \
     --port ${API_PORT} \
     --workers ${UVICORN_WORKERS:-2} \
     --log-level info \
     --access-log \
     --proxy-headers \
     --forwarded-allow-ips='*'"]
