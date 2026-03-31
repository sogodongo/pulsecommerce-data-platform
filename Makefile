.DEFAULT_GOAL := help
PYTHON        := python3.11
PIP           := $(PYTHON) -m pip
PROJECT_ROOT  := $(shell pwd)

# ── Colors ────────────────────────────────────────────────────────────────────
CYAN  := \033[0;36m
RESET := \033[0m

.PHONY: help install install-dev lint format typecheck test test-unit \
        test-integration test-cov dbt-compile dbt-run dbt-test \
        gx-validate flink-package glue-deploy tf-init tf-plan tf-apply \
        docker-api-build docker-api-run clean

# ── Help ──────────────────────────────────────────────────────────────────────
help:
	@echo ""
	@echo "$(CYAN)PulseCommerce Data Platform — Available Commands$(RESET)"
	@echo "────────────────────────────────────────────────"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(CYAN)%-25s$(RESET) %s\n", $$1, $$2}'
	@echo ""

# ── Setup ─────────────────────────────────────────────────────────────────────
install: ## Install production dependencies
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt

install-dev: ## Install all dev dependencies (includes prod)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements-dev.txt
	pre-commit install

# ── Code Quality ──────────────────────────────────────────────────────────────
lint: ## Run ruff linter across all Python sources
	ruff check ingestion/ processing/ analytics/ ml/ tests/

format: ## Auto-format code with ruff
	ruff format ingestion/ processing/ analytics/ ml/ tests/

typecheck: ## Run mypy type checks
	mypy ingestion/ processing/ analytics/ ml/ --ignore-missing-imports

# ── Testing ───────────────────────────────────────────────────────────────────
test: ## Run all tests
	pytest tests/ -v

test-unit: ## Run unit tests only (no external deps)
	pytest tests/unit/ -v -m unit

test-integration: ## Run integration tests (requires Docker / localstack)
	pytest tests/integration/ -v -m integration

test-cov: ## Run tests with coverage report
	pytest tests/ --cov=. --cov-report=term-missing --cov-report=html:htmlcov
	@echo "Coverage report: htmlcov/index.html"

# ── dbt ───────────────────────────────────────────────────────────────────────
dbt-deps: ## Install dbt packages
	cd analytics/dbt && dbt deps

dbt-compile: ## Compile dbt models (validates SQL, no execution)
	cd analytics/dbt && dbt compile --profiles-dir profiles/ci

dbt-run: ## Run dbt Gold models against prod
	cd analytics/dbt && dbt run --models gold --profiles-dir profiles/prod --target prod

dbt-test: ## Run dbt schema + data tests
	cd analytics/dbt && dbt test --profiles-dir profiles/prod --target prod

dbt-docs: ## Generate and serve dbt docs
	cd analytics/dbt && dbt docs generate --profiles-dir profiles/prod && dbt docs serve

# ── Data Quality ──────────────────────────────────────────────────────────────
gx-validate: ## Run Great Expectations validation suites (CI sample data)
	$(PYTHON) tests/quality/run_gx_ci_suite.py

# ── Flink ─────────────────────────────────────────────────────────────────────
flink-package: ## Package PyFlink jobs into a zip for MSF deployment
	cd processing/flink && zip -r flink_jobs.zip . -x "*.pyc" "__pycache__/*"
	@echo "Packaged: processing/flink/flink_jobs.zip"

# ── Glue ──────────────────────────────────────────────────────────────────────
glue-deploy: ## Upload Glue scripts to S3
	aws s3 sync processing/glue/ s3://$(GLUE_SCRIPTS_BUCKET)/glue/ \
	  --exclude "*.pyc" --exclude "__pycache__/*"
	@echo "Glue scripts synced to s3://$(GLUE_SCRIPTS_BUCKET)/glue/"

# ── Terraform ─────────────────────────────────────────────────────────────────
tf-init: ## Initialize Terraform
	cd infrastructure/terraform && terraform init

tf-plan: ## Run Terraform plan
	cd infrastructure/terraform && terraform plan -out=tfplan

tf-apply: ## Apply Terraform plan (requires tfplan)
	cd infrastructure/terraform && terraform apply tfplan

tf-destroy: ## Destroy all Terraform-managed infrastructure (DANGEROUS)
	@echo "WARNING: This will destroy all infrastructure. Press Ctrl+C to cancel."
	@sleep 5
	cd infrastructure/terraform && terraform destroy

# ── FastAPI ───────────────────────────────────────────────────────────────────
docker-api-build: ## Build FastAPI Docker image
	docker build -t pulsecommerce-data-api:latest analytics/api/

docker-api-run: ## Run FastAPI locally on port 8080
	docker run --rm -p 8080:8080 \
	  --env-file .env \
	  pulsecommerce-data-api:latest

api-dev: ## Run FastAPI with hot reload (local dev)
	uvicorn analytics.api.main:app --reload --host 0.0.0.0 --port 8080

# ── Cleanup ───────────────────────────────────────────────────────────────────
clean: ## Remove build artifacts, caches, coverage reports
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null; true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	rm -rf .pytest_cache htmlcov .mypy_cache .ruff_cache
	rm -rf processing/flink/flink_jobs.zip
	rm -rf analytics/dbt/target analytics/dbt/dbt_packages
	@echo "Cleaned build artifacts"
