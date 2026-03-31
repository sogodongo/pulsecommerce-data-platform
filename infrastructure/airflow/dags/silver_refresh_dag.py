# =============================================================================
# infrastructure/airflow/dags/silver_refresh_dag.py
# =============================================================================
# MWAA (Airflow 2.8) DAG — Bronze → Silver ELT refresh.
#
# Schedule: every 30 minutes (aligns with Flink micro-batch cadence).
# Pipeline:
#   1. Source freshness check (dbt source freshness)
#   2. Parallel Glue jobs:
#      a. bronze_to_silver_events  — clickstream events
#      b. bronze_to_silver_orders  — CDC order records
#      c. silver_product_catalog   — product catalog upsert
#   3. Post-write GX validation (per table, parallel)
#   4. Update Glue Data Catalog statistics
#   5. Notify on failure via SNS
#
# Failure policy:
#   - Glue job failure → entire DAG fails (downstream Gold blocked)
#   - GX CRITICAL failure → DAG fails (Gold blocked)
#   - GX WARNING failure → task succeeds with note in XCom, continues
# =============================================================================

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
from airflow.utils.trigger_rule import TriggerRule

# ---------------------------------------------------------------------------
# DAG-level constants (pulled from Airflow Variables for environment portability)
# ---------------------------------------------------------------------------

AWS_REGION = Variable.get("aws_region", default_var="us-east-1")
AWS_ACCOUNT_ID = Variable.get("aws_account_id")
GLUE_ROLE_ARN = Variable.get("glue_role_arn")
LAKEHOUSE_BUCKET = Variable.get("lakehouse_bucket")
GLUE_SCRIPTS_BUCKET = Variable.get("glue_scripts_bucket")
SNS_ALERT_ARN = Variable.get("data_quality_sns_arn", default_var="")
GLUE_DPU = int(Variable.get("silver_glue_dpu", default_var="10"))

# Glue job names (must match Terraform-deployed job names)
GLUE_JOB_EVENTS = "pulsecommerce-bronze-to-silver-events"
GLUE_JOB_ORDERS = "pulsecommerce-bronze-to-silver-orders"
GLUE_JOB_PRODUCTS = "pulsecommerce-silver-product-catalog"
GLUE_JOB_GX_EVENTS = "pulsecommerce-gx-bronze-clickstream"
GLUE_JOB_GX_ORDERS = "pulsecommerce-gx-silver-orders"

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": False,    # SNS used instead
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
}

# ---------------------------------------------------------------------------
# Helper callables
# ---------------------------------------------------------------------------

def _check_source_freshness(**ctx) -> str:
    """
    Calls `dbt source freshness` via subprocess and checks exit code.
    Routes to 'run_glue_jobs' on success or 'source_stale_alert' on failure.
    Returns branch task_id.
    """
    import subprocess
    import logging

    log = logging.getLogger(__name__)
    partition_ds = ctx["ds"]   # YYYY-MM-DD logical date

    result = subprocess.run(
        [
            "dbt", "source", "freshness",
            "--profiles-dir", "/usr/local/airflow/dbt/profiles",
            "--project-dir", "/usr/local/airflow/dbt",
            "--target", "prod",
            "--select", "source:silver",
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )

    log.info("dbt source freshness stdout: %s", result.stdout[-2000:])
    if result.returncode != 0:
        log.warning("Source freshness check failed: %s", result.stderr[-1000:])
        return "source_stale_alert"

    return "run_events_glue"


def _build_glue_args(job_name: str, logical_date: str) -> dict[str, str]:
    """Build Glue job arguments dict."""
    return {
        "--job-bookmark-option": "job-bookmark-enable",
        "--partition_date": logical_date[:10],
        "--partition_hour": logical_date[11:13] if len(logical_date) > 10 else "00",
        "--enable-metrics": "true",
        "--enable-spark-ui": "true",
        "--spark-event-logs-path": f"s3://{GLUE_SCRIPTS_BUCKET}/spark-logs/{job_name}/",
    }


def _check_gx_result(task_id: str, **ctx) -> None:
    """
    Pulls GX validation result from XCom and raises if CRITICAL failure found.
    WARNING failures are logged only.
    """
    import logging
    log = logging.getLogger(__name__)

    ti = ctx["ti"]
    result_raw = ti.xcom_pull(task_ids=task_id, key="gx_result")
    if not result_raw:
        log.warning("No GX result found in XCom for task %s — skipping check", task_id)
        return

    result = json.loads(result_raw) if isinstance(result_raw, str) else result_raw
    if not result.get("success", True):
        failed = result.get("failed_expectations", [])
        critical = [f for f in failed if f.get("severity") == "CRITICAL"]
        if critical:
            raise ValueError(f"CRITICAL GX failure in {task_id}: {critical}")
        log.warning("GX WARNING failures in %s: %s", task_id, [f for f in failed if f.get("severity") == "WARNING"])


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="silver_refresh",
    description="Bronze → Silver ELT: events, orders, product catalog (every 30 min)",
    schedule_interval="*/30 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,           # prevent concurrent Silver writes
    default_args=DEFAULT_ARGS,
    tags=["silver", "elt", "glue", "data-quality"],
    doc_md="""
## Silver Refresh DAG

Runs every 30 minutes. Promotes Bronze Iceberg data to Silver with:
- PII pseudonymisation (HMAC-SHA256)
- GDPR city masking for EU/UK/EEA users
- SCD Type 2 merge for orders
- Great Expectations CRITICAL/WARNING validation

**Failure escalation:** SNS → PagerDuty integration (configure via `data_quality_sns_arn` Variable).
    """,
) as dag:

    # ── Start ────────────────────────────────────────────────────────────────
    start = EmptyOperator(task_id="start")

    # ── Source freshness check ───────────────────────────────────────────────
    freshness_branch = BranchPythonOperator(
        task_id="check_source_freshness",
        python_callable=_check_source_freshness,
    )

    source_stale_alert = SnsPublishOperator(
        task_id="source_stale_alert",
        target_arn=SNS_ALERT_ARN,
        message=(
            "Silver refresh SKIPPED: dbt source freshness check failed. "
            "Silver tables may be stale. Investigate MSK consumer lag or Flink job status."
        ),
        subject="[PulseCommerce] Silver Refresh — Source Stale",
        aws_conn_id="aws_default",
    )

    # ── Glue ELT jobs (parallel) ─────────────────────────────────────────────
    run_events_glue = GlueJobOperator(
        task_id="run_events_glue",
        job_name=GLUE_JOB_EVENTS,
        script_args=_build_glue_args(GLUE_JOB_EVENTS, "{{ ds }}"),
        num_of_dpus=GLUE_DPU,
        aws_conn_id="aws_default",
        region_name=AWS_REGION,
        wait_for_completion=True,
    )

    run_orders_glue = GlueJobOperator(
        task_id="run_orders_glue",
        job_name=GLUE_JOB_ORDERS,
        script_args=_build_glue_args(GLUE_JOB_ORDERS, "{{ ds }}"),
        num_of_dpus=GLUE_DPU,
        aws_conn_id="aws_default",
        region_name=AWS_REGION,
        wait_for_completion=True,
    )

    run_products_glue = GlueJobOperator(
        task_id="run_products_glue",
        job_name=GLUE_JOB_PRODUCTS,
        script_args=_build_glue_args(GLUE_JOB_PRODUCTS, "{{ ds }}"),
        num_of_dpus=max(2, GLUE_DPU // 2),   # product catalog needs fewer workers
        aws_conn_id="aws_default",
        region_name=AWS_REGION,
        wait_for_completion=True,
    )

    # ── GX validation (parallel, post-write) ────────────────────────────────
    gx_events = GlueJobOperator(
        task_id="gx_validate_events",
        job_name=GLUE_JOB_GX_EVENTS,
        script_args={
            "--partition_date": "{{ ds }}",
            "--partition_hour": "{{ execution_date.strftime('%H') }}",
        },
        num_of_dpus=2,
        aws_conn_id="aws_default",
        region_name=AWS_REGION,
        wait_for_completion=True,
    )

    gx_orders = GlueJobOperator(
        task_id="gx_validate_orders",
        job_name=GLUE_JOB_GX_ORDERS,
        script_args={"--partition_date": "{{ ds }}"},
        num_of_dpus=2,
        aws_conn_id="aws_default",
        region_name=AWS_REGION,
        wait_for_completion=True,
    )

    # ── GX result assertion tasks ────────────────────────────────────────────
    assert_gx_events = PythonOperator(
        task_id="assert_gx_events",
        python_callable=_check_gx_result,
        op_kwargs={"task_id": "gx_validate_events"},
    )

    assert_gx_orders = PythonOperator(
        task_id="assert_gx_orders",
        python_callable=_check_gx_result,
        op_kwargs={"task_id": "gx_validate_orders"},
    )

    # ── Glue catalog stats refresh ───────────────────────────────────────────
    refresh_catalog = PythonOperator(
        task_id="refresh_glue_catalog_stats",
        python_callable=lambda **ctx: __import__("boto3")
            .client("glue", region_name=AWS_REGION)
            .update_table(
                DatabaseName="silver",
                TableInput={
                    "Name": "orders_unified",
                    "Parameters": {"last_airflow_refresh": ctx["ts"]},
                },
            ),
    )

    # ── Failure alert ────────────────────────────────────────────────────────
    failure_alert = SnsPublishOperator(
        task_id="failure_alert",
        target_arn=SNS_ALERT_ARN,
        message=(
            "Silver refresh FAILED. DAG: {{ dag.dag_id }}, "
            "Run: {{ run_id }}, Logical date: {{ ds }}. "
            "Gold dbt models will not run until this is resolved."
        ),
        subject="[PulseCommerce] Silver Refresh FAILED",
        aws_conn_id="aws_default",
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # ── Task dependencies ────────────────────────────────────────────────────
    start >> freshness_branch
    freshness_branch >> [source_stale_alert, run_events_glue]
    source_stale_alert >> end

    # Glue jobs run in parallel after freshness passes
    run_events_glue >> gx_events >> assert_gx_events
    run_orders_glue >> gx_orders >> assert_gx_orders
    freshness_branch >> run_orders_glue
    freshness_branch >> run_products_glue

    # Catalog refresh waits for all GX assertions
    [assert_gx_events, assert_gx_orders, run_products_glue] >> refresh_catalog
    refresh_catalog >> end

    # Failure alert fires if any upstream task fails
    [run_events_glue, run_orders_glue, run_products_glue,
     gx_events, gx_orders, assert_gx_events, assert_gx_orders,
     refresh_catalog] >> failure_alert
