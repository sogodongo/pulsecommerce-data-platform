from __future__ import annotations

import os
import subprocess
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.trigger_rule import TriggerRule

AWS_REGION = Variable.get("aws_region", default_var="us-east-1")
SNS_ALERT_ARN = Variable.get("data_quality_sns_arn", default_var="")
REDSHIFT_WORKGROUP = Variable.get("redshift_workgroup", default_var="pulsecommerce")
REDSHIFT_DATABASE = Variable.get("redshift_database", default_var="analytics")

DBT_PROJECT_DIR = "/usr/local/airflow/dbt"
DBT_PROFILES_DIR = "/usr/local/airflow/dbt/profiles"
DBT_TARGET = "prod"

# Set to None to run dbt on MWAA worker via subprocess instead
DBT_GLUE_JOB = Variable.get("dbt_glue_job_name", default_var=None)

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


def _run_dbt(command: list[str], **ctx: Any) -> dict[str, Any]:
    """Run a dbt command via subprocess on the MWAA worker. Raises on non-zero exit."""
    import logging
    log = logging.getLogger(__name__)

    full_cmd = [
        "dbt", *command,
        "--profiles-dir", DBT_PROFILES_DIR,
        "--project-dir", DBT_PROJECT_DIR,
        "--target", DBT_TARGET,
        "--no-use-colors",
    ]
    log.info("Running: %s", " ".join(full_cmd))

    result = subprocess.run(full_cmd, capture_output=True, text=True, timeout=1800)

    log.info("dbt stdout (tail):\n%s", result.stdout[-3000:])
    if result.stderr:
        log.warning("dbt stderr:\n%s", result.stderr[-1000:])

    if result.returncode != 0:
        raise RuntimeError(
            f"dbt command failed (exit {result.returncode}): {' '.join(command)}\n"
            f"stderr: {result.stderr[-500:]}"
        )

    return {"returncode": result.returncode, "stdout_tail": result.stdout[-500:]}


def _dbt_run_select(models: str, **ctx: Any) -> dict[str, Any]:
    return _run_dbt(["run", "--select", models], **ctx)


def _dbt_test_gold(**ctx: Any) -> dict[str, Any]:
    return _run_dbt(["test", "--select", "gold"], **ctx)


def _dbt_deps(**ctx: Any) -> dict[str, Any]:
    return _run_dbt(["deps"], **ctx)


def _dbt_compile(**ctx: Any) -> dict[str, Any]:
    return _run_dbt(["compile", "--select", "gold"], **ctx)


REDSHIFT_MV_REFRESH_SQL = """
REFRESH MATERIALIZED VIEW analytics.mv_daily_revenue;
REFRESH MATERIALIZED VIEW analytics.mv_user_ltv_summary;
REFRESH MATERIALIZED VIEW analytics.mv_product_performance;
"""

with DAG(
    dag_id="gold_models",
    description="Silver → Gold dbt: dims → facts → agg → dbt test → Redshift MV refresh",
    schedule_interval=None,    # triggered by silver_refresh via TriggerDagRunOperator
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["gold", "dbt", "redshift", "kimball"],
    doc_md="""
## Gold Models DAG

Triggered after every successful `silver_refresh` DAG run.

### Execution order
```
sensor → deps+compile → [dim_users, dim_products, dim_channels, dim_geography, dim_date]
       → [fct_orders, fct_sessions]
       → agg_daily_metrics
       → dbt_test
       → redshift_mv_refresh
       → end
```

### dbt target
Runs against `prod` profile (dbt-glue adapter, Spark Iceberg catalog).
Redshift Serverless materialized views refreshed after dbt completes.
    """,
) as dag:

    start = EmptyOperator(task_id="start")

    wait_for_silver = ExternalTaskSensor(
        task_id="wait_for_silver_refresh",
        external_dag_id="silver_refresh",
        external_task_id="end",
        allowed_states=["success"],
        failed_states=["failed", "upstream_failed"],
        timeout=3600,
        poke_interval=60,
        mode="reschedule",     # release worker slot while waiting
    )

    dbt_deps = PythonOperator(
        task_id="dbt_deps",
        python_callable=_dbt_deps,
    )

    dbt_compile = PythonOperator(
        task_id="dbt_compile",
        python_callable=_dbt_compile,
    )

    dim_users = PythonOperator(
        task_id="dim_users",
        python_callable=_dbt_run_select,
        op_kwargs={"models": "dim_users"},
    )

    dim_products = PythonOperator(
        task_id="dim_products",
        python_callable=_dbt_run_select,
        op_kwargs={"models": "dim_products"},
    )

    dim_channels = PythonOperator(
        task_id="dim_channels",
        python_callable=_dbt_run_select,
        op_kwargs={"models": "dim_channels"},
    )

    dim_geography = PythonOperator(
        task_id="dim_geography",
        python_callable=_dbt_run_select,
        op_kwargs={"models": "dim_geography"},
    )

    dim_date = PythonOperator(
        task_id="dim_date",
        python_callable=_dbt_run_select,
        op_kwargs={"models": "dim_date"},
    )

    dims_done = EmptyOperator(task_id="dims_done")

    fct_orders = PythonOperator(
        task_id="fct_orders",
        python_callable=_dbt_run_select,
        op_kwargs={"models": "fct_orders"},
    )

    fct_sessions = PythonOperator(
        task_id="fct_sessions",
        python_callable=_dbt_run_select,
        op_kwargs={"models": "fct_sessions"},
    )

    facts_done = EmptyOperator(task_id="facts_done")

    agg_daily = PythonOperator(
        task_id="agg_daily_metrics",
        python_callable=_dbt_run_select,
        op_kwargs={"models": "agg_daily_metrics"},
    )

    dbt_test = PythonOperator(
        task_id="dbt_test_gold",
        python_callable=_dbt_test_gold,
    )

    redshift_mv_refresh = RedshiftDataOperator(
        task_id="redshift_mv_refresh",
        cluster_identifier=None,                # Serverless
        workgroup_name=REDSHIFT_WORKGROUP,
        database=REDSHIFT_DATABASE,
        sql=REDSHIFT_MV_REFRESH_SQL,
        wait_for_completion=True,
        aws_conn_id="aws_default",
        poll_interval=15,
    )

    failure_alert = SnsPublishOperator(
        task_id="failure_alert",
        target_arn=SNS_ALERT_ARN,
        message=(
            "Gold models DAG FAILED. DAG: {{ dag.dag_id }}, "
            "Run: {{ run_id }}, Logical date: {{ ds }}. "
            "BI dashboards and API may serve stale data."
        ),
        subject="[PulseCommerce] Gold Models DAG FAILED",
        aws_conn_id="aws_default",
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    start >> wait_for_silver >> dbt_deps >> dbt_compile

    dbt_compile >> [dim_users, dim_products, dim_channels, dim_geography, dim_date]
    [dim_users, dim_products, dim_channels, dim_geography, dim_date] >> dims_done

    dims_done >> [fct_orders, fct_sessions] >> facts_done
    facts_done >> agg_daily >> dbt_test >> redshift_mv_refresh >> end

    # Failure alert wires to all compute tasks
    [dbt_deps, dbt_compile, dim_users, dim_products, dim_channels, dim_geography, dim_date,
     fct_orders, fct_sessions, agg_daily, dbt_test, redshift_mv_refresh] >> failure_alert
