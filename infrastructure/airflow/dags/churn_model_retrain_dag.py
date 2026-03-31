from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta
from typing import Any

import boto3
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.sagemaker import (
    SageMakerTrainingOperator,
)
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from airflow.providers.amazon.aws.sensors.sagemaker import SageMakerTrainingSensor
from airflow.utils.trigger_rule import TriggerRule

AWS_REGION = Variable.get("aws_region", default_var="us-east-1")
AWS_ACCOUNT_ID = Variable.get("aws_account_id")
SAGEMAKER_ROLE_ARN = Variable.get("sagemaker_role_arn")
FEATURE_GROUP_NAME = Variable.get("feature_group_name", default_var="pulsecommerce-user-behavioral")
CHURN_ENDPOINT_NAME = Variable.get("churn_endpoint_name", default_var="pulsecommerce-churn-v1")
LAKEHOUSE_BUCKET = Variable.get("lakehouse_bucket")
SNS_ALERT_ARN = Variable.get("data_quality_sns_arn", default_var="")

MIN_AUC_ROC = float(Variable.get("churn_min_auc_roc", default_var="0.80"))
MIN_PRECISION = float(Variable.get("churn_min_precision", default_var="0.65"))

XGBOOST_HYPERPARAMS = {
    "max_depth": "6",
    "eta": "0.3",
    "gamma": "1",
    "min_child_weight": "6",
    "subsample": "0.8",
    "objective": "binary:logistic",
    "num_round": "200",
    "eval_metric": "auc",
    "scale_pos_weight": "3",   # class imbalance: ~3:1 non-churn:churn
}

DEFAULT_ARGS = {
    "owner": "ml-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
}


def _export_training_dataset(**ctx: Any) -> dict[str, str]:
    """
    Export labeled training data from Feature Store offline store via Athena.
    Pushes train/val S3 paths to XCom.
    """
    import logging
    log = logging.getLogger(__name__)

    ds = ctx["ds"]
    sm_client = boto3.client("sagemaker", region_name=AWS_REGION)
    athena_client = boto3.client("athena", region_name=AWS_REGION)

    # Resolve offline store S3 URI from Feature Group metadata
    fg_meta = sm_client.describe_feature_group(FeatureGroupName=FEATURE_GROUP_NAME)
    offline_s3_uri = fg_meta["OfflineStoreConfig"]["S3StorageConfig"]["ResolvedOutputS3Uri"]

    output_prefix = f"s3://{LAKEHOUSE_BUCKET}/ml/training-data/churn/{ds}/"

    query = f"""
    SELECT
        user_id_hashed,
        days_since_last_order,
        order_frequency_30d,
        avg_order_value_usd,
        total_ltv_usd,
        session_count_7d,
        cart_abandonment_rate,
        preferred_category_encoded,
        channel_group_encoded,
        is_gdpr_scope,
        CAST(churned_30d AS INTEGER) AS label
    FROM "{FEATURE_GROUP_NAME.replace('-', '_')}"
    WHERE write_time >= DATE_ADD('day', -90, DATE '{ds}')
      AND is_current = true
    """

    query_execution = athena_client.start_query_execution(
        QueryString=query,
        ResultConfiguration={
            "OutputLocation": f"{output_prefix}athena-results/",
            "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
        },
        WorkGroup="pulsecommerce-ml",
    )
    execution_id = query_execution["QueryExecutionId"]

    for _ in range(60):
        status = athena_client.get_query_execution(QueryExecutionId=execution_id)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(10)

    if state != "SUCCEEDED":
        raise RuntimeError(f"Athena query {execution_id} ended with state {state}")

    train_s3 = f"{output_prefix}train/data.csv"
    val_s3 = f"{output_prefix}validation/data.csv"

    log.info("Training data exported: %s", output_prefix)
    ctx["ti"].xcom_push(key="train_s3", value=train_s3)
    ctx["ti"].xcom_push(key="val_s3", value=val_s3)
    ctx["ti"].xcom_push(key="output_prefix", value=output_prefix)

    return {"train_s3": train_s3, "val_s3": val_s3}


def _build_training_job_config(**ctx: Any) -> dict[str, Any]:
    ti = ctx["ti"]
    ds = ctx["ds"].replace("-", "")
    train_s3 = ti.xcom_pull(task_ids="export_training_dataset", key="train_s3")
    val_s3 = ti.xcom_pull(task_ids="export_training_dataset", key="val_s3")
    output_prefix = ti.xcom_pull(task_ids="export_training_dataset", key="output_prefix")

    job_name = f"pulsecommerce-churn-{ds}-{ctx['run_id'][:8]}"

    xgb_image = (
        f"683313688378.dkr.ecr.{AWS_REGION}.amazonaws.com/"
        f"sagemaker-xgboost:1.7-1"
    )

    config = {
        "TrainingJobName": job_name,
        "AlgorithmSpecification": {
            "TrainingImage": xgb_image,
            "TrainingInputMode": "File",
        },
        "RoleArn": SAGEMAKER_ROLE_ARN,
        "InputDataConfig": [
            {
                "ChannelName": "train",
                "DataSource": {"S3DataSource": {"S3DataType": "S3Prefix", "S3Uri": train_s3, "S3DataDistributionType": "FullyReplicated"}},
                "ContentType": "text/csv",
            },
            {
                "ChannelName": "validation",
                "DataSource": {"S3DataSource": {"S3DataType": "S3Prefix", "S3Uri": val_s3, "S3DataDistributionType": "FullyReplicated"}},
                "ContentType": "text/csv",
            },
        ],
        "OutputDataConfig": {"S3OutputPath": f"{output_prefix}model/"},
        "ResourceConfig": {
            "InstanceType": "ml.m5.2xlarge",
            "InstanceCount": 1,
            "VolumeSizeInGB": 50,
        },
        "HyperParameters": XGBOOST_HYPERPARAMS,
        "StoppingCondition": {"MaxRuntimeInSeconds": 7200},
        "Tags": [
            {"Key": "Project", "Value": "PulseCommerce"},
            {"Key": "Pipeline", "Value": "ChurnRetrain"},
            {"Key": "LogicalDate", "Value": ctx["ds"]},
        ],
    }

    ti.xcom_push(key="training_job_name", value=job_name)
    ti.xcom_push(key="model_output_s3", value=f"{output_prefix}model/{job_name}/output/model.tar.gz")
    return config


def _evaluate_model_metrics(**ctx: Any) -> None:
    """Enforce quality gate: AUC-ROC >= MIN_AUC_ROC. Raises to block deployment."""
    import logging
    log = logging.getLogger(__name__)

    ti = ctx["ti"]
    job_name = ti.xcom_pull(task_ids="build_training_config", key="training_job_name")

    sm_client = boto3.client("sagemaker", region_name=AWS_REGION)
    response = sm_client.describe_training_job(TrainingJobName=job_name)

    metrics = {
        m["MetricName"]: m["Value"]
        for m in response.get("FinalMetricDataList", [])
    }
    log.info("Training metrics: %s", metrics)

    auc = metrics.get("validation:auc", 0.0)
    if auc < MIN_AUC_ROC:
        raise ValueError(
            f"Model quality gate FAILED: AUC-ROC={auc:.4f} < threshold={MIN_AUC_ROC}. "
            f"Deployment blocked. All metrics: {metrics}"
        )

    log.info("Model quality gate PASSED: AUC-ROC=%.4f", auc)
    ti.xcom_push(key="auc_roc", value=auc)
    ti.xcom_push(key="all_metrics", value=metrics)


def _register_model(**ctx: Any) -> str:
    import logging
    log = logging.getLogger(__name__)

    ti = ctx["ti"]
    model_s3 = ti.xcom_pull(task_ids="build_training_config", key="model_output_s3")
    auc = ti.xcom_pull(task_ids="evaluate_model_metrics", key="auc_roc")
    metrics = ti.xcom_pull(task_ids="evaluate_model_metrics", key="all_metrics")

    sm_client = boto3.client("sagemaker", region_name=AWS_REGION)
    xgb_image = f"683313688378.dkr.ecr.{AWS_REGION}.amazonaws.com/sagemaker-xgboost:1.7-1"

    response = sm_client.create_model_package(
        ModelPackageGroupName="pulsecommerce-churn",
        ModelPackageDescription=f"Churn model retrained on {ctx['ds']}. AUC-ROC={auc:.4f}",
        InferenceSpecification={
            "Containers": [{"Image": xgb_image, "ModelDataUrl": model_s3}],
            "SupportedContentTypes": ["text/csv"],
            "SupportedResponseMIMETypes": ["text/csv"],
        },
        ModelApprovalStatus="Approved",
        ModelMetrics={
            "ModelQuality": {
                "Statistics": {
                    "ContentType": "application/json",
                    "S3Uri": f"s3://{LAKEHOUSE_BUCKET}/ml/metrics/{ctx['ds']}/metrics.json",
                }
            }
        },
    )

    arn = response["ModelPackageArn"]
    log.info("Registered model package: %s", arn)
    ti.xcom_push(key="model_package_arn", value=arn)
    return arn


def _deploy_endpoint(**ctx: Any) -> None:
    """Blue/green: create new endpoint config, update live endpoint. SageMaker handles traffic."""
    import logging
    log = logging.getLogger(__name__)

    ti = ctx["ti"]
    model_package_arn = ti.xcom_pull(task_ids="register_model", key="model_package_arn")
    ds = ctx["ds"].replace("-", "")

    sm_client = boto3.client("sagemaker", region_name=AWS_REGION)

    model_name = f"pulsecommerce-churn-model-{ds}"
    sm_client.create_model(
        ModelName=model_name,
        ExecutionRoleArn=SAGEMAKER_ROLE_ARN,
        Containers=[{"ModelPackageName": model_package_arn}],
    )

    config_name = f"pulsecommerce-churn-config-{ds}"
    sm_client.create_endpoint_config(
        EndpointConfigName=config_name,
        ProductionVariants=[
            {
                "VariantName": "primary",
                "ModelName": model_name,
                "InitialInstanceCount": 2,
                "InstanceType": "ml.c5.xlarge",
                "InitialVariantWeight": 1.0,
            }
        ],
        Tags=[{"Key": "Project", "Value": "PulseCommerce"}],
    )

    # Save current config for rollback before switching
    try:
        current_ep = sm_client.describe_endpoint(EndpointName=CHURN_ENDPOINT_NAME)
        ti.xcom_push(key="previous_config", value=current_ep["EndpointConfigName"])
    except sm_client.exceptions.ClientError:
        pass   # first deploy — no previous config to rollback to

    try:
        sm_client.update_endpoint(
            EndpointName=CHURN_ENDPOINT_NAME,
            EndpointConfigName=config_name,
        )
        log.info("Updated endpoint %s → config %s", CHURN_ENDPOINT_NAME, config_name)
    except sm_client.exceptions.ClientError:
        sm_client.create_endpoint(
            EndpointName=CHURN_ENDPOINT_NAME,
            EndpointConfigName=config_name,
        )
        log.info("Created endpoint %s", CHURN_ENDPOINT_NAME)

    waiter = sm_client.get_waiter("endpoint_in_service")
    waiter.wait(
        EndpointName=CHURN_ENDPOINT_NAME,
        WaiterConfig={"Delay": 30, "MaxAttempts": 40},
    )
    ti.xcom_push(key="new_config", value=config_name)
    log.info("Endpoint %s is InService", CHURN_ENDPOINT_NAME)


def _smoke_test_endpoint(**ctx: Any) -> None:
    """Invoke the new endpoint with a synthetic record. Validates score in [0, 1]."""
    import logging
    log = logging.getLogger(__name__)

    sm_runtime = boto3.client("sagemaker-runtime", region_name=AWS_REGION)

    test_payload = "45,0.5,35.0,120.0,3,0.4,2,1,0"
    response = sm_runtime.invoke_endpoint(
        EndpointName=CHURN_ENDPOINT_NAME,
        ContentType="text/csv",
        Body=test_payload,
    )
    score = float(response["Body"].read().decode("utf-8").strip())

    if not 0.0 <= score <= 1.0:
        raise ValueError(f"Smoke test FAILED: endpoint returned out-of-range score {score}")

    log.info("Smoke test PASSED: churn_score=%.4f", score)


def _rollback_endpoint(**ctx: Any) -> None:
    """Restore previous endpoint config on smoke test failure. No-op on first deploy."""
    import logging
    log = logging.getLogger(__name__)

    ti = ctx["ti"]
    previous_config = ti.xcom_pull(task_ids="deploy_endpoint", key="previous_config")

    if not previous_config:
        log.warning("No previous config found — cannot rollback. Manual intervention required.")
        return

    sm_client = boto3.client("sagemaker", region_name=AWS_REGION)
    sm_client.update_endpoint(
        EndpointName=CHURN_ENDPOINT_NAME,
        EndpointConfigName=previous_config,
    )
    log.info("Rolled back endpoint %s → config %s", CHURN_ENDPOINT_NAME, previous_config)


with DAG(
    dag_id="churn_model_retrain",
    description="Weekly churn model retrain: Feature Store export → SageMaker train → gate → register → deploy → smoke test",
    schedule_interval="0 2 * * 0",    # Every Sunday at 02:00 UTC
    start_date=datetime(2024, 1, 7),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["ml", "sagemaker", "churn", "xgboost"],
    doc_md="""
## Churn Model Retrain DAG

Weekly retraining pipeline for the user churn XGBoost model.

### Quality gate
- AUC-ROC must be >= `churn_min_auc_roc` Airflow Variable (default 0.80)
- Failed gate blocks deployment and triggers SNS alert

### Rollback
If smoke test fails after deployment, the previous endpoint config is automatically
restored via the `rollback_endpoint` task.
    """,
) as dag:

    start = EmptyOperator(task_id="start")

    export_training_dataset = PythonOperator(
        task_id="export_training_dataset",
        python_callable=_export_training_dataset,
    )

    build_training_config = PythonOperator(
        task_id="build_training_config",
        python_callable=_build_training_job_config,
    )

    launch_training = SageMakerTrainingOperator(
        task_id="launch_training_job",
        config="{{ ti.xcom_pull(task_ids='build_training_config') }}",
        aws_conn_id="aws_default",
        wait_for_completion=True,
        print_log=True,
        check_interval=60,
    )

    evaluate_model_metrics = PythonOperator(
        task_id="evaluate_model_metrics",
        python_callable=_evaluate_model_metrics,
    )

    register_model = PythonOperator(
        task_id="register_model",
        python_callable=_register_model,
    )

    deploy_endpoint = PythonOperator(
        task_id="deploy_endpoint",
        python_callable=_deploy_endpoint,
    )

    smoke_test = PythonOperator(
        task_id="smoke_test_endpoint",
        python_callable=_smoke_test_endpoint,
    )

    rollback = PythonOperator(
        task_id="rollback_endpoint",
        python_callable=_rollback_endpoint,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    success_notify = SnsPublishOperator(
        task_id="success_notify",
        target_arn=SNS_ALERT_ARN,
        message=(
            "Churn model retrain SUCCEEDED. "
            "AUC-ROC={{ ti.xcom_pull(task_ids='evaluate_model_metrics', key='auc_roc') }}, "
            "Endpoint: {{ var.value.churn_endpoint_name }}, "
            "Logical date: {{ ds }}"
        ),
        subject="[PulseCommerce] Churn Model Retrain Succeeded",
        aws_conn_id="aws_default",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    failure_alert = SnsPublishOperator(
        task_id="failure_alert",
        target_arn=SNS_ALERT_ARN,
        message=(
            "Churn model retrain FAILED. DAG: {{ dag.dag_id }}, "
            "Run: {{ run_id }}, Logical date: {{ ds }}. "
            "Check task logs for quality gate or deployment errors."
        ),
        subject="[PulseCommerce] Churn Model Retrain FAILED",
        aws_conn_id="aws_default",
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    start >> export_training_dataset >> build_training_config >> launch_training
    launch_training >> evaluate_model_metrics >> register_model >> deploy_endpoint
    deploy_endpoint >> smoke_test >> success_notify >> end

    # Rollback fires if deploy or smoke test fails
    [deploy_endpoint, smoke_test] >> rollback

    # Failure alert fires on any upstream failure
    [export_training_dataset, build_training_config, launch_training,
     evaluate_model_metrics, register_model, deploy_endpoint,
     smoke_test, rollback] >> failure_alert
