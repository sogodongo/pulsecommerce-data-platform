from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import boto3

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
AWS_ACCOUNT_ID = os.environ.get("AWS_ACCOUNT_ID", "")
SAGEMAKER_ROLE_ARN = os.environ.get("SAGEMAKER_ROLE_ARN", "")
LAKEHOUSE_BUCKET = os.environ.get("LAKEHOUSE_BUCKET", "")
FEATURE_GROUP_NAME = os.environ.get("FEATURE_GROUP_NAME", "pulsecommerce-user-behavioral")

XGBOOST_IMAGE_URI = (
    f"683313688378.dkr.ecr.{AWS_REGION}.amazonaws.com/sagemaker-xgboost:1.7-1"
)

MIN_AUC_ROC = float(os.environ.get("CHURN_MIN_AUC_ROC", "0.80"))
MIN_PRECISION = float(os.environ.get("CHURN_MIN_PRECISION", "0.65"))

DEFAULT_HYPERPARAMETERS = {
    "max_depth": "6",
    "eta": "0.3",
    "gamma": "1",
    "min_child_weight": "6",
    "subsample": "0.8",
    "colsample_bytree": "0.8",
    "objective": "binary:logistic",
    "num_round": "200",
    "eval_metric": "auc",
    "scale_pos_weight": "3",      # ~3:1 class imbalance in training data
    "seed": "42",
}


@dataclass
class TrainingConfig:
    train_s3_uri: str
    validation_s3_uri: str
    output_s3_prefix: str
    job_name: str = field(
        default_factory=lambda: (
            f"pulsecommerce-churn-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
        )
    )
    instance_type: str = "ml.m5.2xlarge"
    instance_count: int = 1
    volume_size_gb: int = 50
    max_runtime_seconds: int = 7200
    hyperparameters: dict[str, str] = field(default_factory=lambda: dict(DEFAULT_HYPERPARAMETERS))
    tags: list[dict[str, str]] = field(default_factory=list)


@dataclass
class TrainingResult:
    job_name: str
    status: str
    metrics: dict[str, float]
    model_artifact_s3: str
    training_time_seconds: float
    passed_quality_gate: bool
    model_package_arn: str = ""


class ChurnModelTrainer:
    """
    Launches, monitors, and registers a SageMaker XGBoost churn training job.

    Usage:
        trainer = ChurnModelTrainer()
        result = trainer.run(TrainingConfig(train_s3_uri=..., validation_s3_uri=..., output_s3_prefix=...))
    """

    def __init__(self, sm_client: Any | None = None) -> None:
        self._sm = sm_client or boto3.client("sagemaker", region_name=AWS_REGION)

    def run(self, config: TrainingConfig, register: bool = True) -> TrainingResult:
        """Launch training, wait for completion, optionally register in Model Registry."""
        self._launch(config)
        result = self._wait(config)
        if result.status != "Completed":
            raise RuntimeError(f"Training job {config.job_name} ended with status {result.status}")
        if not result.passed_quality_gate:
            raise ValueError(
                f"Quality gate FAILED for {config.job_name}: metrics={result.metrics}. "
                f"Required AUC-ROC >= {MIN_AUC_ROC}"
            )
        if register:
            arn = self._register(config, result)
            result.model_package_arn = arn
        return result

    def _launch(self, config: TrainingConfig) -> None:
        logger.info("Launching training job: %s", config.job_name)
        self._sm.create_training_job(
            TrainingJobName=config.job_name,
            AlgorithmSpecification={
                "TrainingImage": XGBOOST_IMAGE_URI,
                "TrainingInputMode": "File",
                "EnableSageMakerMetricsTimeSeries": True,
                "MetricDefinitions": [
                    {"Name": "validation:auc", "Regex": r"validation-auc:([\d\.]+)"},
                    {"Name": "train:auc",       "Regex": r"train-auc:([\d\.]+)"},
                    {"Name": "validation:error","Regex": r"validation-error:([\d\.]+)"},
                ],
            },
            RoleArn=SAGEMAKER_ROLE_ARN,
            InputDataConfig=[
                {
                    "ChannelName": "train",
                    "DataSource": {
                        "S3DataSource": {
                            "S3DataType": "S3Prefix",
                            "S3Uri": config.train_s3_uri,
                            "S3DataDistributionType": "FullyReplicated",
                        }
                    },
                    "ContentType": "text/csv",
                    "CompressionType": "None",
                },
                {
                    "ChannelName": "validation",
                    "DataSource": {
                        "S3DataSource": {
                            "S3DataType": "S3Prefix",
                            "S3Uri": config.validation_s3_uri,
                            "S3DataDistributionType": "FullyReplicated",
                        }
                    },
                    "ContentType": "text/csv",
                    "CompressionType": "None",
                },
            ],
            OutputDataConfig={"S3OutputPath": config.output_s3_prefix},
            ResourceConfig={
                "InstanceType": config.instance_type,
                "InstanceCount": config.instance_count,
                "VolumeSizeInGB": config.volume_size_gb,
            },
            HyperParameters=config.hyperparameters,
            StoppingCondition={"MaxRuntimeInSeconds": config.max_runtime_seconds},
            EnableManagedSpotTraining=False,
            Tags=config.tags or [{"Key": "Project", "Value": "PulseCommerce"}],
        )
        logger.info("Training job submitted: %s", config.job_name)

    def _wait(self, config: TrainingConfig) -> TrainingResult:
        start_ts = time.time()
        terminal_states = {"Completed", "Failed", "Stopped"}

        while True:
            resp = self._sm.describe_training_job(TrainingJobName=config.job_name)
            status = resp["TrainingJobStatus"]
            elapsed = time.time() - start_ts
            logger.info("Job %s status=%s elapsed=%.0fs", config.job_name, status, elapsed)
            if status in terminal_states:
                break
            time.sleep(30)

        metrics = {
            m["MetricName"]: m["Value"]
            for m in resp.get("FinalMetricDataList", [])
        }
        model_artifact = resp.get("ModelArtifacts", {}).get("S3ModelArtifacts", "")
        auc = metrics.get("validation:auc", 0.0)
        passed = auc >= MIN_AUC_ROC

        if status == "Failed":
            logger.error("Training failed: %s", resp.get("FailureReason"))
        else:
            logger.info("Training completed: metrics=%s, quality_gate=%s", metrics, passed)

        return TrainingResult(
            job_name=config.job_name,
            status=status,
            metrics=metrics,
            model_artifact_s3=model_artifact,
            training_time_seconds=time.time() - start_ts,
            passed_quality_gate=passed,
        )

    def _register(self, config: TrainingConfig, result: TrainingResult) -> str:
        auc = result.metrics.get("validation:auc", 0.0)
        resp = self._sm.create_model_package(
            ModelPackageGroupName="pulsecommerce-churn",
            ModelPackageDescription=(
                f"Churn model | job={config.job_name} | AUC-ROC={auc:.4f}"
            ),
            InferenceSpecification={
                "Containers": [
                    {
                        "Image": XGBOOST_IMAGE_URI,
                        "ModelDataUrl": result.model_artifact_s3,
                    }
                ],
                "SupportedContentTypes": ["text/csv"],
                "SupportedResponseMIMETypes": ["text/csv"],
            },
            ModelApprovalStatus="Approved",
            CustomerMetadataProperties={
                "auc_roc": str(auc),
                "train_job": config.job_name,
                "feature_group": FEATURE_GROUP_NAME,
            },
        )
        arn = resp["ModelPackageArn"]
        logger.info("Registered model package: %s", arn)
        return arn


class ChurnEndpointDeployer:
    """
    Creates or updates a SageMaker real-time endpoint for churn inference.
    Blue/green: creates a new endpoint config then calls UpdateEndpoint on the existing one.
    """

    def __init__(self, sm_client: Any | None = None) -> None:
        self._sm = sm_client or boto3.client("sagemaker", region_name=AWS_REGION)

    def deploy(
        self,
        model_package_arn: str,
        endpoint_name: str,
        instance_type: str = "ml.c5.xlarge",
        instance_count: int = 2,
    ) -> str:
        """Returns the endpoint ARN. Creates if not exists; updates otherwise."""
        ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        model_name = f"pulsecommerce-churn-{ts}"
        config_name = f"pulsecommerce-churn-config-{ts}"

        self._sm.create_model(
            ModelName=model_name,
            ExecutionRoleArn=SAGEMAKER_ROLE_ARN,
            Containers=[{"ModelPackageName": model_package_arn}],
        )

        self._sm.create_endpoint_config(
            EndpointConfigName=config_name,
            ProductionVariants=[
                {
                    "VariantName": "primary",
                    "ModelName": model_name,
                    "InitialInstanceCount": instance_count,
                    "InstanceType": instance_type,
                    "InitialVariantWeight": 1.0,
                }
            ],
        )

        try:
            self._sm.describe_endpoint(EndpointName=endpoint_name)
            self._sm.update_endpoint(
                EndpointName=endpoint_name,
                EndpointConfigName=config_name,
            )
            logger.info("Updated endpoint %s → %s", endpoint_name, config_name)
        except self._sm.exceptions.ClientError:
            self._sm.create_endpoint(
                EndpointName=endpoint_name,
                EndpointConfigName=config_name,
            )
            logger.info("Created endpoint %s", endpoint_name)

        waiter = self._sm.get_waiter("endpoint_in_service")
        waiter.wait(
            EndpointName=endpoint_name,
            WaiterConfig={"Delay": 30, "MaxAttempts": 40},
        )
        desc = self._sm.describe_endpoint(EndpointName=endpoint_name)
        return desc["EndpointArn"]


def _cli() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Launch PulseCommerce churn model training")
    parser.add_argument("--train-s3", required=True, help="S3 URI for training data (CSV)")
    parser.add_argument("--val-s3", required=True, help="S3 URI for validation data (CSV)")
    parser.add_argument("--output-s3", required=True, help="S3 prefix for model output")
    parser.add_argument("--instance-type", default="ml.m5.2xlarge")
    parser.add_argument("--no-register", action="store_true", help="Skip Model Registry registration")
    parser.add_argument("--deploy-endpoint", help="Deploy to this endpoint name after training")
    args = parser.parse_args()

    config = TrainingConfig(
        train_s3_uri=args.train_s3,
        validation_s3_uri=args.val_s3,
        output_s3_prefix=args.output_s3,
        instance_type=args.instance_type,
    )

    trainer = ChurnModelTrainer()
    result = trainer.run(config, register=not args.no_register)
    print(json.dumps(
        {
            "job_name": result.job_name,
            "status": result.status,
            "metrics": result.metrics,
            "model_artifact": result.model_artifact_s3,
            "model_package_arn": result.model_package_arn,
        },
        indent=2,
    ))

    if args.deploy_endpoint and result.model_package_arn:
        deployer = ChurnEndpointDeployer()
        arn = deployer.deploy(result.model_package_arn, args.deploy_endpoint)
        print(f"Deployed to endpoint: {arn}")


if __name__ == "__main__":
    _cli()
