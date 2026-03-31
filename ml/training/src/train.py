#!/usr/bin/env python3
# =============================================================================
# ml/training/src/train.py
# =============================================================================
# SageMaker training script — executed inside the XGBoost managed container.
# Receives hyperparameters and data channel paths via SageMaker conventions:
#   - Hyperparameters: /opt/ml/input/config/hyperparameters.json
#   - Training data:   /opt/ml/input/data/train/  (CSV, no header)
#   - Validation data: /opt/ml/input/data/validation/  (CSV, no header)
#   - Model output:    /opt/ml/model/
#   - Metrics:         stdout (scraped by SageMaker regex metric definitions)
#
# Feature order (must match user_behavioral_features.py FEATURE_SQL output):
#   0  days_since_last_order
#   1  days_since_last_session
#   2  session_count_7d
#   3  session_count_30d
#   4  order_count_30d
#   5  order_count_90d
#   6  order_frequency_30d
#   7  avg_order_value_usd
#   8  total_ltv_usd
#   9  max_order_value_usd
#   10 discount_usage_rate
#   11 cart_abandonment_rate
#   12 avg_session_duration_s
#   13 avg_pages_per_session
#   14 product_view_count_7d
#   15 preferred_category_encoded
#   16 channel_group_encoded
#   17 avg_fraud_score
#   18 refund_count_90d
#   19 is_gdpr_scope
#   20 label  (churned_30d — last column)
# =============================================================================

from __future__ import annotations

import glob
import json
import logging
import os
import pickle
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.metrics import (
    average_precision_score,
    classification_report,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SageMaker path conventions
# ---------------------------------------------------------------------------

SM_INPUT_CONFIG = "/opt/ml/input/config"
SM_INPUT_DATA = "/opt/ml/input/data"
SM_MODEL_DIR = "/opt/ml/model"
SM_OUTPUT_DIR = "/opt/ml/output"

FEATURE_NAMES = [
    "days_since_last_order",
    "days_since_last_session",
    "session_count_7d",
    "session_count_30d",
    "order_count_30d",
    "order_count_90d",
    "order_frequency_30d",
    "avg_order_value_usd",
    "total_ltv_usd",
    "max_order_value_usd",
    "discount_usage_rate",
    "cart_abandonment_rate",
    "avg_session_duration_s",
    "avg_pages_per_session",
    "product_view_count_7d",
    "preferred_category_encoded",
    "channel_group_encoded",
    "avg_fraud_score",
    "refund_count_90d",
    "is_gdpr_scope",
]
LABEL_COL = "label"


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_csv_channel(channel: str) -> pd.DataFrame:
    """
    Load all CSV files from a SageMaker data channel directory.
    CSV has no header — last column is the label.
    """
    channel_dir = os.path.join(SM_INPUT_DATA, channel)
    csv_files = glob.glob(os.path.join(channel_dir, "*.csv"))

    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in channel: {channel_dir}")

    dfs = []
    for f in csv_files:
        df = pd.read_csv(f, header=None)
        dfs.append(df)

    combined = pd.concat(dfs, ignore_index=True)

    # Assign column names: features + label
    all_cols = FEATURE_NAMES + [LABEL_COL]
    if len(combined.columns) != len(all_cols):
        raise ValueError(
            f"Expected {len(all_cols)} columns, got {len(combined.columns)} in {channel}"
        )
    combined.columns = all_cols
    return combined


def preprocess(df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    """
    Basic preprocessing:
    - Clip extreme values (days_since_last_order > 999 → 999)
    - Fill remaining NaN with 0
    - Split features / label
    """
    df = df.copy()

    # Clip recency features — 999 = "no activity" sentinel
    for col in ("days_since_last_order", "days_since_last_session"):
        if col in df.columns:
            df[col] = df[col].clip(upper=999)

    # Clip rates to [0, 1]
    for col in ("discount_usage_rate", "cart_abandonment_rate", "avg_fraud_score"):
        if col in df.columns:
            df[col] = df[col].clip(0.0, 1.0)

    df = df.fillna(0)

    X = df[FEATURE_NAMES].values.astype(np.float32)
    y = df[LABEL_COL].values.astype(np.float32)
    return X, y


# ---------------------------------------------------------------------------
# Training
# ---------------------------------------------------------------------------

def load_hyperparameters() -> dict[str, Any]:
    """Load SageMaker hyperparameters from the config JSON file."""
    hp_path = os.path.join(SM_INPUT_CONFIG, "hyperparameters.json")
    if not os.path.exists(hp_path):
        logger.warning("hyperparameters.json not found — using defaults")
        return {}

    with open(hp_path) as f:
        raw: dict[str, str] = json.load(f)

    # SageMaker passes all values as strings; cast numeric ones
    casted: dict[str, Any] = {}
    int_keys = {"max_depth", "num_round", "seed", "min_child_weight"}
    float_keys = {"eta", "gamma", "subsample", "colsample_bytree", "scale_pos_weight"}

    for k, v in raw.items():
        if k in int_keys:
            casted[k] = int(v)
        elif k in float_keys:
            casted[k] = float(v)
        else:
            casted[k] = v

    return casted


def train(
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_val: np.ndarray,
    y_val: np.ndarray,
    params: dict[str, Any],
    num_round: int = 200,
) -> xgb.Booster:
    """Train XGBoost booster with early stopping on validation AUC."""
    dtrain = xgb.DMatrix(X_train, label=y_train, feature_names=FEATURE_NAMES)
    dval = xgb.DMatrix(X_val, label=y_val, feature_names=FEATURE_NAMES)

    # Remove num_round from params (it's a train() argument, not an XGBoost param)
    xgb_params = {k: v for k, v in params.items() if k != "num_round"}

    # Ensure binary classification defaults
    xgb_params.setdefault("objective", "binary:logistic")
    xgb_params.setdefault("eval_metric", "auc")
    xgb_params.setdefault("seed", 42)

    callbacks = [
        xgb.callback.EarlyStopping(rounds=20, metric_name="auc", maximize=True),
    ]

    booster = xgb.train(
        params=xgb_params,
        dtrain=dtrain,
        num_boost_round=num_round,
        evals=[(dtrain, "train"), (dval, "validation")],
        callbacks=callbacks,
        verbose_eval=10,
    )
    return booster


# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------

def evaluate(booster: xgb.Booster, X: np.ndarray, y: np.ndarray, split: str = "validation") -> dict[str, float]:
    """
    Evaluate the booster on a dataset. Prints metrics to stdout in the format
    SageMaker's MetricDefinitions regex can scrape.
    """
    dmatrix = xgb.DMatrix(X, feature_names=FEATURE_NAMES)
    proba = booster.predict(dmatrix)
    preds = (proba >= 0.5).astype(int)

    auc = roc_auc_score(y, proba)
    ap = average_precision_score(y, proba)
    precision = precision_score(y, preds, zero_division=0)
    recall = recall_score(y, preds, zero_division=0)
    error = 1.0 - (preds == y).mean()

    metrics = {
        f"{split}-auc": auc,
        f"{split}-ap": ap,
        f"{split}-precision": precision,
        f"{split}-recall": recall,
        f"{split}-error": error,
    }

    # Print in SageMaker-scrapable format
    for name, value in metrics.items():
        # Format: "[<split>] <metric-name>:<value>"
        clean_name = name.replace(f"{split}-", "")
        print(f"[{split}] {split}-{clean_name}:{value:.6f}")

    logger.info("%s metrics: %s", split, {k: f"{v:.4f}" for k, v in metrics.items()})
    logger.info("\n%s", classification_report(y, preds, target_names=["not_churn", "churn"]))

    return metrics


def log_feature_importance(booster: xgb.Booster) -> None:
    """Log top-10 feature importances by gain."""
    importance = booster.get_score(importance_type="gain")
    sorted_imp = sorted(importance.items(), key=lambda x: x[1], reverse=True)
    logger.info("Top-10 feature importances (gain):")
    for feat, score in sorted_imp[:10]:
        logger.info("  %-35s %.4f", feat, score)


# ---------------------------------------------------------------------------
# Model serialisation
# ---------------------------------------------------------------------------

def save_model(booster: xgb.Booster, metrics: dict[str, float]) -> None:
    """
    Save XGBoost model to SageMaker model directory.
    SageMaker XGBoost container expects model.json (native XGBoost format).
    Also saves metrics.json for Model Registry metadata.
    """
    os.makedirs(SM_MODEL_DIR, exist_ok=True)
    os.makedirs(SM_OUTPUT_DIR, exist_ok=True)

    model_path = os.path.join(SM_MODEL_DIR, "model.json")
    booster.save_model(model_path)
    logger.info("Model saved: %s", model_path)

    metrics_path = os.path.join(SM_OUTPUT_DIR, "metrics.json")
    with open(metrics_path, "w") as f:
        json.dump(metrics, f, indent=2)
    logger.info("Metrics saved: %s", metrics_path)

    # Feature name mapping (for inference validation)
    feature_path = os.path.join(SM_MODEL_DIR, "feature_names.json")
    with open(feature_path, "w") as f:
        json.dump(FEATURE_NAMES, f)


# ---------------------------------------------------------------------------
# Main entrypoint
# ---------------------------------------------------------------------------

def main() -> None:
    logger.info("=== PulseCommerce Churn Model Training ===")

    # Load hyperparameters
    hp = load_hyperparameters()
    num_round = int(hp.pop("num_round", 200))
    logger.info("Hyperparameters: %s | num_round=%d", hp, num_round)

    # Load data
    logger.info("Loading training data...")
    df_train = load_csv_channel("train")
    logger.info("Loading validation data...")
    df_val = load_csv_channel("validation")

    logger.info("Train rows: %d | Val rows: %d", len(df_train), len(df_val))

    # Class balance info
    churn_rate = df_train[LABEL_COL].mean()
    logger.info("Training churn rate: %.2f%%", churn_rate * 100)

    # Preprocess
    X_train, y_train = preprocess(df_train)
    X_val, y_val = preprocess(df_val)

    # Train
    logger.info("Starting XGBoost training...")
    booster = train(X_train, y_train, X_val, y_val, params=hp, num_round=num_round)

    # Evaluate
    train_metrics = evaluate(booster, X_train, y_train, split="train")
    val_metrics = evaluate(booster, X_val, y_val, split="validation")

    # Feature importance
    log_feature_importance(booster)

    # Quality gate check (informational — actual gate enforced in Airflow)
    auc = val_metrics.get("validation-auc", 0.0)
    min_auc = float(os.environ.get("CHURN_MIN_AUC_ROC", "0.80"))
    if auc < min_auc:
        logger.warning(
            "Quality gate WARNING: validation AUC=%.4f < threshold=%.2f", auc, min_auc
        )
    else:
        logger.info("Quality gate PASSED: validation AUC=%.4f >= %.2f", auc, min_auc)

    # Save
    all_metrics = {**train_metrics, **val_metrics}
    save_model(booster, all_metrics)

    logger.info("=== Training complete ===")


if __name__ == "__main__":
    main()
