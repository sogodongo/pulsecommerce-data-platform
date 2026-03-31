from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime, timezone

from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

CATALOG_NAME = "glue"
TABLE_FQNAME = "bronze.clickstream"
WAREHOUSE = os.environ.get("LAKEHOUSE_BUCKET", "s3://pulsecommerce-lakehouse-123456789012/")


def get_catalog():
    return load_catalog(
        CATALOG_NAME,
        **{
            "type": "glue",
            "warehouse": WAREHOUSE,
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
            "s3.region": os.environ.get("AWS_REGION", "us-east-1"),
        },
    )


def get_table():
    catalog = get_catalog()
    return catalog.load_table(TABLE_FQNAME)


def show_current_schema(dry_run: bool = True) -> None:
    table = get_table()
    schema = table.schema()

    print(f"\n{'='*60}")
    print(f"Table: {TABLE_FQNAME}")
    print(f"Schema ID: {schema.schema_id}")
    print(f"Spec ID:   {table.spec().spec_id}")
    print(f"Snapshots: {len(table.snapshots())}")
    if table.current_snapshot():
        snap = table.current_snapshot()
        print(f"Current snapshot: {snap.snapshot_id} @ {snap.timestamp_ms}")
    print(f"\nColumns ({len(schema.fields)}):")
    for field in schema.fields:
        required = "NOT NULL" if field.required else "nullable"
        print(f"  [{field.field_id:3d}] {field.name:<30s} {str(field.field_type):<20s} {required}")
        if field.doc:
            print(f"       -- {field.doc}")
    print(f"{'='*60}\n")


def show_snapshot_history() -> None:
    table = get_table()
    snapshots = table.snapshots()

    print(f"\nSnapshot history for {TABLE_FQNAME} ({len(snapshots)} snapshots):")
    print(f"{'Snapshot ID':<20} {'Timestamp':<30} {'Operation':<15} {'Summary'}")
    print("-" * 90)
    for snap in sorted(snapshots, key=lambda s: s.timestamp_ms, reverse=True)[:20]:
        ts = datetime.fromtimestamp(snap.timestamp_ms / 1000, tz=timezone.utc).isoformat()
        op = snap.summary.get("operation", "unknown") if snap.summary else "unknown"
        added = snap.summary.get("added-records", "?") if snap.summary else "?"
        deleted = snap.summary.get("deleted-records", "?") if snap.summary else "?"
        print(f"{snap.snapshot_id:<20} {ts:<30} {op:<15} +{added}/-{deleted} rows")


def add_user_agent_column(dry_run: bool = True) -> None:
    # Safe BACKWARD-compatible add — nullable, no default needed
    table = get_table()
    existing_names = {f.name for f in table.schema().fields}

    if "user_agent" in existing_names:
        logger.info("Column 'user_agent' already exists — no action needed.")
        return

    if dry_run:
        logger.info("[DRY RUN] Would add column: user_agent STRING nullable")
        logger.info("[DRY RUN] This is a metadata-only operation (no data rewrite)")
        return

    with table.update_schema() as update:
        update.add_column(
            path="user_agent",
            field_type=StringType(),
            doc="Raw User-Agent string. Added in clickstream schema v1.1.",
        )

    logger.info("Added column 'user_agent' to %s (metadata-only, schema ID incremented)", TABLE_FQNAME)


def add_churn_score_column(dry_run: bool = True) -> None:
    # Added when SageMaker churn endpoint goes live — null until then
    table = get_table()
    existing_names = {f.name for f in table.schema().fields}

    if "churn_score" in existing_names:
        logger.info("Column 'churn_score' already exists.")
        return

    if dry_run:
        logger.info("[DRY RUN] Would add column: churn_score DOUBLE nullable")
        return

    with table.update_schema() as update:
        update.add_column(
            path="churn_score",
            field_type=DoubleType(),
            doc="Real-time churn probability [0.0–1.0] from SageMaker churn endpoint. Null before endpoint launch.",
        )

    logger.info("Added column 'churn_score' to %s", TABLE_FQNAME)


def rename_is_bot_to_bot_detected(dry_run: bool = True) -> None:
    # Iceberg tracks columns by ID not name — old Flink jobs writing is_bot keep working
    # in parallel while new jobs are deployed to write bot_detected.
    table = get_table()

    # is_bot is top-level in this flattened Bronze schema (not nested in a struct)
    existing_names = {f.name for f in table.schema().fields}

    if "bot_detected" in existing_names:
        logger.info("Column already renamed to 'bot_detected'.")
        return

    if "is_bot" not in existing_names:
        logger.warning("Column 'is_bot' not found — cannot rename.")
        return

    if dry_run:
        logger.info("[DRY RUN] Would rename: is_bot → bot_detected")
        logger.info("[DRY RUN] Iceberg tracks by column ID — no data rewrite required")
        return

    with table.update_schema() as update:
        update.rename_column("is_bot", "bot_detected")

    logger.info("Renamed column 'is_bot' → 'bot_detected' in %s", TABLE_FQNAME)


def add_product_quantity_column(dry_run: bool = True) -> None:
    # Missing from original v1 schema — populated by Flink from add_to_cart / purchase events
    table = get_table()
    existing_names = {f.name for f in table.schema().fields}

    if "product_quantity" in existing_names:
        logger.info("Column 'product_quantity' already exists.")
        return

    if dry_run:
        logger.info("[DRY RUN] Would add column: product_quantity INT nullable")
        return

    with table.update_schema() as update:
        update.add_column(
            path="product_quantity",
            field_type=IntegerType(),
            doc="Quantity for add_to_cart / purchase events. Null for other event types.",
        )

    logger.info("Added column 'product_quantity' to %s", TABLE_FQNAME)


def expire_old_snapshots(dry_run: bool = True, older_than_days: int = 7) -> None:
    # S3 Tables handles this automatically — only needed for non-S3-Tables buckets
    from datetime import timedelta

    table = get_table()
    cutoff_ms = int((datetime.now(timezone.utc) - timedelta(days=older_than_days)).timestamp() * 1000)
    snapshots_to_expire = [s for s in table.snapshots() if s.timestamp_ms < cutoff_ms]

    logger.info(
        "Found %d snapshots older than %d days (cutoff: %s)",
        len(snapshots_to_expire),
        older_than_days,
        datetime.fromtimestamp(cutoff_ms / 1000, tz=timezone.utc).isoformat(),
    )

    if dry_run:
        logger.info("[DRY RUN] Would expire %d snapshots", len(snapshots_to_expire))
        return

    table.expire_snapshots().expire_older_than(cutoff_ms).commit()
    logger.info("Expired %d snapshots from %s", len(snapshots_to_expire), TABLE_FQNAME)


def create_branch_for_backfill(branch_name: str, dry_run: bool = True) -> None:
    """
    Create an Iceberg branch for zero-downtime dbt model backfills.
    Pattern: dbt writes to branch → validate → fast-forward merge to main.
    Requires Iceberg format-version=2.
    """
    if dry_run:
        logger.info("[DRY RUN] Would create branch '%s' from current snapshot", branch_name)
        return

    table = get_table()
    current_snapshot_id = table.current_snapshot().snapshot_id

    with table.manage_snapshots() as ms:
        ms.create_branch(branch_name, snapshot_id=current_snapshot_id)

    logger.info(
        "Created branch '%s' from snapshot %d on table %s",
        branch_name, current_snapshot_id, TABLE_FQNAME,
    )


ACTIONS = {
    "show_schema": lambda args: show_current_schema(dry_run=args.dry_run),
    "show_history": lambda args: show_snapshot_history(),
    "add_user_agent": lambda args: add_user_agent_column(dry_run=args.dry_run),
    "add_churn_score": lambda args: add_churn_score_column(dry_run=args.dry_run),
    "rename_is_bot": lambda args: rename_is_bot_to_bot_detected(dry_run=args.dry_run),
    "add_product_quantity": lambda args: add_product_quantity_column(dry_run=args.dry_run),
    "expire_snapshots": lambda args: expire_old_snapshots(dry_run=args.dry_run, older_than_days=args.days),
    "create_branch": lambda args: create_branch_for_backfill(args.branch_name, dry_run=args.dry_run),
}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Iceberg schema evolution manager for bronze.clickstream",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"Available actions:\n  " + "\n  ".join(ACTIONS.keys()),
    )
    parser.add_argument("--action", required=True, choices=list(ACTIONS.keys()), help="Evolution action to run")
    parser.add_argument("--dry-run", action="store_true", default=True, help="Preview changes without applying (default: True)")
    parser.add_argument("--apply", dest="dry_run", action="store_false", help="Apply changes (disables dry-run)")
    parser.add_argument("--days", type=int, default=7, help="For expire_snapshots: age threshold in days")
    parser.add_argument("--branch-name", default="backfill", help="For create_branch: branch name")

    args = parser.parse_args()

    if args.dry_run:
        logger.info("Running in DRY RUN mode — no changes will be applied. Use --apply to commit.")
    else:
        logger.warning("Running in APPLY mode — changes will be committed to Iceberg metadata.")

    ACTIONS[args.action](args)


if __name__ == "__main__":
    main()
