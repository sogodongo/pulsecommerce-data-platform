"""
processing/schema/bootstrap.py

One-time DDL bootstrap runner. Creates all Glue databases and Iceberg tables
across Bronze, Silver, and Gold zones by executing the DDL SQL files via
PySpark (Glue 5.0 session) or Athena.

Run this once on a fresh environment before deploying any Flink / Glue jobs.
Idempotent: all statements use CREATE TABLE IF NOT EXISTS.

Usage (local with Glue Dev Endpoint / Glue Interactive Session):
  python bootstrap.py --zone all --dry-run
  python bootstrap.py --zone bronze --apply
  python bootstrap.py --zone all --apply

Usage (as a Glue Job, triggered by Terraform after infra provisioning):
  Deployed as a one-off Glue job via Terraform null_resource + local-exec.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)

DDL_ROOT = Path(__file__).parent / "ddl"

DDL_FILES: dict[str, list[Path]] = {
    "bronze": [DDL_ROOT / "bronze" / "bronze_tables.sql"],
    "silver": [DDL_ROOT / "silver" / "silver_tables.sql"],
    "gold":   [DDL_ROOT / "gold"   / "gold_tables.sql"],
}

WAREHOUSE = os.environ.get("LAKEHOUSE_BUCKET", "s3://pulsecommerce-lakehouse-123456789012/")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")


# ─────────────────────────────────────────────────────────────────────────────
# PySpark / Glue session
# ─────────────────────────────────────────────────────────────────────────────

def build_spark_session():
    """
    Build a PySpark session configured for Iceberg + Glue Data Catalog.
    In a Glue Job context, SparkContext is pre-initialised — this still works
    because getOrCreate() returns the existing session.
    """
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder
        .appName("pulsecommerce-schema-bootstrap")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.glue_catalog.warehouse", WAREHOUSE)
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.glue_catalog.glue.skip-archive", "true")
        .config("spark.sql.defaultCatalog", "glue_catalog")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.InstanceProfileCredentialsProvider")
        .getOrCreate()
    )


# ─────────────────────────────────────────────────────────────────────────────
# SQL parsing — split on semicolons, skip comments
# ─────────────────────────────────────────────────────────────────────────────

def parse_statements(sql_text: str) -> list[str]:
    """
    Split a SQL file into individual executable statements.
    Handles:
      - Single-line comments (--)
      - Multi-line block comments (/* ... */)
      - Quoted string literals containing semicolons
    """
    statements: list[str] = []
    current: list[str] = []
    in_block_comment = False
    in_single_quote = False

    i = 0
    lines = sql_text.splitlines(keepends=True)
    text = "".join(lines)

    while i < len(text):
        c = text[i]

        # Toggle block comments
        if not in_single_quote and text[i:i+2] == "/*":
            in_block_comment = True
            i += 2
            continue
        if in_block_comment and text[i:i+2] == "*/":
            in_block_comment = False
            i += 2
            continue
        if in_block_comment:
            i += 1
            continue

        # Skip single-line comments
        if not in_single_quote and text[i:i+2] == "--":
            while i < len(text) and text[i] != "\n":
                i += 1
            continue

        # Track string literals (to avoid splitting on embedded semicolons)
        if c == "'":
            in_single_quote = not in_single_quote

        # Statement boundary
        if c == ";" and not in_single_quote:
            stmt = "".join(current).strip()
            if stmt:
                statements.append(stmt)
            current = []
            i += 1
            continue

        current.append(c)
        i += 1

    # Catch unterminated last statement (no trailing semicolon)
    remainder = "".join(current).strip()
    if remainder:
        statements.append(remainder)

    return [s for s in statements if s and not s.isspace()]


# ─────────────────────────────────────────────────────────────────────────────
# Bootstrap execution
# ─────────────────────────────────────────────────────────────────────────────

def run_ddl_file(spark, ddl_file: Path, dry_run: bool) -> tuple[int, int]:
    """
    Execute all statements in a DDL file.
    Returns (succeeded, failed) counts.
    """
    logger.info("Processing DDL file: %s", ddl_file)
    sql_text = ddl_file.read_text(encoding="utf-8")
    statements = parse_statements(sql_text)

    logger.info("  Found %d statements", len(statements))
    succeeded = 0
    failed = 0

    for i, stmt in enumerate(statements, start=1):
        first_line = stmt.splitlines()[0][:80]
        logger.info("  [%d/%d] %s ...", i, len(statements), first_line)

        if dry_run:
            logger.info("    [DRY RUN] Skipping execution")
            succeeded += 1
            continue

        try:
            spark.sql(stmt)
            succeeded += 1
            logger.info("    OK")
        except Exception as exc:
            failed += 1
            logger.error("    FAILED: %s", exc)
            # Don't abort — continue with remaining statements (IF NOT EXISTS makes most safe)

    return succeeded, failed


def bootstrap(zones: list[str], dry_run: bool) -> None:
    """Main bootstrap routine."""
    if dry_run:
        logger.info("=" * 60)
        logger.info("DRY RUN — no DDL will be executed")
        logger.info("=" * 60)
    else:
        logger.info("=" * 60)
        logger.info("APPLY MODE — executing DDL against Glue / Iceberg")
        logger.info("Warehouse: %s", WAREHOUSE)
        logger.info("=" * 60)

    spark = None
    if not dry_run:
        spark = build_spark_session()
        logger.info("Spark session initialised: %s", spark.version)

    total_ok = 0
    total_fail = 0

    for zone in zones:
        ddl_files = DDL_FILES.get(zone, [])
        if not ddl_files:
            logger.warning("No DDL files configured for zone '%s'", zone)
            continue

        logger.info("\n--- Zone: %s ---", zone.upper())
        for ddl_file in ddl_files:
            if not ddl_file.exists():
                logger.error("DDL file not found: %s", ddl_file)
                total_fail += 1
                continue

            ok, fail = run_ddl_file(spark, ddl_file, dry_run)
            total_ok += ok
            total_fail += fail

    logger.info("\n" + "=" * 60)
    logger.info("Bootstrap complete: %d succeeded, %d failed", total_ok, total_fail)

    if total_fail > 0:
        logger.error("%d statements failed — review logs above", total_fail)
        sys.exit(1)


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="PulseCommerce schema bootstrap — creates all Iceberg tables via DDL SQL files",
    )
    parser.add_argument(
        "--zone",
        choices=["bronze", "silver", "gold", "all"],
        default="all",
        help="Which zone(s) to bootstrap (default: all)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Parse and log DDL without executing (default: True)",
    )
    parser.add_argument(
        "--apply",
        dest="dry_run",
        action="store_false",
        help="Execute DDL (disables dry-run)",
    )

    args = parser.parse_args()
    zones = ["bronze", "silver", "gold"] if args.zone == "all" else [args.zone]
    bootstrap(zones, dry_run=args.dry_run)


# ─────────────────────────────────────────────────────────────────────────────
# Glue Job entrypoint (when run as a Glue job, not CLI)
# ─────────────────────────────────────────────────────────────────────────────

def glue_handler():
    """
    Entrypoint when deployed as an AWS Glue job.
    Glue passes job parameters via --ZONE and --DRY_RUN job arguments.
    """
    from awsglue.utils import getResolvedOptions

    args = getResolvedOptions(sys.argv, ["ZONE", "DRY_RUN"])
    zone = args.get("ZONE", "all")
    dry_run = args.get("DRY_RUN", "true").lower() == "true"
    zones = ["bronze", "silver", "gold"] if zone == "all" else [zone]
    bootstrap(zones, dry_run=dry_run)


if __name__ == "__main__":
    # Detect if running as Glue job (sys.argv contains --JOB_NAME)
    if "--JOB_NAME" in sys.argv:
        glue_handler()
    else:
        main()
