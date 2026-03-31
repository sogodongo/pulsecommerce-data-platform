# ADR-002: Apache Iceberg over Delta Lake for the Lakehouse Table Format

| Field       | Value                            |
|-------------|----------------------------------|
| **Status**  | Accepted                         |
| **Date**    | 2024-05-14                       |
| **Deciders**| Platform Engineering, Data Arch  |
| **Tags**    | storage, lakehouse, iceberg      |

---

## Context

PulseCommerce stores all lakehouse data in S3 and queries it from multiple engines:
AWS Glue (Spark), Amazon Athena, Amazon Redshift Spectrum, and dbt-glue. The table format
must provide ACID semantics, schema evolution, time-travel, and efficient incremental reads.

Two open-table formats were evaluated:

- **Apache Iceberg v2** — vendor-neutral, supported natively by AWS Glue Data Catalog, Athena,
  Flink, and Spark.
- **Delta Lake** — originated at Databricks; OSS version has improved but AWS-native support
  lags Iceberg.

---

## Decision

**We adopt Apache Iceberg v2 as the sole table format across Bronze, Silver, and Gold layers.**

---

## Comparison

| Capability | Iceberg v2 | Delta Lake OSS |
|------------|-----------|----------------|
| AWS Glue Data Catalog native support | Yes (GA) | Partial (requires manifest) |
| Athena native read/write | Yes | Read-only (manifest) |
| Redshift Spectrum | Yes | No |
| Flink connector | `iceberg-flink` (Apache) | Unofficial |
| Row-level deletes (GDPR) | Yes — position/equality delete files | Yes (MERGE + vacuum) |
| Partition evolution | Yes — no rewrite needed | No (requires rewrite) |
| Hidden partitioning | Yes | No |
| Schema evolution | Full (add/rename/reorder/widen) | Add/rename only |
| Snapshot isolation | Yes — per-snapshot | Yes — per-version |
| Incremental reads | Yes — snapshot diff API | Yes — Change Data Feed |
| Multi-engine write safety | Yes — optimistic locking | Yes (DeltaLog) |
| AWS S3 Tables | Native | Not supported |

---

## Rationale

1. **AWS Glue Data Catalog native integration.** Glue registers Iceberg tables directly; no
   external Hive Metastore or Databricks workspace is required. Athena and Redshift Spectrum
   read Iceberg tables without manifest generation.

2. **Flink connector maturity.** The `iceberg-flink` connector is a first-party Apache project
   with production use at scale. Flink's `bronze_writer.py` uses it for Bronze table commits.
   Delta Lake has no equivalent official Flink connector.

3. **Partition evolution.** As event volume grows, partition strategies will change (e.g.,
   from daily to hourly partitions). Iceberg supports partition evolution without rewriting
   existing data files. Delta Lake requires a full table rewrite.

4. **GDPR row-level deletes.** Iceberg v2 equality delete files allow targeted deletion of
   individual rows (right-to-erasure requests) without rewriting entire partitions.

5. **Hidden partitioning.** Iceberg's hidden partition transforms (e.g., `HOUR(event_ts)`)
   automatically route writes without requiring consumers to include partition predicates in
   queries. This eliminates the "partition explosion" problem common in Delta layouts.

6. **AWS S3 Tables.** AWS S3 Tables (2024) is built on Iceberg and provides automatic
   compaction, snapshot expiry, and orphan file cleanup. Delta Lake is not supported.

---

## Consequences

### Positive

- Single table format across all engines — no translation layer.
- Iceberg snapshot diff enables zero-scan incremental reads in Glue jobs.
- GDPR delete requests can be processed in < 1 hour without full partition rewrites.
- Time-travel queries (`VERSION AS OF <snapshot_id>`) available in Athena and Spark for audit.

### Negative / Risks

- **Compaction must be managed.** Small-file proliferation from frequent Flink writes requires
  periodic compaction (`CALL glue_catalog.system.rewrite_data_files(...)`). Managed via the
  Airflow `silver_refresh_dag.py` end-of-job step.
- **Iceberg catalog lock.** Concurrent writes from multiple Flink parallelism slots require
  DynamoDB-based lock table (`pulsecommerce-glue-bookmarks`). Mis-configuration could cause
  write conflicts.

### Mitigations

- Lifecycle policies in the `s3-lakehouse` Terraform module expire Bronze data at 90 days,
  preventing unlimited small-file growth.
- Compaction is triggered automatically at the end of each Glue job run.

---

## Alternatives Considered

### Delta Lake

Rejected. Primary blockers: no Flink first-party connector, no Athena native write support,
no Redshift Spectrum support, and no path to AWS S3 Tables.

### Apache Hudi

Considered briefly. Hudi's Copy-on-Write and Merge-on-Read modes are mature, but AWS Glue
native Hudi support is behind Iceberg. The Flink connector requires `HoodieFlinkStreamer`,
adding operational complexity. Rejected in favour of Iceberg's simpler model.

---

## References

- Apache Iceberg specification v2
- AWS Glue Iceberg integration documentation
- Amazon Athena Iceberg support documentation
- PulseCommerce RFC-005: Lakehouse Storage Format Selection
