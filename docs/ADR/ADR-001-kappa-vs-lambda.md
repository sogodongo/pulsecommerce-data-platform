# ADR-001: Kappa Architecture over Lambda Architecture

| Field       | Value                       |
|-------------|-----------------------------|
| **Status**  | Accepted                    |
| **Date**    | 2024-05-10                  |
| **Deciders**| Platform Engineering, Data  |
| **Tags**    | architecture, streaming     |

---

## Context

PulseCommerce requires sub-second fraud detection and real-time session stitching alongside
daily Gold-layer aggregates consumed by the BI team. Two well-known patterns were evaluated:

- **Lambda Architecture** — maintains a separate batch layer (recalculates truth) and a speed
  layer (approximation). Results are merged at query time via a serving layer.
- **Kappa Architecture** — a single streaming pipeline is the system of record. Batch
  reprocessing is achieved by replaying the immutable log from the beginning.

### Forces

| Force | Lambda | Kappa |
|-------|--------|-------|
| Operational complexity | Two codebases to maintain | Single codebase |
| Correctness | Batch layer corrects speed layer | Stream is always correct; replay corrects |
| Latency | Minutes (batch) + seconds (speed) | End-to-end seconds |
| Reprocessing | Full batch rerun | Replay Kafka topic from offset 0 |
| Cloud cost | Batch cluster + stream cluster | Stream cluster only |

---

## Decision

**We adopt the Kappa Architecture.**

The streaming pipeline (Amazon Managed Flink 1.19) is the single source of truth. Bronze
Iceberg tables are the immutable append-only log. Silver and Gold layers are derived by
reprocessing Bronze via Glue Spark jobs when corrections are needed.

---

## Rationale

1. **Unified codebase.** Maintaining two codebases (batch + streaming) for the same business
   logic creates drift and bugs. The Kappa model eliminates this.

2. **MSK Tiered Storage enables infinite replay.** Amazon MSK Tiered Storage retains topic data
   for 90 days with no broker-disk pressure. Full historical replay costs only egress bandwidth.

3. **Iceberg as the immutable log.** Apache Iceberg v2 on S3 provides snapshot isolation,
   time-travel, and `MERGE` semantics. Bronze is append-only; Silver is upserted via
   `MERGE INTO`. This is sufficient for the batch-correction use case without a separate
   batch layer.

4. **Flink's exactly-once guarantees.** With RocksDB state backend, incremental checkpoints
   to S3, and Kafka source with `EXACTLY_ONCE` semantics, the stream can safely replace a
   batch truth layer for fraud scoring and session stitching.

5. **Latency requirements.** The fraud detection SLA is < 500 ms from event ingestion to
   score availability. A Lambda batch layer contributes nothing to this path.

---

## Consequences

### Positive

- Single streaming codebase for all event-driven logic.
- End-to-end fraud scores and session metrics available within seconds.
- Reprocessing via Kafka offset replay and Iceberg time-travel is well understood.

### Negative / Risks

- **Stateful reprocessing complexity.** Replaying a topic into a stateful Flink job requires
  clearing RocksDB state first — documented in the ops runbook.
- **Late data.** Events arriving after the watermark threshold (default: 5 minutes) are
  dropped. A Glue reprocessing job must be triggered manually for large late-data incidents.

### Mitigations

- The `silver_refresh_dag.py` Airflow DAG runs Glue Spark jobs every 30 minutes using
  Iceberg incremental reads (`start-snapshot-id` → `end-snapshot-id`) to catch late-arriving
  Bronze events, providing a lightweight correction layer.
- Watermark configuration is externalised as a Flink application property
  (`allowed_lateness_ms`) to allow tuning without redeployment.

---

## Alternatives Considered

### Lambda Architecture

Rejected. The batch truth layer requires a second Spark cluster, a second set of dbt models,
and a serving-layer merge. Estimated additional engineering cost: ~3 sprints. Operational
overhead persists indefinitely.

### Pure Batch (no streaming)

Rejected. The fraud detection and personalisation use cases require sub-second latency. Pure
batch cannot meet the SLA.

---

## References

- Jay Kreps, "Questioning the Lambda Architecture" (2014) — original Kappa proposal
- Apache Flink: Stateful Stream Processing documentation
- Amazon MSK Tiered Storage documentation
- PulseCommerce Platform RFC-003: Real-Time Fraud Detection Requirements
