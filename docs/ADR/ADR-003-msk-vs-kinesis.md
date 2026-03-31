# ADR-003: Amazon MSK over Amazon Kinesis Data Streams

| Field       | Value                            |
|-------------|----------------------------------|
| **Status**  | Accepted                         |
| **Date**    | 2024-05-20                       |
| **Deciders**| Platform Engineering             |
| **Tags**    | messaging, kafka, kinesis        |

---

## Context

PulseCommerce requires a managed event streaming backbone that can handle peak clickstream
ingest of ~50,000 events/second while integrating with Confluent Schema Registry, supporting
Flink exactly-once semantics, and retaining events for 90 days for replay.

Two AWS-managed options were evaluated:

- **Amazon MSK (Managed Streaming for Apache Kafka)** — fully managed Kafka, compatible with
  the Kafka protocol ecosystem.
- **Amazon Kinesis Data Streams** — AWS-proprietary streaming service with a different
  producer/consumer API.

---

## Decision

**We adopt Amazon MSK 3.6 with MSK Tiered Storage.**

---

## Comparison

| Capability | Amazon MSK 3.6 | Kinesis Data Streams |
|------------|---------------|----------------------|
| Protocol compatibility | Kafka wire protocol | Proprietary KCL/SDK |
| Schema Registry | Confluent / AWS Glue SR | AWS Glue SR only |
| Flink connector | `kafka-connector` (official) | `kinesis-connector` (official) |
| Exactly-once semantics | Yes (Kafka transactions) | Effectively-once via KCL |
| Retention | MSK Tiered Storage: unlimited | Max 365 days |
| Consumer groups | Kafka consumer groups (native) | KCL application state in DynamoDB |
| Max throughput per partition | Configurable per topic | 1 MB/s write, 2 MB/s read per shard |
| MSK Tiered Storage | Yes | N/A |
| Community / OSS ecosystem | Vast (Kafka ecosystem) | Limited to AWS |

---

## Rationale

1. **Confluent Schema Registry compatibility.** The platform uses Avro schemas with full-
   compatibility enforcement and schema evolution. Confluent Schema Registry speaks the
   Confluent wire format. Kinesis has no native Confluent SR integration.

2. **Flink exactly-once with Kafka transactions.** Flink's Kafka source/sink supports
   two-phase commit (2PC) with Kafka transactions for end-to-end exactly-once delivery.
   The Kinesis connector achieves effectively-once via idempotent writes but does not
   support 2PC — unacceptable for fraud scoring where duplicate processing inflates scores.

3. **MSK Tiered Storage enables 90-day replay.** MSK Tiered Storage offloads log segments
   to S3, enabling 90-day retention at ~10% of the cost of broker-local storage.

4. **Consumer group flexibility.** Multiple independent consumer groups (Flink bronze_writer,
   fraud_scorer, session_stitcher, ad attribution batch) read the same topic independently
   with their own offsets. Kinesis shard iterators require explicit position tracking in
   application state, adding complexity for multiple consumers.

5. **Partition throughput.** At 50,000 events/second with ~1 KB average payload (50 MB/s),
   Kinesis would require at least 50 shards (1 MB/s/shard write limit). MSK partitions have
   no hard per-partition throughput limit.

6. **Ecosystem tooling.** The Kafka ecosystem (kcat, Confluent CLI, kafka-python, Schema
   Registry REST API) is battle-tested. The team's existing Kafka expertise transfers directly.

---

## Consequences

### Positive

- Full Kafka wire protocol compatibility — any Kafka client works unchanged.
- Schema evolution with full backward/forward compatibility enforced at produce time.
- Exactly-once delivery from Flink to Bronze Iceberg.
- 90-day replay at low cost via Tiered Storage.

### Negative / Risks

- **Broker management.** MSK requires cluster sizing decisions (broker count, instance type,
  storage per broker). Under-provisioning leads to throttling.
- **VPC networking.** MSK brokers live in private subnets; Flink applications require VPC
  config and private connectivity.

### Mitigations

- CloudWatch alarms: consumer lag > 100,000 (triggers SNS alert) and disk usage > 80%.
- MSK broker instance type: `kafka.m5.xlarge` (4 vCPU, 16 GB).
- MSK cluster is deployed in private subnets with SG rules restricted to Flink SG only.

---

## Alternatives Considered

### Amazon Kinesis Data Streams

Rejected. Primary blockers: no Confluent Schema Registry support, no Kafka 2PC exactly-once
for Flink, and lack of consumer group semantics.

### Self-managed Kafka on EC2

Rejected. Operational burden (ZooKeeper/KRaft management, rolling upgrades, broker failure
recovery) is not acceptable for a team of this size.

### Amazon EventBridge

Rejected. EventBridge is designed for low-volume event routing and does not support consumer
group replay, exactly-once semantics, or Flink connectors.

---

## References

- Amazon MSK documentation: Tiered Storage
- Apache Flink: Kafka connector exactly-once semantics
- Confluent Schema Registry wire format specification
- PulseCommerce RFC-002: Event Streaming Platform Selection
