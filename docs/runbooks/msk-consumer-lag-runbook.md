# Runbook: MSK Consumer Lag — Detection and Remediation

| Field        | Value                                          |
|--------------|------------------------------------------------|
| **Service**  | Amazon MSK (Kafka)                             |
| **Alarm**    | `MSKConsumerLag` — threshold > 100,000 msgs   |
| **Severity** | HIGH                                           |
| **Team**     | Platform Engineering / On-Call                 |
| **Updated**  | 2024-06-01                                     |

---

## 1. What This Alarm Means

The `SumOffsetLag` CloudWatch metric across all Kafka consumer groups on the MSK cluster
has exceeded 100,000 messages. This indicates that one or more Flink applications are
consuming events slower than they are being produced.

**Consequences if not remediated within 30 minutes:**
- Fraud score latency degrades — events are processed stale
- Session stitcher windows drift — session boundaries become inaccurate
- MSK Tiered Storage throttle risk if consumers fall behind by > 4 hours
- Bronze Iceberg write backlog — snapshot IDs grow without Glue jobs catching up

---

## 2. Initial Triage (< 5 minutes)

### 2.1 Identify which consumer group is lagging

```bash
# List all consumer groups and their lag
kafka-consumer-groups.sh \
  --bootstrap-server <MSK_BROKER_ENDPOINT>:9098 \
  --command-config /etc/kafka/client.properties \
  --describe --all-groups \
  | sort -k6 -rn \
  | head -20
```

**Key columns:**
- `GROUP` — consumer group ID (e.g., `flink-bronze-writer`, `flink-fraud-scorer`)
- `LAG` — number of unconsumed messages per partition
- `CONSUMER-ID` — active consumer instance (empty = no active consumer)

### 2.2 Check if Flink applications are running

Navigate to the AWS Console → Managed Flink → Applications:

| Application | Expected Status |
|-------------|----------------|
| `pulsecommerce-bronze-writer` | RUNNING |
| `pulsecommerce-session-stitcher` | RUNNING |
| `pulsecommerce-fraud-scorer` | RUNNING |

If any application shows `STOPPED`, `FAILED`, or `RESTARTING`, follow
**Section 4: Flink Application Down**.

### 2.3 Check producer throughput

In CloudWatch, check the MSK metric `BytesInPerSec` for each broker. If there is a sudden
spike, a traffic surge (e.g., flash sale) may be causing temporary lag.

---

## 3. Common Causes and Remediation

### 3.1 Flink parallelism too low for current throughput

**Symptoms:** Lag increases steadily; Flink application is RUNNING but CPU usage is high.

**Check parallelism:**
```bash
# In Flink Web UI (via Session Manager tunnel to Flink TaskManager)
# Or via CloudWatch Logs
aws logs filter-log-events \
  --log-group-name /aws/flink/pulsecommerce/bronze-writer \
  --filter-pattern "parallelism" \
  --start-time $(date -d '1 hour ago' +%s)000
```

**Remediation:**
1. In the AWS Console → Managed Flink → `pulsecommerce-bronze-writer` → Configure
2. Increase `ParallelismPerKPU` from current value (default: 1) to 2
3. Trigger a snapshot before modifying: **Actions → Create snapshot**
4. Apply the configuration change (causes brief restart)
5. Monitor lag for 10 minutes — should decrease at rate proportional to new throughput

### 3.2 MSK broker disk usage > 80%

**Symptoms:** `KafkaBrokerDiskUsed` alarm is also firing; consumer lag is secondary.

**Check disk usage:**
```bash
aws cloudwatch get-metric-statistics \
  --namespace AWS/Kafka \
  --metric-name KafkaBrokerDiskUsed \
  --dimensions Name=Cluster\ Name,Value=pulsecommerce-msk \
  --start-time $(date -d '1 hour ago' -u +%Y-%m-%dT%H:%M:%SZ) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%SZ) \
  --period 300 \
  --statistics Maximum
```

**Remediation:**
1. Verify MSK Tiered Storage is active for all topics:
   ```
   kafka-configs.sh --bootstrap-server <BROKER>:9098 \
     --command-config /etc/kafka/client.properties \
     --describe --topic clickstream-events
   ```
   Confirm `remote.storage.enable=true` and `local.retention.ms=86400000` (24h local).
2. If Tiered Storage is not offloading: check S3 bucket permissions for the MSK service role.
3. Emergency: reduce `local.retention.bytes` per topic to force faster offload.

### 3.3 Kafka rebalance storm

**Symptoms:** `CONSUMER-ID` columns are constantly changing; lag oscillates rather than
growing monotonically.

**Check rebalance events:**
```bash
aws logs filter-log-events \
  --log-group-name /aws/flink/pulsecommerce/bronze-writer \
  --filter-pattern "Rebalance" \
  --start-time $(date -d '30 minutes ago' +%s)000
```

**Remediation:**
1. Increase `session.timeout.ms` in the Flink Kafka source properties
   (Flink application property group → `KafkaConsumerConfig`).
2. Ensure Flink checkpoint interval is < `max.poll.interval.ms` (default 300s).
   If checkpoints are taking > 5 minutes, see the Flink Checkpoint Failure runbook.

### 3.4 Poison message / deserialization error

**Symptoms:** Lag grows on one partition only; other partitions are fine.

**Check for errors:**
```bash
aws logs filter-log-events \
  --log-group-name /aws/flink/pulsecommerce/bronze-writer \
  --filter-pattern "DeserializationException OR SchemaRegistryException" \
  --start-time $(date -d '30 minutes ago' +%s)000
```

**Remediation:**
1. Identify the bad message offset from the error log.
2. In the Flink application property group, set `auto.offset.reset=latest` temporarily
   and restart the application to skip the bad message.
3. Investigate the Schema Registry for schema evolution violations.
4. File a bug with the producing team; restore `auto.offset.reset=earliest` after fix.

---

## 4. Flink Application Down

If the consumer group shows no active consumers (`CONSUMER-ID` is empty), the Flink
application is not running.

### 4.1 Restart from latest checkpoint

```bash
# Via AWS Console: Managed Flink → Application → Actions → Run
# Or via CLI:
aws kinesisanalyticsv2 start-application \
  --application-name pulsecommerce-bronze-writer \
  --run-configuration '{"ApplicationRestoreConfiguration": {"ApplicationRestoreType": "RESTORE_FROM_LATEST_SNAPSHOT"}}'
```

**Monitor recovery:**
```bash
aws kinesisanalyticsv2 describe-application \
  --application-name pulsecommerce-bronze-writer \
  --query 'ApplicationDetail.ApplicationStatus'
```

Expected: `STARTING` → `RUNNING` within 3 minutes.

### 4.2 Application stuck in RESTARTING

If the application has been RESTARTING for > 10 minutes, the checkpoint may be corrupt.

1. Check TaskManager logs for `RestoreException` or `InvalidCheckpointException`.
2. List available snapshots:
   ```bash
   aws kinesisanalyticsv2 list-application-snapshots \
     --application-name pulsecommerce-bronze-writer
   ```
3. Restore from the previous good snapshot:
   ```bash
   aws kinesisanalyticsv2 start-application \
     --application-name pulsecommerce-bronze-writer \
     --run-configuration '{
       "ApplicationRestoreConfiguration": {
         "ApplicationRestoreType": "RESTORE_FROM_CUSTOM_SNAPSHOT",
         "SnapshotName": "<SNAPSHOT_NAME>"
       }
     }'
   ```
4. If no good snapshot exists, restore with `SKIP_RESTORE_FROM_SNAPSHOT` (state is lost;
   consumer group resets to latest offset — **some events will be skipped**).
   - After recovery: trigger a manual Bronze→Silver Glue job for the affected time window.

---

## 5. Post-Incident Steps

1. Confirm `SumOffsetLag` drops below 10,000 and is trending to 0.
2. Verify Bronze Iceberg table has new snapshots (Athena: `SELECT * FROM bronze.clickstream.snapshots ORDER BY committed_at DESC LIMIT 5`).
3. Verify Silver Iceberg table is up to date (check `processed_at` max timestamp).
4. If lag exceeded 4 hours: check whether any fraud alerts were missed during the window
   and trigger a manual reprocessing Glue job for that period.
5. File an incident report documenting:
   - Root cause
   - Duration of lag
   - Estimated number of delayed events
   - Remediation steps taken

---

## 6. Escalation

| Duration | Action |
|----------|--------|
| 0–15 min | On-call engineer investigates per this runbook |
| 15–30 min | Escalate to Platform Engineering lead |
| > 30 min | Escalate to Engineering Manager; consider disabling fraud checks downstream |

---

## 7. Related Runbooks and References

- `docs/runbooks/flink-checkpoint-failure-runbook.md`
- Terraform module: `infrastructure/terraform/modules/msk/main.tf`
- CloudWatch dashboard: `PulseCommerce-MSK-Overview` (bookmark in shared console)
- MSK consumer lag alarm: `MSKConsumerLag` in CloudWatch Alarms
