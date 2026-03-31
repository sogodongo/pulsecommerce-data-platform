# Runbook: Flink Checkpoint Failure — Detection and Remediation

| Field        | Value                                               |
|--------------|-----------------------------------------------------|
| **Service**  | Amazon Managed Flink (Kinesis Analytics v2)         |
| **Alarm**    | `FlinkApplicationDowntime` — uptime metric < 1     |
| **Severity** | CRITICAL                                            |
| **Team**     | Platform Engineering / On-Call                      |
| **Updated**  | 2024-06-01                                          |

---

## 1. What This Alarm Means

The `uptime` CloudWatch metric for one or more Flink applications has dropped below 1,
indicating the application has stopped or is restarting. Flink applications restart
automatically after failure, but if checkpoints fail, the application may enter a restart
loop or fall back to an earlier state, causing duplicate or lost event processing.

**Affected applications:**
- `pulsecommerce-bronze-writer` — writes Bronze Iceberg records from Kafka
- `pulsecommerce-session-stitcher` — builds real-time session windows
- `pulsecommerce-fraud-scorer` — produces fraud risk scores

---

## 2. Initial Triage (< 5 minutes)

### 2.1 Check application status

```bash
for app in pulsecommerce-bronze-writer pulsecommerce-session-stitcher pulsecommerce-fraud-scorer; do
  echo "=== $app ==="
  aws kinesisanalyticsv2 describe-application \
    --application-name "$app" \
    --query 'ApplicationDetail.{Status:ApplicationStatus,LastUpdate:LastUpdateTimestamp}' \
    --output table
done
```

Expected: `RUNNING`. Investigate if: `STOPPING`, `STOPPED`, `STARTING`, `RESTARTING`.

### 2.2 Check checkpoint metrics in CloudWatch

Navigate to: CloudWatch → Metrics → Kinesis Analytics → Application Metrics

Key metrics to examine:
| Metric | Healthy Value | Problem |
|--------|--------------|---------|
| `lastCheckpointDuration` | < 60,000 ms | Checkpoints taking too long |
| `lastCheckpointSize` | < 10 GB | State too large |
| `numberOfFailedCheckpoints` | 0 | Checkpoint failures |
| `uptime` | > 0 increasing | 0 = application down |
| `fullRestarts` | 0 | > 0 = crash loop |

### 2.3 Check TaskManager logs

```bash
# Replace APPLICATION_NAME with the affected app
aws logs filter-log-events \
  --log-group-name /aws/flink/pulsecommerce/<APPLICATION_NAME> \
  --filter-pattern "CheckpointException OR checkpoint failed OR RestoreException" \
  --start-time $(date -d '30 minutes ago' +%s)000 \
  --limit 50
```

---

## 3. Common Causes and Remediation

### 3.1 Checkpoint timeout (most common)

**Symptoms:** `lastCheckpointDuration` consistently > 60s; `numberOfFailedCheckpoints` > 0;
application may still be RUNNING but will eventually restart.

**Root causes:**
- State size has grown too large (RocksDB compaction backlog)
- S3 write throughput throttled during checkpoint
- TaskManager GC pause during checkpoint

**Immediate remediation:**
1. Verify checkpoint S3 bucket (`pulsecommerce-flink-checkpoints`) has no bucket policy
   blocking the Flink IAM role.
2. Check S3 request throttling:
   ```bash
   aws cloudwatch get-metric-statistics \
     --namespace AWS/S3 \
     --metric-name 5xxErrors \
     --dimensions Name=BucketName,Value=pulsecommerce-flink-checkpoints \
     --start-time $(date -d '1 hour ago' -u +%Y-%m-%dT%H:%M:%SZ) \
     --end-time $(date -u +%Y-%m-%dT%H:%M:%SZ) \
     --period 60 --statistics Sum
   ```
3. If S3 throttling: add S3 request rate prefix randomisation to checkpoint path
   (change `state.checkpoints.dir` to use a hash prefix).
4. If state size > 10 GB: the session stitcher may have accumulated stale sessions.
   Trigger a manual state cleanup:
   - Stop the application with a savepoint
   - Inspect RocksDB state size in the checkpoint S3 path
   - Restart with increased `taskmanager.memory.managed.fraction`

**Configuration tuning (requires application restart):**
```
# Increase checkpoint timeout (current: 5 min)
execution.checkpointing.timeout: 600000

# Enable incremental checkpoints (should already be enabled)
state.backend.incremental: true

# Reduce checkpoint interval during recovery
execution.checkpointing.interval: 120000
```

### 3.2 Out-of-memory / GC pressure

**Symptoms:** `fullRestarts` incrementing; logs contain `OutOfMemoryError` or
`GCOverheadLimitExceeded`; checkpoint fails immediately after start.

**Check:**
```bash
aws logs filter-log-events \
  --log-group-name /aws/flink/pulsecommerce/<APPLICATION_NAME> \
  --filter-pattern "OutOfMemoryError OR GCOverheadLimit" \
  --start-time $(date -d '1 hour ago' +%s)000
```

**Remediation:**
1. Increase KPU count for the application:
   - Console → Managed Flink → Application → Configure
   - Increase `Parallelism` (adds KPUs, each KPU = 1 vCPU + 4 GB)
2. If session stitcher: the `session_events_state` ListState may be accumulating unbounded
   events. Verify that the session gap timer (`SESSION_GAP_MS = 1800000`) is firing and
   clearing state. Check for timer registration failures in logs.
3. If fraud scorer: verify that the `last_country_state` and `event_ts_state` are being
   cleared on `on_timer()` — unbounded growth should not occur, but verify with a log grep:
   ```bash
   aws logs filter-log-events \
     --log-group-name /aws/flink/pulsecommerce/fraud-scorer \
     --filter-pattern "on_timer" \
     --start-time $(date -d '1 hour ago' +%s)000 \
     --limit 20
   ```

### 3.3 S3 backend write failure

**Symptoms:** Checkpoint fails with `IOException` or `AccessDeniedException`; application
enters restart loop.

**Check IAM permissions:**
```bash
aws iam simulate-principal-policy \
  --policy-source-arn arn:aws:iam::<ACCOUNT_ID>:role/pulsecommerce-flink-role \
  --action-names s3:PutObject s3:GetObject s3:DeleteObject \
  --resource-arns "arn:aws:s3:::pulsecommerce-flink-checkpoints/*"
```

All actions should return `allowed`.

**Check KMS permissions:**
```bash
aws iam simulate-principal-policy \
  --policy-source-arn arn:aws:iam::<ACCOUNT_ID>:role/pulsecommerce-flink-role \
  --action-names kms:GenerateDataKey kms:Decrypt \
  --resource-arns "arn:aws:kms:us-east-1:<ACCOUNT_ID>:key/<LAKEHOUSE_KMS_KEY_ID>"
```

**Remediation:**
- If permission denied: check for recent IAM policy changes in CloudTrail.
- If KMS key disabled: re-enable via `aws kms enable-key --key-id <KEY_ID>`.
- If S3 bucket policy changed: revert to Terraform-managed state.

### 3.4 Kafka source connectivity loss

**Symptoms:** Application is RUNNING, but checkpoint fails with
`KafkaConsumerException`; MSK lag alarm may also be firing.

**Check:**
```bash
aws logs filter-log-events \
  --log-group-name /aws/flink/pulsecommerce/bronze-writer \
  --filter-pattern "KafkaException OR Connection refused OR Authentication failed" \
  --start-time $(date -d '30 minutes ago' +%s)000
```

**Remediation:**
1. Verify MSK security group allows inbound port 9098 from the Flink security group.
2. Check MSK broker health in the console.
3. If IAM auth failure: rotate the Flink IAM role trust policy and verify the MSK
   cluster `allow.everyone.if.no.acl.found=false` setting.

---

## 4. Savepoint-Based Recovery

Use savepoints for controlled restarts (preserves state exactly, no event loss or duplication).

### 4.1 Create a savepoint before stopping

```bash
# Get the application ARN
APP_ARN=$(aws kinesisanalyticsv2 describe-application \
  --application-name pulsecommerce-bronze-writer \
  --query 'ApplicationDetail.ApplicationARN' --output text)

# Trigger a snapshot (Managed Flink's equivalent of a Flink savepoint)
aws kinesisanalyticsv2 create-application-snapshot \
  --application-name pulsecommerce-bronze-writer \
  --snapshot-name "manual-$(date +%Y%m%d-%H%M%S)"
```

### 4.2 Stop the application

```bash
aws kinesisanalyticsv2 stop-application \
  --application-name pulsecommerce-bronze-writer \
  --force false   # wait for in-flight records to complete
```

### 4.3 Restart from savepoint

```bash
SNAPSHOT_NAME="manual-20240615-103000"  # replace with actual snapshot name

aws kinesisanalyticsv2 start-application \
  --application-name pulsecommerce-bronze-writer \
  --run-configuration "{
    \"ApplicationRestoreConfiguration\": {
      \"ApplicationRestoreType\": \"RESTORE_FROM_CUSTOM_SNAPSHOT\",
      \"SnapshotName\": \"$SNAPSHOT_NAME\"
    }
  }"
```

---

## 5. Emergency: State Reset (Last Resort)

**WARNING:** This procedure discards all in-flight state. Use only if:
- No valid savepoint/snapshot exists
- Application has been down > 2 hours with unrecoverable state
- Data loss is acceptable for the affected window

```bash
aws kinesisanalyticsv2 start-application \
  --application-name pulsecommerce-bronze-writer \
  --run-configuration '{
    "ApplicationRestoreConfiguration": {
      "ApplicationRestoreType": "SKIP_RESTORE_FROM_SNAPSHOT"
    }
  }'
```

**Post-reset actions required:**
1. Identify the time window of the outage.
2. Trigger a manual Bronze→Silver Glue job for that window:
   - Set `LAST_SNAPSHOT_ID=0` to force full reload of the affected partition.
3. Verify Silver Iceberg table has no gaps (check `processed_at` continuity in Athena).
4. For fraud scorer: active sessions will lose accumulated risk scores. Alert the fraud
   operations team to review manual queues for the affected time window.

---

## 6. Post-Incident Steps

1. Confirm all three Flink applications are RUNNING with `uptime` metric > 0.
2. Confirm `numberOfFailedCheckpoints` is 0 for 15 consecutive minutes.
3. Confirm `lastCheckpointDuration` < 60,000 ms.
4. Check MSK consumer lag (see MSK Consumer Lag runbook) to verify backlog is clearing.
5. Verify Bronze and Silver Iceberg tables have recent snapshots.
6. File incident report including:
   - Which application failed
   - Checkpoint failure type and root cause
   - State loss (if any)
   - Events potentially affected (estimated count from MSK lag + duration)

---

## 7. Escalation

| Duration | Action |
|----------|--------|
| 0–10 min | On-call engineer investigates per this runbook |
| 10–20 min | Escalate to Platform Engineering lead |
| > 20 min | Escalate to Engineering Manager; consider disabling fraud checks |

---

## 8. Related Runbooks and References

- `docs/runbooks/msk-consumer-lag-runbook.md`
- Terraform module: `infrastructure/terraform/modules/flink/main.tf`
- Flink state backend configuration: application property groups in Managed Flink console
- CloudWatch alarms: `FlinkBronzeWriterDowntime`, `FlinkSessionStitcherDowntime`,
  `FlinkFraudScorerDowntime`
- Checkpoint S3 path: `s3://pulsecommerce-flink-checkpoints/`
