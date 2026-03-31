"""
ingestion/batch/ad_attribution_producer.py

Pulls daily ad attribution data from Facebook Marketing API and Google Ads API,
normalises both into a canonical attribution schema, and produces records to
the MSK topic `prod.ecommerce.ad-attribution.v1`.

Triggered by: AWS EventBridge Scheduler → Lambda (daily at 02:00 UTC)
Deployed as:  AWS Lambda (Python 3.11 runtime, 512 MB, 15 min timeout)
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Iterator

import boto3
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

TOPIC = "prod.ecommerce.ad-attribution.v1"

# ─────────────────────────────────────────────────────────────────────────────
# Canonical attribution record
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class AttributionRecord:
    """Normalised attribution record written to MSK."""
    source: str                     # facebook | google
    campaign_id: str
    campaign_name: str
    ad_set_id: str
    ad_set_name: str
    ad_id: str
    ad_name: str
    report_date: str                # YYYY-MM-DD
    impressions: int
    clicks: int
    spend_usd: float
    conversions: int
    conversion_value_usd: float
    currency: str
    country: str
    device_platform: str           # mobile | desktop | tablet | unknown
    ingested_at: str               # ISO-8601 UTC
    raw_payload: dict = field(default_factory=dict, repr=False)

    def to_dict(self) -> dict:
        d = {k: v for k, v in self.__dict__.items() if k != "raw_payload"}
        return d


# ─────────────────────────────────────────────────────────────────────────────
# Kafka producer setup
# ─────────────────────────────────────────────────────────────────────────────

def _build_producer() -> Producer:
    msk_brokers = _get_secret("MSK_BROKERS") or os.environ["MSK_BROKERS"]
    return Producer({
        "bootstrap.servers": msk_brokers,
        "security.protocol": "SSL",
        "compression.type": "lz4",
        "batch.size": 65536,           # 64 KB batch
        "linger.ms": 100,              # wait 100 ms to fill batches
        "acks": "all",                 # full durability (all ISRs must ack)
        "retries": 5,
        "retry.backoff.ms": 500,
        "delivery.timeout.ms": 120000,
        "enable.idempotence": True,    # exactly-once producer semantics
        "message.max.bytes": 1048576,
    })


def _delivery_callback(err, msg) -> None:
    if err:
        logger.error("Delivery failed for key=%s: %s", msg.key(), err)
    else:
        logger.debug("Delivered to %s [%d] @ offset %d", msg.topic(), msg.partition(), msg.offset())


# ─────────────────────────────────────────────────────────────────────────────
# AWS Secrets Manager helper
# ─────────────────────────────────────────────────────────────────────────────

def _get_secret(secret_name: str) -> str | None:
    """Fetch a secret from AWS Secrets Manager. Returns None on failure."""
    try:
        client = boto3.client("secretsmanager", region_name=os.environ["AWS_REGION"])
        response = client.get_secret_value(SecretId=f"pulsecommerce/{secret_name}")
        return response["SecretString"]
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Facebook Marketing API
# ─────────────────────────────────────────────────────────────────────────────

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_facebook_attribution(report_date: date) -> list[AttributionRecord]:
    """
    Pull daily ad performance from the Facebook Marketing API (Insights endpoint).
    Uses the facebook-business SDK under the hood.
    """
    import requests

    access_token = _get_secret("FACEBOOK_ACCESS_TOKEN") or os.environ["FACEBOOK_ACCESS_TOKEN"]
    ad_account_id = os.environ["FACEBOOK_AD_ACCOUNT_ID"]
    date_str = report_date.isoformat()

    url = f"https://graph.facebook.com/v19.0/{ad_account_id}/insights"
    params = {
        "access_token": access_token,
        "time_range": json.dumps({"since": date_str, "until": date_str}),
        "level": "ad",
        "fields": ",".join([
            "campaign_id", "campaign_name",
            "adset_id", "adset_name",
            "ad_id", "ad_name",
            "impressions", "clicks", "spend",
            "actions", "action_values",
            "account_currency",
        ]),
        "breakdowns": "device_platform,country",
        "limit": 500,
    }

    records: list[AttributionRecord] = []
    from datetime import datetime, timezone

    while url:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()

        for row in payload.get("data", []):
            conversions = sum(
                int(a["value"]) for a in row.get("actions", [])
                if a["action_type"] == "purchase"
            )
            conversion_value = sum(
                float(a["value"]) for a in row.get("action_values", [])
                if a["action_type"] == "purchase"
            )
            records.append(AttributionRecord(
                source="facebook",
                campaign_id=row["campaign_id"],
                campaign_name=row["campaign_name"],
                ad_set_id=row["adset_id"],
                ad_set_name=row["adset_name"],
                ad_id=row["ad_id"],
                ad_name=row["ad_name"],
                report_date=date_str,
                impressions=int(row.get("impressions", 0)),
                clicks=int(row.get("clicks", 0)),
                spend_usd=float(row.get("spend", 0.0)),
                conversions=conversions,
                conversion_value_usd=conversion_value,
                currency=row.get("account_currency", "USD"),
                country=row.get("country", "UNKNOWN"),
                device_platform=row.get("device_platform", "unknown"),
                ingested_at=datetime.now(timezone.utc).isoformat(),
                raw_payload=row,
            ))

        # Pagination
        paging = payload.get("paging", {})
        url = paging.get("next")
        params = {}  # next URL already contains all params

    logger.info("Fetched %d Facebook attribution records for %s", len(records), date_str)
    return records


# ─────────────────────────────────────────────────────────────────────────────
# Google Ads API
# ─────────────────────────────────────────────────────────────────────────────

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_google_attribution(report_date: date) -> list[AttributionRecord]:
    """
    Pull daily ad performance from the Google Ads API using GAQL.
    Requires google-ads Python client library.
    """
    from google.ads.googleads.client import GoogleAdsClient
    from datetime import datetime, timezone

    developer_token = _get_secret("GOOGLE_ADS_DEVELOPER_TOKEN") or os.environ["GOOGLE_ADS_DEVELOPER_TOKEN"]
    customer_id = os.environ["GOOGLE_ADS_CUSTOMER_ID"].replace("-", "")
    date_str = report_date.isoformat()

    client = GoogleAdsClient.load_from_dict({
        "developer_token": developer_token,
        "use_proto_plus": True,
    })
    ga_service = client.get_service("GoogleAdsService")

    query = f"""
        SELECT
            campaign.id,
            campaign.name,
            ad_group.id,
            ad_group.name,
            ad_group_ad.ad.id,
            ad_group_ad.ad.name,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros,
            metrics.conversions,
            metrics.conversions_value,
            segments.device,
            segments.geo_target_country,
            customer.currency_code
        FROM ad_group_ad
        WHERE segments.date = '{date_str}'
          AND campaign.status = 'ENABLED'
    """

    records: list[AttributionRecord] = []
    response = ga_service.search(customer_id=customer_id, query=query)

    device_map = {"MOBILE": "mobile", "DESKTOP": "desktop", "TABLET": "tablet"}

    for row in response:
        records.append(AttributionRecord(
            source="google",
            campaign_id=str(row.campaign.id),
            campaign_name=row.campaign.name,
            ad_set_id=str(row.ad_group.id),
            ad_set_name=row.ad_group.name,
            ad_id=str(row.ad_group_ad.ad.id),
            ad_name=row.ad_group_ad.ad.name,
            report_date=date_str,
            impressions=int(row.metrics.impressions),
            clicks=int(row.metrics.clicks),
            spend_usd=row.metrics.cost_micros / 1_000_000,
            conversions=int(row.metrics.conversions),
            conversion_value_usd=float(row.metrics.conversions_value),
            currency=row.customer.currency_code,
            country=row.segments.geo_target_country,
            device_platform=device_map.get(row.segments.device.name, "unknown"),
            ingested_at=datetime.now(timezone.utc).isoformat(),
        ))

    logger.info("Fetched %d Google Ads attribution records for %s", len(records), date_str)
    return records


# ─────────────────────────────────────────────────────────────────────────────
# Producer
# ─────────────────────────────────────────────────────────────────────────────

def normalize_and_produce(records: list[AttributionRecord], producer: Producer) -> int:
    """Serialize records to JSON and produce to MSK. Returns records produced."""
    produced = 0
    for record in records:
        payload = json.dumps(record.to_dict()).encode("utf-8")
        key = record.campaign_id.encode("utf-8")
        producer.produce(
            topic=TOPIC,
            key=key,
            value=payload,
            on_delivery=_delivery_callback,
        )
        produced += 1
        # Poll to serve delivery callbacks every 1000 records
        if produced % 1000 == 0:
            producer.poll(0)

    producer.flush(timeout=30)
    return produced


# ─────────────────────────────────────────────────────────────────────────────
# Lambda / CLI entrypoint
# ─────────────────────────────────────────────────────────────────────────────

def handler(event: dict, context) -> dict:
    """
    AWS Lambda handler.
    event = {"report_date": "2025-11-14"}  or empty (defaults to yesterday).
    """
    report_date_str = event.get("report_date")
    if report_date_str:
        report_date = date.fromisoformat(report_date_str)
    else:
        report_date = date.today() - timedelta(days=1)

    logger.info("Starting ad attribution ingestion for %s", report_date)

    producer = _build_producer()
    total_produced = 0

    try:
        fb_records = fetch_facebook_attribution(report_date)
        total_produced += normalize_and_produce(fb_records, producer)
        logger.info("Produced %d Facebook records", len(fb_records))
    except Exception:
        logger.exception("Facebook attribution fetch failed — skipping, will retry next run")

    try:
        goog_records = fetch_google_attribution(report_date)
        total_produced += normalize_and_produce(goog_records, producer)
        logger.info("Produced %d Google records", len(goog_records))
    except Exception:
        logger.exception("Google Ads attribution fetch failed — skipping, will retry next run")

    logger.info("Ad attribution ingestion complete. Total records produced: %d", total_produced)
    return {"status": "ok", "report_date": report_date.isoformat(), "records_produced": total_produced}


if __name__ == "__main__":
    # Local testing: python ad_attribution_producer.py 2025-11-14
    import sys
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    result = handler({"report_date": date_arg} if date_arg else {}, None)
    print(json.dumps(result, indent=2))
