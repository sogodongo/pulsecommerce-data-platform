"""
ingestion/batch/product_catalog_producer.py

Polls the internal Product Catalog Service REST API every 15 minutes,
normalises product records, and produces to MSK topic
`prod.ecommerce.product-catalog.v1`.

Triggered by: AWS EventBridge Scheduler → Lambda (every 15 minutes)
Deployed as:  AWS Lambda (Python 3.11, 256 MB, 5 min timeout)

Design notes:
- Uses an ETag / Last-Modified cache in DynamoDB to skip unchanged pages.
- Only changed/new products are produced to Kafka (delta-only approach).
- Partition key: sku — ensures all updates for a product go to the same
  partition, preserving order for Flink enrichment lookups.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Iterator

import boto3
import requests
from confluent_kafka import Producer
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

TOPIC = "prod.ecommerce.product-catalog.v1"
ETAG_TABLE = "pulsecommerce-catalog-etag-cache"   # DynamoDB table for ETag state
PAGE_SIZE = 200


# ─────────────────────────────────────────────────────────────────────────────
# Canonical product record
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ProductRecord:
    sku: str
    name: str
    category: str
    subcategory: str | None
    brand: str
    price_usd: float
    cost_usd: float | None
    margin_pct: float | None
    currency: str
    stock_quantity: int
    is_active: bool
    weight_kg: float | None
    tags: list[str]
    image_url: str | None
    created_at: str
    updated_at: str
    ingested_at: str
    content_hash: str               # SHA-256 of key fields — used for change detection

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_api_response(cls, raw: dict) -> "ProductRecord":
        now = datetime.now(timezone.utc).isoformat()
        price = float(raw.get("price_usd") or raw.get("price") or 0.0)
        cost = float(raw["cost_usd"]) if raw.get("cost_usd") else None
        margin = round((price - cost) / price * 100, 2) if cost and price else None

        key_fields = f"{raw['sku']}|{price}|{raw.get('name')}|{raw.get('stock_quantity', 0)}|{raw.get('is_active', True)}"
        content_hash = hashlib.sha256(key_fields.encode()).hexdigest()

        return cls(
            sku=raw["sku"],
            name=raw.get("name", ""),
            category=raw.get("category", "uncategorised"),
            subcategory=raw.get("subcategory"),
            brand=raw.get("brand", ""),
            price_usd=price,
            cost_usd=cost,
            margin_pct=margin,
            currency=raw.get("currency", "USD"),
            stock_quantity=int(raw.get("stock_quantity", 0)),
            is_active=bool(raw.get("is_active", True)),
            weight_kg=float(raw["weight_kg"]) if raw.get("weight_kg") else None,
            tags=raw.get("tags") or [],
            image_url=raw.get("image_url"),
            created_at=raw.get("created_at", now),
            updated_at=raw.get("updated_at", now),
            ingested_at=now,
            content_hash=content_hash,
        )


# ─────────────────────────────────────────────────────────────────────────────
# ETag cache (DynamoDB) — skip unchanged pages
# ─────────────────────────────────────────────────────────────────────────────

class ETagCache:
    """Persists HTTP ETag / content hashes to DynamoDB to detect catalog changes."""

    def __init__(self):
        self._ddb = boto3.resource("dynamodb", region_name=os.environ["AWS_REGION"])
        self._table = self._ddb.Table(ETAG_TABLE)

    def get(self, key: str) -> str | None:
        try:
            resp = self._table.get_item(Key={"cache_key": key})
            return resp.get("Item", {}).get("etag")
        except Exception:
            return None

    def put(self, key: str, etag: str) -> None:
        try:
            self._table.put_item(Item={"cache_key": key, "etag": etag, "updated_at": datetime.now(timezone.utc).isoformat()})
        except Exception:
            logger.warning("ETagCache put failed for key=%s — continuing without cache", key)


# ─────────────────────────────────────────────────────────────────────────────
# Product Catalog API client
# ─────────────────────────────────────────────────────────────────────────────

class ProductCatalogClient:
    """
    Thin client for the internal Product Catalog REST API.
    Supports cursor-based pagination and conditional GET (ETag).
    """

    def __init__(self, base_url: str, api_key: str, etag_cache: ETagCache):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({
            "X-API-Key": api_key,
            "Accept": "application/json",
            "User-Agent": "PulseCommerce-DataPlatform/1.0",
        })
        self.etag_cache = etag_cache

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=15),
        retry=retry_if_exception_type(requests.RequestException),
    )
    def _get_page(self, cursor: str | None = None) -> tuple[list[dict], str | None, bool]:
        """
        Fetch one page of products.
        Returns (records, next_cursor, is_unchanged).
        is_unchanged=True when server returns 304 Not Modified.
        """
        params: dict = {"limit": PAGE_SIZE}
        if cursor:
            params["cursor"] = cursor

        cache_key = f"page:{cursor or 'first'}"
        cached_etag = self.etag_cache.get(cache_key)
        headers = {}
        if cached_etag:
            headers["If-None-Match"] = cached_etag

        resp = self.session.get(
            f"{self.base_url}/v1/products",
            params=params,
            headers=headers,
            timeout=20,
        )

        if resp.status_code == 304:
            logger.debug("Page cursor=%s unchanged (304)", cursor)
            return [], None, True

        resp.raise_for_status()
        data = resp.json()

        new_etag = resp.headers.get("ETag")
        if new_etag:
            self.etag_cache.put(cache_key, new_etag)

        records = data.get("products") or data.get("data") or []
        next_cursor = data.get("next_cursor") or data.get("pagination", {}).get("next_cursor")
        return records, next_cursor, False

    def iter_all_products(self) -> Iterator[ProductRecord]:
        """Paginate through the full catalog, yielding ProductRecord objects."""
        cursor = None
        page_num = 0

        while True:
            page_num += 1
            raw_records, next_cursor, is_unchanged = self._get_page(cursor)

            if is_unchanged:
                # Entire page unchanged — skip to next cursor (we don't know it without fetching)
                # In production: track cursor → next_cursor mapping in DynamoDB
                logger.debug("Page %d unchanged, skipping", page_num)
                break

            for raw in raw_records:
                try:
                    yield ProductRecord.from_api_response(raw)
                except (KeyError, ValueError) as exc:
                    logger.warning("Skipping malformed product record sku=%s: %s", raw.get("sku"), exc)

            logger.info("Fetched page %d: %d records, next_cursor=%s", page_num, len(raw_records), next_cursor)

            if not next_cursor:
                break
            cursor = next_cursor


# ─────────────────────────────────────────────────────────────────────────────
# Change detection — skip records that haven't changed since last run
# ─────────────────────────────────────────────────────────────────────────────

class SkuHashCache:
    """Tracks content_hash per SKU in DynamoDB to avoid re-producing unchanged products."""

    def __init__(self):
        self._ddb = boto3.resource("dynamodb", region_name=os.environ["AWS_REGION"])
        self._table = self._ddb.Table(ETAG_TABLE)

    def has_changed(self, sku: str, content_hash: str) -> bool:
        try:
            resp = self._table.get_item(Key={"cache_key": f"sku:{sku}"})
            stored = resp.get("Item", {}).get("etag")
            return stored != content_hash
        except Exception:
            return True  # assume changed if we can't check

    def mark_seen(self, sku: str, content_hash: str) -> None:
        try:
            self._table.put_item(Item={
                "cache_key": f"sku:{sku}",
                "etag": content_hash,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            })
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────────────────────
# Kafka producer
# ─────────────────────────────────────────────────────────────────────────────

def _build_producer() -> Producer:
    msk_brokers = os.environ["MSK_BROKERS"]
    return Producer({
        "bootstrap.servers": msk_brokers,
        "security.protocol": "SSL",
        "compression.type": "lz4",
        "batch.size": 32768,
        "linger.ms": 50,
        "acks": "all",
        "retries": 5,
        "enable.idempotence": True,
    })


def _delivery_callback(err, msg) -> None:
    if err:
        logger.error("Delivery failed sku=%s: %s", msg.key(), err)


# ─────────────────────────────────────────────────────────────────────────────
# Lambda / CLI entrypoint
# ─────────────────────────────────────────────────────────────────────────────

def handler(event: dict, context) -> dict:
    """
    AWS Lambda handler. Runs on 15-minute EventBridge schedule.
    event = {} (no parameters required; always pulls current catalog state).
    """
    catalog_base_url = os.environ["PRODUCT_CATALOG_API_URL"]
    api_key = os.environ.get("PRODUCT_CATALOG_API_KEY", "")

    etag_cache = ETagCache()
    sku_cache = SkuHashCache()
    catalog_client = ProductCatalogClient(catalog_base_url, api_key, etag_cache)
    producer = _build_producer()

    produced = 0
    skipped = 0

    for product in catalog_client.iter_all_products():
        if not sku_cache.has_changed(product.sku, product.content_hash):
            skipped += 1
            continue

        payload = json.dumps(product.to_dict()).encode("utf-8")
        producer.produce(
            topic=TOPIC,
            key=product.sku.encode("utf-8"),
            value=payload,
            on_delivery=_delivery_callback,
        )
        sku_cache.mark_seen(product.sku, product.content_hash)
        produced += 1

        if produced % 500 == 0:
            producer.poll(0)
            logger.info("Progress: %d produced, %d skipped (unchanged)", produced, skipped)

    producer.flush(timeout=30)

    logger.info(
        "Product catalog ingestion complete. produced=%d skipped=%d",
        produced, skipped,
    )
    return {
        "status": "ok",
        "records_produced": produced,
        "records_skipped_unchanged": skipped,
    }


if __name__ == "__main__":
    result = handler({}, None)
    print(json.dumps(result, indent=2))
