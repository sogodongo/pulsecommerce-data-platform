-- =============================================================================
-- PulseCommerce — Bronze Layer Iceberg Table DDL
-- =============================================================================
-- Zone:    Bronze (raw, immutable)
-- Engine:  AWS Glue 5.0 (Spark 3.5) / Amazon Athena v3
-- Catalog: AWS Glue Data Catalog (glue_catalog)
-- Storage: Amazon S3 Tables bucket (built-in Iceberg support)
-- SLA:     Written within 30s of Kafka offset commit (Flink streaming write)
--
-- Design principles:
--   • Raw source data is preserved unchanged (raw_payload column)
--   • Schema validated at write time but no transformations applied
--   • PII fields present — access restricted to data-engineering-role only
--   • Partitioned by ingestion time (event_date, event_hour) for incremental
--     Silver reads using Iceberg snapshot diffs
--   • format-version=2 enables row-level deletes (for GDPR erasure requests)
--   • All tables use hidden partitioning — analysts query on event_ts directly
-- =============================================================================

-- Create Glue catalog databases if they don't exist
CREATE DATABASE IF NOT EXISTS glue_catalog.bronze
COMMENT 'Raw, immutable Bronze zone — PulseCommerce Medallion Lakehouse';


-- -----------------------------------------------------------------------------
-- 1. bronze.clickstream
--    Source: Web / Mobile SDK events via MSK → Flink Bronze Writer job
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS glue_catalog.bronze.clickstream (

    -- Identity
    event_id        STRING      COMMENT 'UUID v4 — globally unique. Primary dedup key.',
    user_id         STRING      COMMENT 'Platform user ID (PII — hashed in Silver). Partition key on MSK.',
    session_id      STRING      COMMENT 'Client-generated session identifier.',

    -- Event classification
    event_type      STRING      COMMENT 'product_view | add_to_cart | remove_from_cart | checkout_start | checkout_complete | purchase | search | page_view | wishlist_add | wishlist_remove',

    -- Timing
    event_ts        TIMESTAMP   COMMENT 'Client-side event timestamp (may drift; 4h late-arrival watermark in Flink).',
    ingested_at     TIMESTAMP   COMMENT 'Server-side timestamp when Flink wrote to Bronze.',
    kafka_offset    BIGINT      COMMENT 'MSK partition offset — used for exactly-once tracking.',
    kafka_partition INT         COMMENT 'MSK partition number.',

    -- Device context
    device_type     STRING      COMMENT 'mobile | tablet | desktop',
    device_os       STRING      COMMENT 'iOS | Android | Windows | macOS | Linux',
    app_version     STRING      COMMENT 'App or SDK version string.',
    user_agent      STRING      COMMENT 'Raw User-Agent string (nullable for older clients).',

    -- Page context
    page_url        STRING      COMMENT 'Relative URL of the page/screen.',
    page_referrer   STRING      COMMENT 'HTTP referrer or SDK-reported referrer. Null for direct traffic.',
    page_title      STRING      COMMENT 'Page title or screen name.',

    -- Product context (nullable for non-product events)
    product_sku     STRING      COMMENT 'Stock Keeping Unit. Null for page_view, search events.',
    product_category STRING     COMMENT 'Top-level product category (e.g. footwear, electronics).',
    product_price_usd DOUBLE    COMMENT 'Listed price in USD at event time.',
    product_quantity INT        COMMENT 'Quantity for add_to_cart / purchase events.',

    -- Geographic context
    geo_country     STRING      COMMENT 'ISO 3166-1 alpha-2 country code.',
    geo_city        STRING      COMMENT 'City name (PII — masked to MASKED for EU/UK in Silver).',
    geo_timezone    STRING      COMMENT 'IANA timezone identifier.',
    geo_lat         DOUBLE      COMMENT 'Latitude (optional — only apps with location permission).',
    geo_lon         DOUBLE      COMMENT 'Longitude (optional — only apps with location permission).',

    -- Flags
    is_bot          BOOLEAN     COMMENT 'True if classified as bot/crawler. Filtered in Silver.',
    is_internal     BOOLEAN     COMMENT 'True for internal PulseCommerce staff events.',
    ab_cohort       STRING      COMMENT 'A/B experiment cohort (e.g. exp_checkout_v3).',

    -- Event-specific fields
    search_query    STRING      COMMENT 'Search term. Present only for event_type=search.',
    order_id        STRING      COMMENT 'Order ID. Present only for purchase / checkout_complete.',

    -- Data quality
    dq_flag         STRING      COMMENT 'NULL=passed | WARNING=soft failure | CRITICAL=hard failure (record written but flagged)',
    dq_failure_msg  STRING      COMMENT 'Great Expectations failure message if dq_flag is set.',

    -- Raw preservation for reprocessing
    raw_payload     STRING      COMMENT 'Original JSON event payload. Allows full reprocessing from Bronze on schema changes.',

    -- Partition columns (hidden partitions — Iceberg maps timestamps automatically)
    event_date      DATE        COMMENT 'Partition column: derived from event_ts. Do not filter directly — use event_ts.',
    event_hour      INT         COMMENT 'Partition column: 0–23 hour of event_ts UTC.'
)
USING iceberg
PARTITIONED BY (event_date, event_hour)
TBLPROPERTIES (
    'table_type'                            = 'ICEBERG',
    'format-version'                        = '2',
    'write.target-file-size-bytes'          = '134217728',   -- 128 MB target file size
    'write.distribution-mode'              = 'hash',         -- hash distribution by partition key
    'write.parquet.compression-codec'      = 'snappy',
    'write.merge.mode'                      = 'merge-on-read',
    'read.split.target-size'               = '134217728',
    'history.expire.max-snapshot-age-ms'   = '604800000',   -- expire snapshots older than 7 days
    'history.expire.min-snapshots-to-keep' = '5',
    'write.metadata.compression-codec'     = 'gzip',
    'comment'                               = 'Bronze clickstream — raw Web/Mobile SDK events. PII present. Data Engineering role only.'
);


-- -----------------------------------------------------------------------------
-- 2. bronze.orders_cdc
--    Source: Debezium PostgreSQL CDC → MSK → Flink / Kafka Connect S3 Sink
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS glue_catalog.bronze.orders_cdc (

    -- CDC envelope fields (Debezium ExtractNewRecordState unwrap applied)
    order_id            STRING      COMMENT 'UUID — PK of the orders table.',
    user_id             STRING      COMMENT 'FK to users. PII — hashed in Silver.',
    status              STRING      COMMENT 'pending | confirmed | shipped | delivered | cancelled | refunded',
    currency            STRING      COMMENT 'ISO 4217 code.',
    total_amount        DOUBLE      COMMENT 'Order total in source currency.',
    total_amount_usd    DOUBLE      COMMENT 'Order total in USD at creation-time FX rate.',
    item_count          INT         COMMENT 'Number of distinct SKUs.',
    shipping_country    STRING      COMMENT 'ISO 3166-1 alpha-2 shipping destination.',
    promo_code          STRING      COMMENT 'Applied promotional code.',
    payment_method      STRING      COMMENT 'card | mpesa | paypal | bank_transfer | crypto',
    fraud_flag          BOOLEAN     COMMENT 'Post-auth fraud flag set by scoring service.',
    first_cart_at       TIMESTAMP   COMMENT 'When first item was added to cart (session linkage).',
    created_at          TIMESTAMP   COMMENT 'Order creation timestamp (source DB).',
    updated_at          TIMESTAMP   COMMENT 'Last update timestamp (source DB).',

    -- Debezium source metadata
    cdc_operation       STRING      COMMENT 'c=INSERT, u=UPDATE, d=DELETE, r=snapshot read',
    cdc_ts_ms           BIGINT      COMMENT 'Source DB transaction commit epoch ms.',
    cdc_lsn             BIGINT      COMMENT 'PostgreSQL Log Sequence Number — used for dedup.',
    cdc_source_table    STRING      COMMENT 'Source table: orders | order_items | users | payments',
    ingested_at         TIMESTAMP   COMMENT 'Flink write timestamp.',

    op_date             DATE        COMMENT 'Partition column: date derived from cdc_ts_ms.'
)
USING iceberg
PARTITIONED BY (op_date, cdc_source_table)
TBLPROPERTIES (
    'table_type'                            = 'ICEBERG',
    'format-version'                        = '2',
    'write.target-file-size-bytes'          = '134217728',
    'write.parquet.compression-codec'      = 'snappy',
    'write.merge.mode'                      = 'merge-on-read',
    'history.expire.max-snapshot-age-ms'   = '604800000',
    'history.expire.min-snapshots-to-keep' = '5',
    'comment'                               = 'Bronze CDC events from PostgreSQL orders/users/payments via Debezium. PII present.'
);


-- -----------------------------------------------------------------------------
-- 3. bronze.ad_attribution_raw
--    Source: Lambda ad_attribution_producer (Facebook + Google Ads, daily)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS glue_catalog.bronze.ad_attribution_raw (

    source              STRING      COMMENT 'Ad platform: facebook | google',
    campaign_id         STRING      COMMENT 'Platform campaign identifier.',
    campaign_name       STRING      COMMENT 'Human-readable campaign name.',
    ad_set_id           STRING      COMMENT 'Ad set / ad group identifier.',
    ad_set_name         STRING      COMMENT 'Ad set name.',
    ad_id               STRING      COMMENT 'Individual ad identifier.',
    ad_name             STRING      COMMENT 'Ad creative name.',
    report_date         DATE        COMMENT 'Attribution report date (YYYY-MM-DD).',
    impressions         BIGINT      COMMENT 'Total ad impressions.',
    clicks              BIGINT      COMMENT 'Total clicks.',
    spend_usd           DOUBLE      COMMENT 'Total spend normalised to USD.',
    conversions         INT         COMMENT 'Attributed purchase conversions.',
    conversion_value_usd DOUBLE     COMMENT 'Attributed revenue in USD.',
    currency            STRING      COMMENT 'Original billing currency.',
    country             STRING      COMMENT 'ISO 3166-1 alpha-2 country.',
    device_platform     STRING      COMMENT 'mobile | desktop | tablet | unknown',
    ingested_at         TIMESTAMP   COMMENT 'Lambda write timestamp.'
)
USING iceberg
PARTITIONED BY (report_date, source)
TBLPROPERTIES (
    'table_type'                            = 'ICEBERG',
    'format-version'                        = '2',
    'write.target-file-size-bytes'          = '67108864',    -- 64 MB (lower volume)
    'write.parquet.compression-codec'      = 'snappy',
    'history.expire.max-snapshot-age-ms'   = '2592000000',  -- 30 days (ad data audited monthly)
    'comment'                               = 'Bronze ad attribution — daily pull from Facebook + Google Ads.'
);


-- -----------------------------------------------------------------------------
-- 4. bronze.product_catalog_raw
--    Source: Lambda product_catalog_producer (REST API, every 15 min)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS glue_catalog.bronze.product_catalog_raw (

    sku                 STRING      COMMENT 'Stock Keeping Unit — natural key.',
    name                STRING      COMMENT 'Product display name.',
    category            STRING      COMMENT 'Top-level category.',
    subcategory         STRING      COMMENT 'Sub-category (nullable).',
    brand               STRING      COMMENT 'Brand name.',
    price_usd           DOUBLE      COMMENT 'Listed price in USD.',
    cost_usd            DOUBLE      COMMENT 'COGS in USD (nullable — not all categories disclosed).',
    margin_pct          DOUBLE      COMMENT 'Gross margin % = (price - cost) / price * 100.',
    currency            STRING      COMMENT 'Source currency code.',
    stock_quantity      INT         COMMENT 'Units in stock at ingestion time.',
    is_active           BOOLEAN     COMMENT 'False for delisted / discontinued products.',
    weight_kg           DOUBLE      COMMENT 'Shipping weight in kilograms (nullable).',
    tags                STRING      COMMENT 'JSON array of searchable tags.',
    image_url           STRING      COMMENT 'Primary product image URL.',
    created_at          TIMESTAMP   COMMENT 'Product creation timestamp in source system.',
    updated_at          TIMESTAMP   COMMENT 'Last modification timestamp in source system.',
    ingested_at         TIMESTAMP   COMMENT 'Lambda write timestamp.',
    content_hash        STRING      COMMENT 'SHA-256 of key fields — used for change detection.'
)
USING iceberg
PARTITIONED BY (category)
TBLPROPERTIES (
    'table_type'                            = 'ICEBERG',
    'format-version'                        = '2',
    'write.target-file-size-bytes'          = '33554432',    -- 32 MB (small table)
    'write.parquet.compression-codec'      = 'snappy',
    'write.upsert.enabled'                  = 'true',        -- upsert by sku
    'history.expire.max-snapshot-age-ms'   = '604800000',
    'comment'                               = 'Bronze product catalog — full snapshot every 15 min from Product Service API.'
);
