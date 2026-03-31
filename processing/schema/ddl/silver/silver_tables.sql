CREATE DATABASE IF NOT EXISTS glue_catalog.silver
COMMENT 'Silver zone — cleansed, PII-masked, enriched. 15-minute SLA.';

-- 1. silver.enriched_events
--    Source: bronze.clickstream → Glue bronze_to_silver_events job
--    Joined with: bronze.product_catalog_raw (product enrichment)
CREATE TABLE IF NOT EXISTS glue_catalog.silver.enriched_events (

    -- Identity (PII-safe)
    event_id            STRING      COMMENT 'UUID v4 — dedup key. Exactly one record per event_id.',
    user_id_hashed      STRING      COMMENT 'HMAC-SHA256(user_id || PII_SALT) — irreversible pseudonymisation.',
    session_id          STRING      COMMENT 'Client session identifier (not hashed — no PII).',

    -- Event classification
    event_type          STRING      COMMENT 'Canonical event type enum value.',

    -- Timing
    event_ts            TIMESTAMP   COMMENT 'Client event timestamp (watermarked, late arrivals merged).',
    ingested_at         TIMESTAMP   COMMENT 'Bronze ingestion timestamp.',
    processed_at        TIMESTAMP   COMMENT 'Silver processing timestamp.',

    -- Device context
    device_type         STRING      COMMENT 'mobile | tablet | desktop',
    device_os           STRING      COMMENT 'iOS | Android | Windows | macOS | Linux',
    app_version         STRING      COMMENT 'App/SDK version.',

    -- Page context
    page_url            STRING      COMMENT 'Relative page URL.',
    page_referrer       STRING      COMMENT 'HTTP referrer (nullable).',

    -- Product context (enriched from product catalog)
    product_sku         STRING      COMMENT 'SKU (nullable for non-product events).',
    product_category    STRING      COMMENT 'Top-level category.',
    product_subcategory STRING      COMMENT 'Sub-category (from product catalog lookup).',
    product_brand       STRING      COMMENT 'Brand name (from product catalog lookup).',
    product_price_usd   DOUBLE      COMMENT 'Listed price at event time.',
    product_cost_usd    DOUBLE      COMMENT 'COGS at event time (from catalog — nullable).',
    product_margin_pct  DOUBLE      COMMENT 'Gross margin % at event time.',
    product_quantity    INT         COMMENT 'Quantity for cart/purchase events.',

    -- Geographic context (EU/UK city masked)
    geo_country         STRING      COMMENT 'ISO 3166-1 alpha-2.',
    geo_city            STRING      COMMENT 'City name or MASKED (GDPR: EU/UK users).',
    geo_timezone        STRING      COMMENT 'IANA timezone.',
    geo_region          STRING      COMMENT 'Derived world region: EMEA | APAC | AMER | LATAM',

    -- A/B experimentation
    ab_cohort           STRING      COMMENT 'Experiment cohort (nullable).',

    -- Event-specific payload
    search_query        STRING      COMMENT 'Search term (search events only).',
    order_id            STRING      COMMENT 'Order ID (purchase / checkout_complete only).',

    -- Fraud (from Flink enrichment)
    fraud_score         DOUBLE      COMMENT 'Real-time fraud score [0.0–1.0] assigned by Flink fraud scorer.',
    fraud_signals       STRING      COMMENT 'JSON array of triggered signal names (e.g. ["VELOCITY_SPIKE","GEO_HOP"]).',

    -- Data quality lineage
    dq_flag             STRING      COMMENT 'NULL | WARNING | CRITICAL from Bronze GX validation.',

    -- Partition columns
    event_date          DATE        COMMENT 'Partition column: date of event_ts.'
)
USING iceberg
PARTITIONED BY (event_date)
TBLPROPERTIES (
    'table_type'                            = 'ICEBERG',
    'format-version'                        = '2',
    'write.target-file-size-bytes'          = '134217728',
    'write.parquet.compression-codec'      = 'snappy',
    'write.merge.mode'                      = 'merge-on-read',
    'read.split.target-size'               = '134217728',
    'history.expire.max-snapshot-age-ms'   = '1209600000',  -- 14 days
    'history.expire.min-snapshots-to-keep' = '10',
    'comment'                               = 'Silver enriched events — deduplicated, PII-masked, bot-filtered, product-enriched clickstream.'
);

-- 2. silver.user_sessions
--    Source: Flink session_stitcher job output → Silver merge
--    Pattern: 15-min inactivity gap = new session
CREATE TABLE IF NOT EXISTS glue_catalog.silver.user_sessions (

    session_id              STRING      COMMENT 'Flink-generated session ID (UUID derived from user_id + window start).',
    user_id_hashed          STRING      COMMENT 'HMAC-SHA256 user identifier.',

    -- Session bounds
    session_start_ts        TIMESTAMP   COMMENT 'Timestamp of first event in session.',
    session_end_ts          TIMESTAMP   COMMENT 'Timestamp of last event in session (or gap boundary).',
    session_duration_s      INT         COMMENT 'session_end_ts - session_start_ts in seconds.',

    -- Funnel progression
    funnel_stage_reached    STRING      COMMENT 'Deepest funnel stage: browse | product_view | add_to_cart | checkout_start | purchase',
    page_views              INT         COMMENT 'Total page_view events in session.',
    product_views           INT         COMMENT 'Total product_view events.',
    cart_adds               INT         COMMENT 'Total add_to_cart events.',
    cart_removes            INT         COMMENT 'Total remove_from_cart events.',
    searches                INT         COMMENT 'Total search events.',
    checkout_attempts       INT         COMMENT 'Total checkout_start events.',
    purchases               INT         COMMENT 'Total purchase events (usually 0 or 1).',

    -- Revenue attribution
    revenue_attributed_usd  DOUBLE      COMMENT 'Sum of purchase amounts within session.',
    cart_value_usd          DOUBLE      COMMENT 'Total value of items in cart at session end.',
    cart_abandonment        BOOLEAN     COMMENT 'True if add_to_cart occurred but no purchase.',

    -- Context
    entry_page_url          STRING      COMMENT 'URL of the first page in the session.',
    exit_page_url           STRING      COMMENT 'URL of the last page in the session.',
    entry_referrer          STRING      COMMENT 'Referrer of the entry page.',
    device_type             STRING      COMMENT 'Device type (from first event in session).',
    device_os               STRING      COMMENT 'OS (from first event in session).',
    geo_country             STRING      COMMENT 'Country (from first event in session).',
    ab_cohort               STRING      COMMENT 'A/B cohort (from first event).',

    -- Fraud
    max_fraud_score         DOUBLE      COMMENT 'Maximum fraud_score across all events in session.',
    fraud_flagged           BOOLEAN     COMMENT 'True if max_fraud_score > 0.7.',

    -- Processing metadata
    session_date            DATE        COMMENT 'Partition column: date of session_start_ts.',
    processed_at            TIMESTAMP   COMMENT 'Silver processing timestamp.'
)
USING iceberg
PARTITIONED BY (session_date)
TBLPROPERTIES (
    'table_type'                            = 'ICEBERG',
    'format-version'                        = '2',
    'write.target-file-size-bytes'          = '134217728',
    'write.parquet.compression-codec'      = 'snappy',
    'write.merge.mode'                      = 'merge-on-read',
    'history.expire.max-snapshot-age-ms'   = '1209600000',
    'comment'                               = 'Silver user sessions — Flink session-stitched (15-min inactivity gap). Funnel metrics per session.'
);

-- 3. silver.orders_unified
--    Source: bronze.orders_cdc → Glue bronze_to_silver_orders job
--    Pattern: SCD Type 2 — tracks status transitions over the order lifecycle
CREATE TABLE IF NOT EXISTS glue_catalog.silver.orders_unified (

    -- Natural key
    order_id            STRING      COMMENT 'Order UUID — PK in source system.',
    user_id_hashed      STRING      COMMENT 'HMAC-SHA256 user identifier.',

    -- Order state
    status              STRING      COMMENT 'Current status: pending | confirmed | shipped | delivered | cancelled | refunded',
    prev_status         STRING      COMMENT 'Previous status (for transition analysis).',
    status_changed_at   TIMESTAMP   COMMENT 'Timestamp of the most recent status change.',

    -- Financials
    currency            STRING      COMMENT 'Source currency ISO 4217.',
    total_amount        DOUBLE      COMMENT 'Order total in source currency.',
    total_amount_usd    DOUBLE      COMMENT 'Order total in USD.',
    item_count          INT         COMMENT 'Distinct SKU count.',
    promo_code          STRING      COMMENT 'Applied promo code (nullable).',
    discount_usd        DOUBLE      COMMENT 'Discount amount in USD (nullable).',

    -- Logistics
    shipping_country    STRING      COMMENT 'ISO 3166-1 alpha-2 destination.',
    payment_method      STRING      COMMENT 'card | mpesa | paypal | bank_transfer | crypto',

    -- Fraud
    fraud_flag          BOOLEAN     COMMENT 'Post-auth fraud flag from scoring service.',
    fraud_score         DOUBLE      COMMENT 'Flink real-time fraud score at order time.',

    -- Session linkage
    first_cart_at       TIMESTAMP   COMMENT 'When first item was carted (links to silver.user_sessions).',

    -- Timestamps from source
    created_at          TIMESTAMP   COMMENT 'Order creation in source DB.',
    updated_at          TIMESTAMP   COMMENT 'Last update in source DB.',

    -- SCD Type 2 fields
    effective_from      TIMESTAMP   COMMENT 'When this version of the record became active.',
    effective_to        TIMESTAMP   COMMENT 'When this version was superseded. NULL = current.',
    is_current          BOOLEAN     COMMENT 'True for the most recent version of the order.',
    record_version      INT         COMMENT 'Monotonically increasing version number per order_id.',

    -- CDC lineage
    cdc_lsn             BIGINT      COMMENT 'Source PostgreSQL LSN — for dedup and ordering.',
    processed_at        TIMESTAMP   COMMENT 'Silver processing timestamp.',

    -- Partition
    updated_date        DATE        COMMENT 'Partition column: date of updated_at.'
)
USING iceberg
PARTITIONED BY (updated_date)
TBLPROPERTIES (
    'table_type'                            = 'ICEBERG',
    'format-version'                        = '2',
    'write.target-file-size-bytes'          = '134217728',
    'write.parquet.compression-codec'      = 'snappy',
    'write.merge.mode'                      = 'merge-on-read',
    'history.expire.max-snapshot-age-ms'   = '1209600000',
    'comment'                               = 'Silver orders — SCD Type 2. Full order lifecycle history from CDC. PII-safe (user_id hashed).'
);

-- 4. silver.product_catalog
--    Source: bronze.product_catalog_raw → Glue silver_product_catalog job
--    Pattern: Full refresh upsert on sku — always reflects current catalog state
CREATE TABLE IF NOT EXISTS glue_catalog.silver.product_catalog (

    sku                 STRING      COMMENT 'SKU — natural and surrogate key for this table.',
    name                STRING,
    category            STRING,
    subcategory         STRING,
    brand               STRING,
    price_usd           DOUBLE,
    cost_usd            DOUBLE,
    margin_pct          DOUBLE,
    stock_quantity      INT,
    is_active           BOOLEAN,
    weight_kg           DOUBLE,
    tags                ARRAY<STRING> COMMENT 'Parsed from raw JSON array in Bronze.',
    image_url           STRING,
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP,
    processed_at        TIMESTAMP   COMMENT 'Silver processing timestamp.',
    content_hash        STRING      COMMENT 'SHA-256 fingerprint for change detection.'
)
USING iceberg
PARTITIONED BY (category)
TBLPROPERTIES (
    'table_type'                    = 'ICEBERG',
    'format-version'                = '2',
    'write.target-file-size-bytes'  = '33554432',
    'write.parquet.compression-codec' = 'snappy',
    'write.upsert.enabled'          = 'true',
    'history.expire.max-snapshot-age-ms' = '604800000',
    'comment'                       = 'Silver product catalog — current state upserted every 15 min. Source of truth for Flink enrichment lookups.'
);
