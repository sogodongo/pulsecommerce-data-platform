CREATE DATABASE IF NOT EXISTS glue_catalog.gold
COMMENT 'Gold zone — Kimball star schema, hourly SLA. Analytics and ML consumption layer.';

-- 1. dim_date
--    Static dimension, pre-populated for 10 years (2020–2030).
--    Generated once by bootstrap.py — never updated by dbt.
CREATE TABLE IF NOT EXISTS glue_catalog.gold.dim_date (

    date_key        INT         COMMENT 'Surrogate key: YYYYMMDD integer (e.g. 20251114).',
    full_date       DATE        COMMENT 'Calendar date.',
    year            INT,
    quarter         INT         COMMENT '1–4',
    month           INT         COMMENT '1–12',
    month_name      STRING      COMMENT 'January … December',
    week_of_year    INT         COMMENT 'ISO week number 1–53.',
    day_of_month    INT         COMMENT '1–31',
    day_of_week     INT         COMMENT '1=Monday … 7=Sunday (ISO)',
    day_name        STRING      COMMENT 'Monday … Sunday',
    is_weekend      BOOLEAN,
    is_month_start  BOOLEAN,
    is_month_end    BOOLEAN,
    is_quarter_end  BOOLEAN,
    fiscal_year     INT         COMMENT 'PulseCommerce fiscal year (starts Feb 1).',
    fiscal_quarter  INT         COMMENT 'Fiscal quarter 1–4.'
)
USING iceberg
TBLPROPERTIES (
    'table_type'    = 'ICEBERG',
    'format-version'= '2',
    'write.parquet.compression-codec' = 'snappy',
    'comment'       = 'Date dimension — 2020-01-01 to 2030-12-31. Static; updated only for fiscal calendar changes.'
);

-- 2. dim_users
--    SCD Type 2 — tracks user segment changes over time.
--    Managed by dbt snapshot (dbt_scd_id as surrogate).
CREATE TABLE IF NOT EXISTS glue_catalog.gold.dim_users (

    user_key            BIGINT      COMMENT 'Surrogate key (dbt-generated hash).',
    user_id_hashed      STRING      COMMENT 'HMAC-SHA256 natural key — links to Silver and fact tables.',

    -- Attributes
    country             STRING      COMMENT 'ISO 3166-1 alpha-2 signup country.',
    signup_date         DATE        COMMENT 'Account registration date.',
    user_segment        STRING      COMMENT 'occasional | regular | power | at_risk — computed by ML churn model.',
    ltv_band            STRING      COMMENT 'bronze | silver | gold | platinum — based on 90-day spend.',
    email_subscribed    BOOLEAN     COMMENT 'Marketing email opt-in status.',
    preferred_device    STRING      COMMENT 'Most frequent device_type in last 30 days.',
    preferred_category  STRING      COMMENT 'Top spend category in last 90 days.',
    geo_region          STRING      COMMENT 'EMEA | APAC | AMER | LATAM',

    -- SCD Type 2 fields
    effective_from      TIMESTAMP   COMMENT 'When this version became active.',
    effective_to        TIMESTAMP   COMMENT 'When superseded. NULL = current record.',
    is_current          BOOLEAN     COMMENT 'True for the most recent version.',
    dbt_scd_id          STRING      COMMENT 'dbt snapshot surrogate — SHA hash of natural + effective_from.',
    dbt_updated_at      TIMESTAMP   COMMENT 'dbt snapshot updated_at.'
)
USING iceberg
TBLPROPERTIES (
    'table_type'                    = 'ICEBERG',
    'format-version'                = '2',
    'write.target-file-size-bytes'  = '67108864',
    'write.parquet.compression-codec' = 'snappy',
    'write.merge.mode'              = 'merge-on-read',
    'history.expire.max-snapshot-age-ms' = '2592000000',  -- 30 days (need history for SCD lookback)
    'comment'                       = 'dim_users — SCD Type 2. Tracks user segment and LTV band changes over time.'
);

-- 3. dim_products
--    Full refresh hourly — reflects current product catalog state.
CREATE TABLE IF NOT EXISTS glue_catalog.gold.dim_products (

    product_key     BIGINT      COMMENT 'Surrogate key (hash of sku).',
    sku             STRING      COMMENT 'Natural key — links to Silver enriched_events.',
    name            STRING,
    category        STRING,
    subcategory     STRING,
    brand           STRING,
    price_usd       DOUBLE      COMMENT 'Current listed price in USD.',
    cost_usd        DOUBLE,
    margin_pct      DOUBLE      COMMENT 'Current gross margin %.',
    is_active       BOOLEAN,
    tags            STRING      COMMENT 'Pipe-delimited tag string for Redshift compatibility.',
    weight_kg       DOUBLE,
    price_band      STRING      COMMENT 'budget (<$25) | mid ($25–$100) | premium ($100–$300) | luxury (>$300)',
    updated_at      TIMESTAMP   COMMENT 'Source system last-modified timestamp.',
    dbt_updated_at  TIMESTAMP   COMMENT 'dbt run timestamp.'
)
USING iceberg
TBLPROPERTIES (
    'table_type'                    = 'ICEBERG',
    'format-version'                = '2',
    'write.target-file-size-bytes'  = '33554432',
    'write.parquet.compression-codec' = 'snappy',
    'comment'                       = 'dim_products — full refresh every hour from silver.product_catalog.'
);

-- 4. dim_channels
--    Marketing channel / UTM parameter dimension.
--    Derived from ad attribution + session referrer data.
CREATE TABLE IF NOT EXISTS glue_catalog.gold.dim_channels (

    channel_key     BIGINT      COMMENT 'Surrogate key.',
    source          STRING      COMMENT 'UTM source: google | facebook | instagram | email | organic | direct | referral',
    medium          STRING      COMMENT 'UTM medium: cpc | cpm | email | social | organic | referral',
    campaign        STRING      COMMENT 'UTM campaign name (nullable).',
    ad_set          STRING      COMMENT 'Ad set / ad group (nullable).',
    ab_cohort       STRING      COMMENT 'A/B test cohort (nullable).',
    channel_group   STRING      COMMENT 'Collapsed channel: Paid Search | Paid Social | Email | Organic | Direct | Other',
    dbt_updated_at  TIMESTAMP
)
USING iceberg
TBLPROPERTIES (
    'table_type'    = 'ICEBERG',
    'format-version'= '2',
    'write.parquet.compression-codec' = 'snappy',
    'comment'       = 'dim_channels — marketing attribution channel dimension.'
);

-- 5. dim_geography
--    Country-level geography dimension with region mapping.
CREATE TABLE IF NOT EXISTS glue_catalog.gold.dim_geography (

    geo_key         BIGINT      COMMENT 'Surrogate key (hash of country).',
    country         STRING      COMMENT 'ISO 3166-1 alpha-2.',
    country_name    STRING      COMMENT 'Full country name.',
    region          STRING      COMMENT 'EMEA | APAC | AMER | LATAM',
    subregion       STRING      COMMENT 'e.g. East Africa, Western Europe, Southeast Asia',
    is_gdpr_scope   BOOLEAN     COMMENT 'True for EU + UK + EEA countries.',
    currency_code   STRING      COMMENT 'Primary local currency ISO 4217.',
    dbt_updated_at  TIMESTAMP
)
USING iceberg
TBLPROPERTIES (
    'table_type'    = 'ICEBERG',
    'format-version'= '2',
    'write.parquet.compression-codec' = 'snappy',
    'comment'       = 'dim_geography — country-level geographic dimension with GDPR scope flag.'
);

-- 6. fct_orders
--    Grain: one row per order (current state only).
--    Partitioned by order_date for date-range query pruning in BI tools.
CREATE TABLE IF NOT EXISTS glue_catalog.gold.fct_orders (

    -- Keys
    order_id                STRING      COMMENT 'Natural key from Silver orders_unified.',
    user_key                BIGINT      COMMENT 'FK → dim_users.user_key (current record).',
    product_key             BIGINT      COMMENT 'FK → dim_products.product_key (primary SKU).',
    channel_key             BIGINT      COMMENT 'FK → dim_channels.channel_key.',
    geo_key                 BIGINT      COMMENT 'FK → dim_geography.geo_key.',
    date_key                INT         COMMENT 'FK → dim_date.date_key (order_date as YYYYMMDD).',

    -- Measures
    order_date              DATE        COMMENT 'Date the order was placed (partition column).',
    total_amount_usd        DOUBLE      COMMENT 'Order total in USD.',
    item_count              INT         COMMENT 'Number of distinct SKUs.',
    discount_usd            DOUBLE      COMMENT 'Discount applied in USD.',
    net_amount_usd          DOUBLE      COMMENT 'total_amount_usd - discount_usd.',

    -- Derived metrics
    order_tier              STRING      COMMENT 'low_value (<$50) | mid_value ($50–$200) | high_value (>=$200)',
    days_consideration      INT         COMMENT 'Days between first_cart_at and order placement.',
    funnel_stage_reached    STRING      COMMENT 'Deepest funnel stage in the attributed session.',
    attributed_session_id   STRING      COMMENT 'FK → silver.user_sessions.session_id (best-match).',

    -- Fraud
    fraud_score             DOUBLE      COMMENT 'Flink real-time fraud score at order time [0.0–1.0].',
    fraud_flagged           BOOLEAN     COMMENT 'True if fraud_score > 0.7.',
    fraud_signals           STRING      COMMENT 'JSON array of triggered signals.',

    -- Order lifecycle
    status                  STRING      COMMENT 'Current order status.',
    payment_method          STRING,
    shipping_country        STRING,

    -- Timestamps
    created_at              TIMESTAMP   COMMENT 'Order placement timestamp.',
    updated_at              TIMESTAMP   COMMENT 'Last status update timestamp.',
    dbt_updated_at          TIMESTAMP   COMMENT 'dbt run timestamp.'
)
USING iceberg
PARTITIONED BY (order_date)
TBLPROPERTIES (
    'table_type'                            = 'ICEBERG',
    'format-version'                        = '2',
    'write.target-file-size-bytes'          = '134217728',
    'write.parquet.compression-codec'      = 'snappy',
    'write.merge.mode'                      = 'merge-on-read',
    'read.split.target-size'               = '134217728',
    'history.expire.max-snapshot-age-ms'   = '2592000000',  -- 30 days
    'history.expire.min-snapshots-to-keep' = '24',          -- keep 24 hourly snapshots
    'comment'                               = 'fct_orders — grain: one row per order (current state). Kimball star schema core fact.'
);

-- 7. fct_sessions
--    Grain: one row per user session.
--    Partitioned by session_date.
CREATE TABLE IF NOT EXISTS glue_catalog.gold.fct_sessions (

    -- Keys
    session_id              STRING      COMMENT 'Natural key from silver.user_sessions.',
    user_key                BIGINT      COMMENT 'FK → dim_users.user_key.',
    geo_key                 BIGINT      COMMENT 'FK → dim_geography.geo_key.',
    channel_key             BIGINT      COMMENT 'FK → dim_channels.channel_key (entry referrer).',
    date_key                INT         COMMENT 'FK → dim_date.date_key.',

    -- Session measures
    session_date            DATE        COMMENT 'Partition column: date of session start.',
    session_start_ts        TIMESTAMP,
    session_end_ts          TIMESTAMP,
    session_duration_s      INT,

    -- Engagement metrics
    page_views              INT,
    product_views           INT,
    cart_adds               INT,
    cart_removes            INT,
    searches                INT,
    checkout_attempts       INT,
    purchases               INT,

    -- Revenue
    revenue_attributed_usd  DOUBLE      COMMENT 'Sum of purchase amounts within session.',
    cart_value_usd          DOUBLE      COMMENT 'Cart value at session end.',
    cart_abandonment        BOOLEAN,

    -- Funnel
    funnel_exit_stage       STRING      COMMENT 'Deepest stage reached: browse | product_view | add_to_cart | checkout_start | purchase',

    -- Device & geo
    device_type             STRING,
    device_os               STRING,

    -- Fraud
    max_fraud_score         DOUBLE,
    fraud_flagged           BOOLEAN,

    -- A/B
    ab_cohort               STRING,

    dbt_updated_at          TIMESTAMP
)
USING iceberg
PARTITIONED BY (session_date)
TBLPROPERTIES (
    'table_type'                            = 'ICEBERG',
    'format-version'                        = '2',
    'write.target-file-size-bytes'          = '134217728',
    'write.parquet.compression-codec'      = 'snappy',
    'write.merge.mode'                      = 'merge-on-read',
    'history.expire.max-snapshot-age-ms'   = '2592000000',
    'comment'                               = 'fct_sessions — grain: one row per user session. Session-level funnel and revenue attribution.'
);

-- 8. agg_daily_metrics
--    Pre-aggregated daily KPI table for fast dashboard queries.
--    Materialized in Redshift as a native MV for sub-second latency.
CREATE TABLE IF NOT EXISTS glue_catalog.gold.agg_daily_metrics (

    metric_date             DATE        COMMENT 'Partition column: business date.',

    -- Order KPIs
    total_orders            BIGINT,
    unique_buyers           BIGINT,
    gross_revenue_usd       DOUBLE,
    net_revenue_usd         DOUBLE      COMMENT 'After discounts.',
    avg_order_value_usd     DOUBLE,
    median_order_value_usd  DOUBLE,

    -- Fraud KPIs
    flagged_fraud_orders    BIGINT,
    fraud_exposure_usd      DOUBLE,
    fraud_rate_pct          DOUBLE      COMMENT 'flagged_fraud_orders / total_orders * 100',

    -- Session KPIs
    total_sessions          BIGINT,
    unique_visitors         BIGINT,
    avg_session_duration_s  DOUBLE,
    avg_page_views          DOUBLE,
    cart_abandonment_rate   DOUBLE      COMMENT 'Sessions with cart_abandonment=true / sessions with cart_adds',

    -- Funnel KPIs
    product_view_sessions   BIGINT,
    add_to_cart_sessions    BIGINT,
    checkout_start_sessions BIGINT,
    purchase_sessions       BIGINT,
    overall_conversion_rate DOUBLE      COMMENT 'purchase_sessions / total_sessions',

    -- Acquisition
    new_buyers              BIGINT      COMMENT 'Users placing their first order on this date.',
    returning_buyers        BIGINT,

    -- Processing metadata
    dbt_updated_at          TIMESTAMP   COMMENT 'dbt run timestamp for freshness monitoring.'
)
USING iceberg
PARTITIONED BY (metric_date)
TBLPROPERTIES (
    'table_type'                    = 'ICEBERG',
    'format-version'                = '2',
    'write.target-file-size-bytes'  = '33554432',
    'write.parquet.compression-codec' = 'snappy',
    'history.expire.max-snapshot-age-ms' = '2592000000',
    'comment'                       = 'agg_daily_metrics — pre-aggregated daily KPIs. Primary source for executive dashboards.'
);

-- Redshift Serverless — External Iceberg Tables via Spectrum
-- Run these in Redshift Serverless after Gold tables are created in Glue.
/*
-- Create external schema pointing to Glue Data Catalog (Gold zone)
CREATE EXTERNAL SCHEMA IF NOT EXISTS spectrum_gold
FROM DATA CATALOG
DATABASE 'gold'
IAM_ROLE 'arn:aws:iam::${AWS_ACCOUNT_ID}:role/pulsecommerce-redshift-role'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

-- Materialized view for hot dashboard: Daily Revenue by Country
-- Refreshes automatically when new Iceberg snapshots are committed.
CREATE MATERIALIZED VIEW mv_daily_revenue_by_country
AUTO REFRESH YES
AS
SELECT
    o.order_date,
    g.country,
    g.country_name,
    g.region,
    COUNT(DISTINCT o.order_id)      AS orders,
    COUNT(DISTINCT o.user_key)      AS unique_buyers,
    SUM(o.total_amount_usd)         AS revenue_usd,
    SUM(o.net_amount_usd)           AS net_revenue_usd,
    AVG(o.total_amount_usd)         AS aov_usd,
    SUM(CASE WHEN o.fraud_flagged THEN o.total_amount_usd ELSE 0 END) AS fraud_exposure_usd
FROM spectrum_gold.fct_orders o
JOIN spectrum_gold.dim_geography g USING (geo_key)
GROUP BY 1, 2, 3, 4;

-- Materialized view for funnel drop-off (Superset dashboard)
CREATE MATERIALIZED VIEW mv_weekly_funnel
AUTO REFRESH YES
AS
SELECT
    DATE_TRUNC('week', session_date)        AS week_start,
    funnel_exit_stage,
    COUNT(DISTINCT session_id)              AS sessions,
    SUM(revenue_attributed_usd)             AS revenue_usd
FROM spectrum_gold.fct_sessions
GROUP BY 1, 2;
*/
