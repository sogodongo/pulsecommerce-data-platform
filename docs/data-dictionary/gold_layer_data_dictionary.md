# Gold Layer Data Dictionary

**Catalog:** `glue_catalog.gold`
**Engine access:** Amazon Redshift Serverless, Amazon Athena, dbt-glue
**Refresh cadence:** Triggered by `gold_models_dag.py` after Silver refresh completes
**Last updated:** 2024-06-01

---

## Table of Contents

1. [dim_users](#dim_users)
2. [dim_products](#dim_products)
3. [dim_channels](#dim_channels)
4. [dim_geography](#dim_geography)
5. [dim_date](#dim_date)
6. [fct_orders](#fct_orders)
7. [fct_sessions](#fct_sessions)
8. [agg_daily_metrics](#agg_daily_metrics)

---

## dim_users

**Description:** SCD Type 2 user dimension. One row per user per attribute version.
Current record is identified by `is_current = true`.

**Source:** `silver.enriched_events` (latest profile signal per user), `silver.orders_unified`
**dbt model:** `analytics/dbt/models/gold/dim_users.sql`

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `user_id_hashed` | VARCHAR | NOT NULL | HMAC-SHA256 pseudonymised user identifier. Used as join key across Silver and Gold. Never contains raw PII. |
| `first_seen_at` | TIMESTAMP | NOT NULL | Timestamp of the user's first recorded event in Bronze. |
| `last_seen_at` | TIMESTAMP | NOT NULL | Timestamp of the user's most recent event in Silver. |
| `device_type` | VARCHAR | NULL | Most recently observed device type: `desktop`, `mobile`, `tablet`. |
| `device_os` | VARCHAR | NULL | Most recently observed operating system. |
| `geo_country` | VARCHAR(2) | NULL | ISO 3166-1 alpha-2 country code from most recent event. |
| `geo_region` | VARCHAR | NULL | Derived geographic region: `EMEA`, `APAC`, `AMER`, `LATAM`, `OTHER`, `UNKNOWN`. |
| `geo_city` | VARCHAR | NULL | City name, or `MASKED` for GDPR-scope countries (EU/UK/EEA). |
| `preferred_category` | VARCHAR | NULL | Product category with the highest purchase frequency for this user. NULL if no purchases. |
| `ab_cohort` | VARCHAR | NULL | A/B test cohort assignment (`A`, `B`, or NULL). |
| `total_orders` | BIGINT | NOT NULL | Lifetime count of completed orders. |
| `total_revenue_usd` | DECIMAL(18,2) | NOT NULL | Lifetime net revenue in USD. |
| `avg_order_value_usd` | DECIMAL(10,2) | NULL | Average order value across all orders. NULL if no orders. |
| `days_since_last_order` | INTEGER | NULL | Calendar days between today and the most recent order. NULL if no orders. |
| `churn_risk_score` | DOUBLE | NULL | ML model output (0–1). Updated by Flink `churn_enrichment.py` via SageMaker Feature Store. NULL if not yet scored. |
| `ltv_segment` | VARCHAR | NULL | Lifetime value segment: `HIGH` (revenue > $1,000), `MEDIUM` ($200–$1,000), `LOW` (< $200), `NEW` (no orders). |
| `effective_from` | TIMESTAMP | NOT NULL | SCD2: start of this attribute version's validity. |
| `effective_to` | TIMESTAMP | NULL | SCD2: end of this attribute version's validity. NULL if `is_current = true`. |
| `is_current` | BOOLEAN | NOT NULL | SCD2: `true` for the currently active record per user. |
| `record_version` | INTEGER | NOT NULL | SCD2: monotonically increasing version number per user, starting at 1. |
| `dbt_updated_at` | TIMESTAMP | NOT NULL | Timestamp of the dbt run that created or last updated this record. |

**Partitioned by:** `geo_country`
**Primary key (logical):** `(user_id_hashed, record_version)`
**Unique constraint (current):** `user_id_hashed WHERE is_current = true`

---

## dim_products

**Description:** Product dimension with current attributes. Not SCD2 — products are
snapshotted from `silver.product_catalog` on each Gold refresh.

**Source:** `silver.product_catalog`
**dbt model:** `analytics/dbt/models/gold/dim_products.sql`

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `sku` | VARCHAR | NOT NULL | Stock Keeping Unit — unique product identifier. |
| `product_name` | VARCHAR | NULL | Human-readable product name from catalog. |
| `category` | VARCHAR | NULL | Top-level product category (e.g., `Electronics`, `Apparel`). |
| `subcategory` | VARCHAR | NULL | Second-level category (e.g., `Laptops`, `Running Shoes`). |
| `brand` | VARCHAR | NULL | Brand or manufacturer name. |
| `price_usd` | DECIMAL(10,2) | NULL | Current list price in USD. |
| `cost_usd` | DECIMAL(10,2) | NULL | Unit cost in USD (used for margin calculation). |
| `margin_pct` | DOUBLE | NULL | Gross margin percentage: `(price - cost) / price`. |
| `is_active` | BOOLEAN | NOT NULL | `true` if the product is currently available in the catalog. |
| `catalog_source` | VARCHAR | NULL | Origin of the catalog record: `shopify`, `erp`, `manual`. |
| `ingested_at` | TIMESTAMP | NOT NULL | Timestamp the product record arrived in Silver. |
| `dbt_updated_at` | TIMESTAMP | NOT NULL | Timestamp of the dbt run that created or last updated this record. |

**Primary key (logical):** `sku`

---

## dim_channels

**Description:** Marketing channel dimension derived from UTM parameters and referrer
analysis on clickstream events.

**Source:** `silver.enriched_events` (referrer / UTM parsing)
**dbt model:** `analytics/dbt/models/gold/dim_channels.sql`

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `channel_id` | VARCHAR | NOT NULL | Surrogate key: MD5 hash of `(channel_name, channel_source, channel_medium)`. |
| `channel_name` | VARCHAR | NOT NULL | Canonical channel name: `organic_search`, `paid_search`, `email`, `social`, `direct`, `referral`, `affiliate`, `unknown`. |
| `channel_source` | VARCHAR | NULL | UTM source parameter (e.g., `google`, `facebook`, `newsletter`). |
| `channel_medium` | VARCHAR | NULL | UTM medium parameter (e.g., `cpc`, `email`, `organic`). |
| `channel_campaign` | VARCHAR | NULL | UTM campaign parameter. NULL for non-campaign traffic. |
| `is_paid` | BOOLEAN | NOT NULL | `true` for channels with direct advertising spend (paid_search, social ads, affiliate). |
| `dbt_updated_at` | TIMESTAMP | NOT NULL | Timestamp of the dbt run that created or last updated this record. |

**Primary key (logical):** `channel_id`

---

## dim_geography

**Description:** Country-level geographic dimension with region classification and
GDPR jurisdiction flag.

**Source:** Static seed + `silver.enriched_events` (discovered countries)
**dbt model:** `analytics/dbt/models/gold/dim_geography.sql`

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `country_code` | VARCHAR(2) | NOT NULL | ISO 3166-1 alpha-2 country code. |
| `country_name` | VARCHAR | NULL | Full English country name. |
| `geo_region` | VARCHAR | NOT NULL | Macro-region: `EMEA`, `APAC`, `AMER`, `LATAM`, `OTHER`. |
| `is_gdpr_jurisdiction` | BOOLEAN | NOT NULL | `true` for EU member states + UK + EEA (Iceland, Liechtenstein, Norway). Drives city masking in Silver PII processing. |
| `dbt_updated_at` | TIMESTAMP | NOT NULL | Timestamp of the dbt run that created or last updated this record. |

**Primary key (logical):** `country_code`
**Row count:** ~250 (one per recognised country/territory)

---

## dim_date

**Description:** Calendar dimension covering 2020-01-01 through 2030-12-31. Pre-computed
by dbt — no source table required.

**dbt model:** `analytics/dbt/models/gold/dim_date.sql`

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `date_key` | INTEGER | NOT NULL | Compact date key in `YYYYMMDD` format (e.g., `20240615`). |
| `date_actual` | DATE | NOT NULL | Calendar date. |
| `day_of_week` | INTEGER | NOT NULL | Day number: 1 (Monday) through 7 (Sunday). ISO 8601. |
| `day_name` | VARCHAR | NOT NULL | Full day name: `Monday` through `Sunday`. |
| `week_of_year` | INTEGER | NOT NULL | ISO week number (1–53). |
| `month_actual` | INTEGER | NOT NULL | Month number (1–12). |
| `month_name` | VARCHAR | NOT NULL | Full month name: `January` through `December`. |
| `quarter_actual` | INTEGER | NOT NULL | Quarter (1–4). |
| `year_actual` | INTEGER | NOT NULL | 4-digit year. |
| `is_weekend` | BOOLEAN | NOT NULL | `true` for Saturday and Sunday. |
| `is_uk_bank_holiday` | BOOLEAN | NOT NULL | `true` for UK public holidays (static list). |
| `is_us_federal_holiday` | BOOLEAN | NOT NULL | `true` for US federal holidays (static list). |

**Primary key (logical):** `date_key`
**Row count:** ~3,653 (10 years)

---

## fct_orders

**Description:** Order-grain fact table. One row per order, representing the terminal
state of each order from the Silver SCD2 orders table.

**Source:** `silver.orders_unified` (`is_current = true` records)
**dbt model:** `analytics/dbt/models/gold/fct_orders.sql`

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `order_id` | VARCHAR | NOT NULL | Unique order identifier from the source OMS. |
| `user_id_hashed` | VARCHAR | NOT NULL | FK → `dim_users.user_id_hashed`. |
| `order_date_key` | INTEGER | NOT NULL | FK → `dim_date.date_key`. Derived from `order_timestamp`. |
| `channel_id` | VARCHAR | NULL | FK → `dim_channels.channel_id`. NULL if channel attribution not resolved. |
| `country_code` | VARCHAR(2) | NULL | FK → `dim_geography.country_code`. |
| `status` | VARCHAR | NOT NULL | Terminal order status: `delivered`, `cancelled`, `refunded`, `returned`. Excludes in-flight statuses. |
| `gross_amount_usd` | DECIMAL(18,2) | NOT NULL | Pre-discount, pre-tax order value in USD. |
| `discount_amount_usd` | DECIMAL(18,2) | NOT NULL | Total discount applied. 0 if no discount. |
| `tax_amount_usd` | DECIMAL(18,2) | NOT NULL | Tax collected. 0 for tax-exempt orders. |
| `net_amount_usd` | DECIMAL(18,2) | NOT NULL | `gross - discount + tax`. Revenue recognised amount. |
| `item_count` | INTEGER | NOT NULL | Number of distinct line items in the order. |
| `total_quantity` | INTEGER | NOT NULL | Total units ordered across all line items. |
| `payment_method` | VARCHAR | NULL | Payment instrument: `credit_card`, `paypal`, `apple_pay`, `google_pay`, `bank_transfer`, `crypto`. |
| `fraud_score` | DOUBLE | NULL | Fraud risk score at time of order (0–1). From Flink fraud_scorer. |
| `is_flagged_fraud` | BOOLEAN | NOT NULL | `true` if `fraud_score >= 0.85` at order time. |
| `is_first_order` | BOOLEAN | NOT NULL | `true` if this is the user's first completed order. |
| `order_timestamp` | TIMESTAMP | NOT NULL | Timestamp of order creation in the source OMS. |
| `fulfilled_timestamp` | TIMESTAMP | NULL | Timestamp of order fulfilment/delivery. NULL for non-delivered orders. |
| `cdc_op` | VARCHAR | NOT NULL | Last CDC operation that produced this record: `c` (create), `u` (update). |
| `dbt_updated_at` | TIMESTAMP | NOT NULL | Timestamp of the dbt run that created or last updated this record. |

**Partitioned by:** `order_date_key` (month granularity in Iceberg hidden partitioning)
**Primary key (logical):** `order_id`

---

## fct_sessions

**Description:** Session-grain fact table. One row per user session, produced by the
Flink `session_stitcher.py` application and persisted to Silver, then promoted to Gold.

**Source:** `silver.enriched_events` (session_id grain aggregation)
**dbt model:** `analytics/dbt/models/gold/fct_sessions.sql`

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `session_id` | VARCHAR | NOT NULL | Unique session identifier, assigned by the Flink session stitcher. |
| `user_id_hashed` | VARCHAR | NOT NULL | FK → `dim_users.user_id_hashed`. |
| `session_date_key` | INTEGER | NOT NULL | FK → `dim_date.date_key`. Derived from session start timestamp. |
| `channel_id` | VARCHAR | NULL | FK → `dim_channels.channel_id`. Attribution from first event in session. |
| `country_code` | VARCHAR(2) | NULL | FK → `dim_geography.country_code`. From first event in session. |
| `session_start_ts` | TIMESTAMP | NOT NULL | Timestamp of the first event in the session. |
| `session_end_ts` | TIMESTAMP | NOT NULL | Timestamp of the last event in the session. |
| `session_duration_s` | INTEGER | NOT NULL | Session duration in seconds: `session_end_ts - session_start_ts`. |
| `event_count` | INTEGER | NOT NULL | Total number of events in the session. |
| `page_view_count` | INTEGER | NOT NULL | Number of `page_view` events. |
| `product_view_count` | INTEGER | NOT NULL | Number of `product_view` events. |
| `add_to_cart_count` | INTEGER | NOT NULL | Number of `add_to_cart` events. |
| `checkout_count` | INTEGER | NOT NULL | Number of `checkout` events. |
| `purchase_count` | INTEGER | NOT NULL | Number of `purchase` events. |
| `funnel_stage` | VARCHAR | NOT NULL | Deepest funnel stage reached in the session: `page_view`, `product_view`, `add_to_cart`, `checkout`, `purchase`. |
| `session_revenue_usd` | DECIMAL(10,2) | NOT NULL | Sum of `product_price_usd * product_quantity` for `purchase` events only. 0 if no purchase. |
| `cart_value_usd` | DECIMAL(10,2) | NOT NULL | Sum of `product_price_usd * product_quantity` for `add_to_cart` events. Clamped to 0 (no negatives). |
| `entry_page_url` | VARCHAR | NULL | URL of the first page viewed in the session. |
| `exit_page_url` | VARCHAR | NULL | URL of the last page viewed in the session. |
| `device_type` | VARCHAR | NULL | Device type from first event in session. |
| `device_os` | VARCHAR | NULL | Operating system from first event in session. |
| `max_fraud_score` | DOUBLE | NULL | Maximum fraud score observed across all session events. NULL if no events were scored. |
| `is_bounced` | BOOLEAN | NOT NULL | `true` if `event_count = 1` (single-page session with no further interaction). |
| `is_converted` | BOOLEAN | NOT NULL | `true` if `purchase_count >= 1`. |
| `dbt_updated_at` | TIMESTAMP | NOT NULL | Timestamp of the dbt run that created or last updated this record. |

**Partitioned by:** `session_date_key` (month granularity)
**Primary key (logical):** `session_id`

---

## agg_daily_metrics

**Description:** Pre-aggregated daily business metrics rollup. One row per calendar day.
Used by the FastAPI `/v1/daily-metrics` endpoint and BI dashboards.

**Source:** `gold.fct_orders`, `gold.fct_sessions`, `gold.dim_users`
**dbt model:** `analytics/dbt/models/gold/agg_daily_metrics.sql`
**Refreshed by:** `gold_models_dag.py` + Redshift Serverless materialized view (`mv_daily_metrics`)

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `metric_date` | DATE | NOT NULL | Calendar date of the aggregated metrics. |
| `total_sessions` | BIGINT | NOT NULL | Total number of sessions started on this date. |
| `unique_users` | BIGINT | NOT NULL | Count of distinct `user_id_hashed` values across all sessions. |
| `new_users` | BIGINT | NOT NULL | Count of users whose `first_seen_at` is on this date. |
| `total_orders` | BIGINT | NOT NULL | Total orders with `order_date_key` matching this date. |
| `gross_revenue_usd` | DECIMAL(18,2) | NOT NULL | Sum of `gross_amount_usd` for all orders on this date. |
| `net_revenue_usd` | DECIMAL(18,2) | NOT NULL | Sum of `net_amount_usd` for all orders on this date. |
| `avg_order_value_usd` | DECIMAL(10,2) | NULL | `net_revenue_usd / total_orders`. NULL if no orders. |
| `conversion_rate` | DOUBLE | NULL | `total_orders / total_sessions`. Ranges 0–1. NULL if no sessions. |
| `bounce_rate` | DOUBLE | NULL | Proportion of sessions with `is_bounced = true`. Ranges 0–1. |
| `avg_session_duration_s` | DOUBLE | NULL | Average session duration in seconds. NULL if no sessions. |
| `fraud_flagged_orders` | BIGINT | NOT NULL | Count of orders with `is_flagged_fraud = true`. |
| `fraud_rate` | DOUBLE | NULL | `fraud_flagged_orders / total_orders`. NULL if no orders. |
| `cart_abandonment_rate` | DOUBLE | NULL | Sessions with `add_to_cart_count >= 1` but `purchase_count = 0`, as proportion of sessions with any cart activity. |
| `top_category` | VARCHAR | NULL | Product category with highest `net_revenue_usd` on this date. |
| `top_country` | VARCHAR(2) | NULL | Country code with highest `total_orders` on this date. |
| `dbt_updated_at` | TIMESTAMP | NOT NULL | Timestamp of the dbt run that created or last updated this record. |

**Primary key (logical):** `metric_date`
**Retention:** Redshift materialized view `mv_daily_metrics` caches last 90 days.

---

## Notes

### PII and Privacy

- `user_id_hashed` is an HMAC-SHA256 pseudonym seeded with a rotating salt stored in
  AWS Secrets Manager. It is not reversible without the salt.
- `geo_city` is `MASKED` for all users from GDPR-scope countries. Do not attempt to
  unmask this field for reporting — use `geo_country` and `geo_region` instead.
- Raw PII columns (`user_id`, `geo_lat`, `geo_lon`, `raw_payload`) are dropped in the
  Silver transform and never appear in Gold.

### Null Semantics

- `NULL` in a dimension FK column (e.g., `channel_id IS NULL`) indicates the dimension
  attribute was not resolvable, not that the dimension member does not exist.
- Metrics marked `NULL` in `agg_daily_metrics` (e.g., `conversion_rate`) occur only
  on days with zero sessions or zero orders — treat as 0 in visualisations.

### Currency

All monetary values are in **USD**. Currency conversion from non-USD transactions is
applied at the Silver layer using daily exchange rates from `silver.fx_rates`. If no
exchange rate is available for a given date/currency pair, the order's `net_amount_usd`
is NULL and that order is excluded from Gold revenue aggregates.
