-- =============================================================================
-- tests/assert_fct_orders_no_negative_revenue.sql
-- =============================================================================
-- Data test: no order should have a negative total_amount_usd or net_amount_usd.
-- Negative values indicate a data transformation error (e.g. discount > price).
-- Returns rows that FAIL the assertion (dbt convention: failing = non-empty result).
-- =============================================================================

select
    order_id,
    total_amount_usd,
    net_amount_usd,
    discount_usd,
    status
from {{ ref('fct_orders') }}
where total_amount_usd < 0
   or net_amount_usd   < 0
