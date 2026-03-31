-- =============================================================================
-- staging/stg_orders.sql
-- =============================================================================
-- Thin view over silver.orders_unified — current records only.
-- Applies incremental lookback window and renames for downstream consistency.
--
-- Note: Only is_current = true rows are surfaced here.
-- SCD2 history is available directly from silver.orders_unified for
-- audit/compliance queries but not needed in Gold fact tables.
-- =============================================================================

with source as (

    select *
    from {{ source('silver', 'orders_unified') }}
    where is_current = true

    {% if is_incremental() %}
    and processed_at >= current_timestamp() - interval {{ var('incremental_lookback_hours') }} hours
    {% endif %}

),

renamed as (

    select
        -- Keys
        order_id,
        user_id_hashed,

        -- Status
        status,
        prev_status,
        status_changed_at,

        -- Financials
        currency,
        total_amount,
        total_amount_usd,
        coalesce(discount_usd, 0.0)             as discount_usd,
        total_amount_usd - coalesce(discount_usd, 0.0)
                                                as net_amount_usd,
        item_count,
        promo_code,

        -- Applied
        shipping_country,
        payment_method,

        -- Fraud
        fraud_flag,
        coalesce(fraud_score, 0.0)              as fraud_score,

        -- Session linkage
        first_cart_at,

        -- Timestamps
        created_at,
        updated_at,
        cast(created_at as date)                as order_date,
        processed_at,

        -- Derived tiers
        case
            when total_amount_usd >= 200 then 'high_value'
            when total_amount_usd >= 50  then 'mid_value'
            else 'low_value'
        end                                     as order_tier,

        -- Consideration window (days between first cart add and purchase)
        datediff(
            cast(created_at as date),
            cast(first_cart_at as date)
        )                                       as days_consideration

    from source

    -- Only include terminal + active statuses (exclude internal system states)
    where status in ('pending','confirmed','shipped','delivered','cancelled','refunded')

)

select * from renamed
