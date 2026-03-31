{{
    config(
        materialized          = 'incremental',
        incremental_strategy  = 'merge',
        unique_key            = 'order_id',
        file_format           = 'iceberg',
        partition_by          = [{'field': 'order_date', 'data_type': 'date'}],
        on_schema_change      = 'sync_all_columns'
    )
}}

with orders as (

    select * from {{ ref('silver_orders_unified') }}

    {% if is_incremental() %}
    where updated_at >= current_timestamp() - interval {{ var('incremental_lookback_hours') }} hours
    {% endif %}

),

-- Dimension lookups — surrogate key joins
dim_users as (
    select user_key, user_id_hashed
    from {{ ref('dim_users') }}
    where is_current = true
),

dim_products as (
    select product_key, sku
    from {{ ref('dim_products') }}
    where is_active = true
),

dim_geography as (
    select geo_key, country
    from {{ ref('dim_geography') }}
),

dim_date as (
    select date_key, full_date
    from {{ ref('dim_date') }}
),

-- Primary product SKU per order: derived from Silver enriched_events
-- (pick the highest-value SKU in the order as the representative product)
primary_sku as (

    select
        order_id,
        product_sku,
        row_number() over (
            partition by order_id
            order by coalesce(product_price_usd, 0) desc
        ) as _rn
    from {{ source('silver', 'enriched_events') }}
    where event_type = 'purchase'
      and order_id   is not null
      and product_sku is not null

),

primary_sku_deduped as (
    select order_id, product_sku
    from primary_sku
    where _rn = 1
),

-- Channel attribution from the attributed session's entry referrer
session_channel as (

    select
        s.session_id,
        dc.channel_key
    from {{ ref('silver_user_sessions') }}  s
    left join {{ ref('dim_channels') }}     dc
        on lower(s.entry_referrer) like concat('%', lower(dc.source), '%')

),

final as (

    select
        -- Natural key
        o.order_id,

        -- Surrogate FK keys (null-safe: use -1 as unknown key)
        coalesce(du.user_key,   -1)             as user_key,
        coalesce(dp.product_key,-1)             as product_key,
        coalesce(sc.channel_key,-1)             as channel_key,
        coalesce(dg.geo_key,    -1)             as geo_key,
        coalesce(dd.date_key,   -1)             as date_key,

        -- Date
        o.order_date,

        -- Measures
        o.total_amount_usd,
        o.item_count,
        o.discount_usd,
        o.net_amount_usd,

        -- Derived
        o.order_tier,
        o.days_consideration,
        o.funnel_stage_reached,
        o.attributed_session_id,

        -- Fraud
        o.fraud_score,
        case when o.fraud_score > {{ var('fraud_score_threshold') }}
            then true else false
        end                                     as fraud_flagged,
        o.fraud_flag                            as fraud_confirmed,

        -- Order metadata
        o.status,
        o.payment_method,
        o.shipping_country,

        -- Timestamps
        o.created_at,
        o.updated_at,
        current_timestamp()                     as dbt_updated_at

    from orders           o
    left join dim_users   du on o.user_id_hashed        = du.user_id_hashed
    left join primary_sku_deduped ps on o.order_id      = ps.order_id
    left join dim_products dp on ps.product_sku         = dp.sku
    left join dim_geography dg on o.shipping_country    = dg.country
    left join dim_date     dd on o.order_date           = dd.full_date
    left join session_channel sc on o.attributed_session_id = sc.session_id

    -- Exclude orders with no revenue (cancelled before confirmation)
    where o.total_amount_usd > 0
       or o.status not in ('cancelled')

)

select * from final
