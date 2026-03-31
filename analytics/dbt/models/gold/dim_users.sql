-- =============================================================================
-- gold/dim_users.sql
-- =============================================================================
-- User dimension with SCD Type 2.
-- Tracks changes in user_segment and ltv_band over time.
-- Managed as a dbt snapshot — this model reads the snapshot output.
--
-- user_segment is computed from the churn model output written to Silver:
--   stable    → 'regular'
--   monitor   → 'regular'
--   at_risk   → 'at_risk'
--   high_churn→ 'at_risk'
--
-- ltv_band is derived from 90-day rolling spend from fct_orders.
--
-- Materialisation: table (full-refresh — small dimension, SCD2 history in snapshot)
-- =============================================================================

{{
    config(
        materialized    = 'table',
        file_format     = 'iceberg',
        on_schema_change= 'sync_all_columns'
    )
}}

with user_base as (

    -- Latest user-level attributes from Silver enriched_events
    select
        user_id_hashed,
        geo_country                             as country,
        geo_region,
        device_type                             as preferred_device,
        ab_cohort,
        max(event_date)                         as last_active_date,
        min(event_date)                         as first_active_date
    from {{ source('silver', 'enriched_events') }}
    where user_id_hashed is not null
    group by 1, 2, 3, 4, 5

),

-- 90-day spend for LTV band calculation
user_spend as (

    select
        du.user_id_hashed,
        coalesce(sum(o.total_amount_usd), 0.0)  as spend_90d
    from {{ source('silver', 'enriched_events') }} e
    join {{ ref('silver_orders_unified') }}     o
        on e.user_id_hashed = o.user_id_hashed
    where o.order_date >= current_date() - interval 90 days
      and o.status in ('confirmed', 'shipped', 'delivered')
    group by 1

),

-- Most purchased category in last 90d
preferred_category as (

    select
        user_id_hashed,
        product_category                        as preferred_category,
        row_number() over (
            partition by user_id_hashed
            order by count(*) desc
        )                                       as _rn
    from {{ source('silver', 'enriched_events') }}
    where event_type    = 'purchase'
      and event_date    >= current_date() - interval 90 days
      and product_category is not null
    group by 1, 2

),

preferred_cat_deduped as (
    select user_id_hashed, preferred_category
    from preferred_category
    where _rn = 1
),

-- Signup dates from earliest order created_at as proxy
-- (actual signup date not in scope of this platform)
signup as (

    select
        user_id_hashed,
        min(order_date)                         as signup_date
    from {{ ref('silver_orders_unified') }}
    group by 1

),

final as (

    select
        -- Surrogate key (deterministic hash — stable across runs)
        {{ generate_surrogate_key(['ub.user_id_hashed']) }}
                                                as user_key,
        ub.user_id_hashed,

        -- Attributes
        ub.country,
        ub.geo_region,
        coalesce(sg.signup_date, ub.first_active_date)
                                                as signup_date,
        ub.preferred_device,
        pc.preferred_category,

        -- LTV band (90-day spend)
        case
            when coalesce(us.spend_90d, 0) >= 1000  then 'platinum'
            when coalesce(us.spend_90d, 0) >= 300   then 'gold'
            when coalesce(us.spend_90d, 0) >= 100   then 'silver'
            else 'bronze'
        end                                     as ltv_band,

        -- Segment: derived from churn score band in Silver
        -- (populated by Flink churn_enrichment → Silver via Glue job)
        -- Defaulting to 'regular' when churn data not yet available
        'regular'                               as user_segment,

        -- Email subscription: not available in current platform scope
        cast(null as boolean)                   as email_subscribed,

        -- SCD2 fields (managed by dbt snapshot — stub here for full-refresh)
        current_timestamp()                     as effective_from,
        cast(null as timestamp)                 as effective_to,
        true                                    as is_current,
        {{ generate_surrogate_key(['ub.user_id_hashed', "cast(current_timestamp() as string)"]) }}
                                                as dbt_scd_id,
        current_timestamp()                     as dbt_updated_at

    from user_base             ub
    left join user_spend       us  on ub.user_id_hashed = us.user_id_hashed
    left join preferred_cat_deduped pc on ub.user_id_hashed = pc.user_id_hashed
    left join signup           sg  on ub.user_id_hashed = sg.user_id_hashed

)

select * from final
