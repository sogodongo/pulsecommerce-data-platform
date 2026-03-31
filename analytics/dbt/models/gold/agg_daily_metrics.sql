{{
    config(
        materialized          = 'incremental',
        incremental_strategy  = 'merge',
        unique_key            = 'metric_date',
        file_format           = 'iceberg',
        partition_by          = [{'field': 'metric_date', 'data_type': 'date'}],
        on_schema_change      = 'sync_all_columns'
    )
}}

with orders as (

    select *
    from {{ ref('fct_orders') }}

    {% if is_incremental() %}
    where order_date >= current_date() - interval {{ var('incremental_lookback_hours') }} hours
    {% endif %}

),

sessions as (

    select *
    from {{ ref('fct_sessions') }}

    {% if is_incremental() %}
    where session_date >= current_date() - interval {{ var('incremental_lookback_hours') }} hours
    {% endif %}

),

-- First order per user (to identify new buyers)
first_order_dates as (

    select
        user_key,
        min(order_date)     as first_order_date
    from {{ ref('fct_orders') }}
    where status in ('confirmed','shipped','delivered')
    group by 1

),

-- Order-level aggregations
order_agg as (

    select
        o.order_date                                            as metric_date,

        count(distinct o.order_id)                             as total_orders,
        count(distinct o.user_key)                             as unique_buyers,

        sum(o.total_amount_usd)                                as gross_revenue_usd,
        sum(o.net_amount_usd)                                  as net_revenue_usd,
        avg(o.total_amount_usd)                                as avg_order_value_usd,
        percentile_approx(o.total_amount_usd, 0.5)            as median_order_value_usd,

        -- Fraud
        sum(case when o.fraud_flagged then 1 else 0 end)       as flagged_fraud_orders,
        sum(case when o.fraud_flagged
                 then o.total_amount_usd else 0 end)           as fraud_exposure_usd,

        -- New vs returning buyers
        count(distinct case
            when fo.first_order_date = o.order_date
            then o.user_key
        end)                                                   as new_buyers,
        count(distinct case
            when fo.first_order_date < o.order_date
            then o.user_key
        end)                                                   as returning_buyers

    from orders o
    left join first_order_dates fo using (user_key)
    where o.status in ('confirmed','shipped','delivered','pending')
    group by 1

),

-- Session-level aggregations
session_agg as (

    select
        session_date                                            as metric_date,

        count(distinct session_id)                             as total_sessions,
        count(distinct user_key)                               as unique_visitors,
        avg(session_duration_s)                                as avg_session_duration_s,
        avg(page_views)                                        as avg_page_views,

        -- Cart abandonment rate: sessions with cart_adds but no purchase
        sum(case when cart_abandonment then 1 else 0 end) * 1.0
          / nullif(sum(case when cart_adds > 0 then 1 else 0 end), 0)
                                                               as cart_abandonment_rate,

        -- Funnel stage counts
        count(distinct case
            when funnel_exit_stage in ('product_view','add_to_cart','checkout_start','purchase')
            then session_id end)                               as product_view_sessions,

        count(distinct case
            when funnel_exit_stage in ('add_to_cart','checkout_start','purchase')
            then session_id end)                               as add_to_cart_sessions,

        count(distinct case
            when funnel_exit_stage in ('checkout_start','purchase')
            then session_id end)                               as checkout_start_sessions,

        count(distinct case
            when funnel_exit_stage = 'purchase'
            then session_id end)                               as purchase_sessions

    from sessions
    group by 1

),

final as (

    select
        coalesce(o.metric_date, s.metric_date)                  as metric_date,

        -- Order KPIs
        coalesce(o.total_orders,           0)                   as total_orders,
        coalesce(o.unique_buyers,          0)                   as unique_buyers,
        coalesce(o.gross_revenue_usd,      0.0)                 as gross_revenue_usd,
        coalesce(o.net_revenue_usd,        0.0)                 as net_revenue_usd,
        coalesce(o.avg_order_value_usd,    0.0)                 as avg_order_value_usd,
        coalesce(o.median_order_value_usd, 0.0)                 as median_order_value_usd,

        -- Fraud KPIs
        coalesce(o.flagged_fraud_orders,   0)                   as flagged_fraud_orders,
        coalesce(o.fraud_exposure_usd,     0.0)                 as fraud_exposure_usd,
        coalesce(o.flagged_fraud_orders, 0) * 100.0
          / nullif(o.total_orders, 0)                           as fraud_rate_pct,

        -- Session KPIs
        coalesce(s.total_sessions,         0)                   as total_sessions,
        coalesce(s.unique_visitors,        0)                   as unique_visitors,
        coalesce(s.avg_session_duration_s, 0.0)                 as avg_session_duration_s,
        coalesce(s.avg_page_views,         0.0)                 as avg_page_views,
        coalesce(s.cart_abandonment_rate,  0.0)                 as cart_abandonment_rate,

        -- Funnel KPIs
        coalesce(s.product_view_sessions,  0)                   as product_view_sessions,
        coalesce(s.add_to_cart_sessions,   0)                   as add_to_cart_sessions,
        coalesce(s.checkout_start_sessions,0)                   as checkout_start_sessions,
        coalesce(s.purchase_sessions,      0)                   as purchase_sessions,

        -- Overall conversion rate
        coalesce(s.purchase_sessions, 0) * 1.0
          / nullif(s.total_sessions, 0)                         as overall_conversion_rate,

        -- Buyer acquisition
        coalesce(o.new_buyers,       0)                         as new_buyers,
        coalesce(o.returning_buyers, 0)                         as returning_buyers,

        current_timestamp()                                     as dbt_updated_at

    from order_agg   o
    full outer join session_agg s using (metric_date)

)

select * from final
where metric_date is not null
