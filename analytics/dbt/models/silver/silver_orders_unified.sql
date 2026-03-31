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

    select * from {{ ref('stg_orders') }}

),

sessions as (

    select
        session_id,
        user_id_hashed,
        session_start_ts,
        session_end_ts,
        funnel_stage_reached,
        revenue_attributed_usd
    from {{ ref('silver_user_sessions') }}

),

-- Best-match session: the session belonging to the same user that was
-- most recently active within 30 minutes before the order was placed
session_attribution as (

    select
        o.order_id,
        s.session_id                                as attributed_session_id,
        s.funnel_stage_reached,
        row_number() over (
            partition by o.order_id
            order by s.session_start_ts desc
        )                                           as _rn
    from orders o
    left join sessions s
        on  o.user_id_hashed     = s.user_id_hashed
        and o.created_at         >= s.session_start_ts
        and o.created_at         <= s.session_start_ts + interval 30 minutes

),

attributed as (

    select
        order_id,
        attributed_session_id,
        funnel_stage_reached
    from session_attribution
    where _rn = 1

),

final as (

    select
        o.order_id,
        o.user_id_hashed,
        o.status,
        o.prev_status,
        o.status_changed_at,
        o.currency,
        o.total_amount,
        o.total_amount_usd,
        o.discount_usd,
        o.net_amount_usd,
        o.item_count,
        o.promo_code,
        o.shipping_country,
        o.payment_method,
        o.fraud_flag,
        o.fraud_score,
        o.first_cart_at,
        o.created_at,
        o.updated_at,
        o.order_date,
        o.order_tier,
        o.days_consideration,
        o.processed_at,

        -- Session attribution
        a.attributed_session_id,
        a.funnel_stage_reached

    from orders       o
    left join attributed a using (order_id)

)

select * from final
