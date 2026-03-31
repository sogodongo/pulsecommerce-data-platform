{{
    config(
        materialized          = 'incremental',
        incremental_strategy  = 'merge',
        unique_key            = 'session_id',
        file_format           = 'iceberg',
        partition_by          = [{'field': 'session_date', 'data_type': 'date'}],
        on_schema_change      = 'sync_all_columns'
    )
}}

with source as (

    select *
    from {{ source('silver', 'user_sessions') }}

    {% if is_incremental() %}
    where processed_at >= current_timestamp() - interval {{ var('incremental_lookback_hours') }} hours
    {% endif %}

),

cleaned as (

    select
        session_id,
        user_id_hashed,

        -- Timestamps
        session_start_ts,
        session_end_ts,
        coalesce(session_duration_s, 0)             as session_duration_s,
        cast(session_start_ts as date)              as session_date,

        -- Funnel
        funnel_stage_reached,
        coalesce(page_views,        0)              as page_views,
        coalesce(product_views,     0)              as product_views,
        coalesce(cart_adds,         0)              as cart_adds,
        coalesce(cart_removes,      0)              as cart_removes,
        coalesce(searches,          0)              as searches,
        coalesce(checkout_attempts, 0)              as checkout_attempts,
        coalesce(purchases,         0)              as purchases,

        -- Revenue
        coalesce(revenue_attributed_usd, 0.0)       as revenue_attributed_usd,
        coalesce(cart_value_usd,         0.0)       as cart_value_usd,
        coalesce(cart_abandonment,       false)     as cart_abandonment,

        -- Context
        entry_page_url,
        exit_page_url,
        entry_referrer,
        device_type,
        device_os,
        geo_country,
        ab_cohort,

        -- Fraud
        coalesce(max_fraud_score, 0.0)              as max_fraud_score,
        coalesce(fraud_flagged,   false)            as fraud_flagged,

        processed_at

    from source

    -- Discard zero-event phantom sessions (shouldn't exist but guard anyway)
    where session_id is not null
      and user_id_hashed is not null

)

select * from cleaned
