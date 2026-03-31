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

with sessions as (

    select * from {{ ref('silver_user_sessions') }}

    {% if is_incremental() %}
    where processed_at >= current_timestamp() - interval {{ var('incremental_lookback_hours') }} hours
    {% endif %}

),

dim_users as (
    select user_key, user_id_hashed
    from {{ ref('dim_users') }}
    where is_current = true
),

dim_geography as (
    select geo_key, country
    from {{ ref('dim_geography') }}
),

dim_channels as (
    select channel_key, source
    from {{ ref('dim_channels') }}
),

dim_date as (
    select date_key, full_date
    from {{ ref('dim_date') }}
),

-- Parse UTM source from entry_referrer using simple pattern matching
referrer_parsed as (

    select
        session_id,
        case
            when entry_referrer like '%google%'     then 'google'
            when entry_referrer like '%facebook%'   then 'facebook'
            when entry_referrer like '%instagram%'  then 'instagram'
            when entry_referrer like '%email%'
              or entry_referrer like '%newsletter%' then 'email'
            when entry_referrer is null
              or entry_referrer = ''                then 'direct'
            else 'referral'
        end as inferred_source

    from sessions

),

final as (

    select
        -- Natural key
        s.session_id,

        -- Surrogate FK keys
        coalesce(du.user_key,   -1)             as user_key,
        coalesce(dg.geo_key,    -1)             as geo_key,
        coalesce(dc.channel_key,-1)             as channel_key,
        coalesce(dd.date_key,   -1)             as date_key,

        -- Date
        s.session_date,
        s.session_start_ts,
        s.session_end_ts,
        s.session_duration_s,

        -- Engagement
        s.page_views,
        s.product_views,
        s.cart_adds,
        s.cart_removes,
        s.searches,
        s.checkout_attempts,
        s.purchases,

        -- Revenue
        s.revenue_attributed_usd,
        s.cart_value_usd,
        s.cart_abandonment,

        -- Funnel
        s.funnel_stage_reached                  as funnel_exit_stage,

        -- Device & geo
        s.device_type,
        s.device_os,

        -- Fraud
        s.max_fraud_score,
        s.fraud_flagged,

        -- A/B
        s.ab_cohort,

        current_timestamp()                     as dbt_updated_at

    from sessions           s
    left join dim_users     du on s.user_id_hashed          = du.user_id_hashed
    left join dim_geography dg on s.geo_country             = dg.country
    left join dim_date      dd on s.session_date            = dd.full_date
    left join referrer_parsed rp on s.session_id            = rp.session_id
    left join dim_channels  dc on rp.inferred_source        = dc.source

)

select * from final
