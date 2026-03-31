with source as (

    select *
    from {{ source('silver', 'enriched_events') }}

    {% if is_incremental() %}
    -- Lookback window: re-process last N hours to catch Silver late-arrivals
    where processed_at >= current_timestamp() - interval {{ var('incremental_lookback_hours') }} hours
    {% endif %}

),

renamed as (

    select
        -- Identity
        event_id,
        user_id_hashed,
        session_id,

        -- Event
        event_type,
        event_ts,
        event_date,
        ingested_at,
        processed_at,

        -- Device
        device_type,
        device_os,
        app_version,

        -- Page
        page_url,
        page_referrer,

        -- Product (nullable for non-product events)
        product_sku,
        product_category,
        product_subcategory,
        product_brand,
        product_price_usd,
        product_cost_usd,
        product_margin_pct,
        product_quantity,

        -- Geography
        geo_country,
        geo_city,
        geo_timezone,
        geo_region,

        -- Experimentation
        ab_cohort,

        -- Event-specific
        search_query,
        order_id,

        -- Fraud signals
        fraud_score,
        fraud_signals,
        case when fraud_score > {{ var('fraud_score_threshold') }}
            then true else false
        end                             as fraud_flagged,

        -- Data quality
        dq_flag

    from source

    -- Exclude events still flagged CRITICAL that somehow made it to Silver
    where dq_flag != 'CRITICAL' or dq_flag is null

)

select * from renamed
