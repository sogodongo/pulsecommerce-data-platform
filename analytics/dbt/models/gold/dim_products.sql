-- =============================================================================
-- gold/dim_products.sql
-- =============================================================================
-- Product dimension — full refresh from silver.product_catalog.
-- Adds surrogate key, price_band, and pipe-delimited tags for
-- Redshift Spectrum compatibility (Redshift doesn't support ARRAY natively).
--
-- Materialisation: table (full refresh — ~100K SKUs, fast to rebuild)
-- =============================================================================

{{
    config(
        materialized    = 'table',
        file_format     = 'iceberg',
        on_schema_change= 'sync_all_columns'
    )
}}

with catalog as (

    select *
    from {{ source('silver', 'product_catalog') }}

),

final as (

    select
        -- Surrogate key
        {{ generate_surrogate_key(['sku']) }}    as product_key,
        sku,

        -- Attributes
        name,
        category,
        subcategory,
        brand,
        price_usd,
        cost_usd,
        margin_pct,
        is_active,
        weight_kg,

        -- Tags: array → pipe-delimited string for Redshift compatibility
        array_join(tags, '|')                   as tags,

        image_url,

        -- Derived price band
        case
            when price_usd < 25   then 'budget'
            when price_usd < 100  then 'mid'
            when price_usd < 300  then 'premium'
            else                       'luxury'
        end                                     as price_band,

        updated_at,
        current_timestamp()                     as dbt_updated_at

    from catalog

    -- Include all SKUs (active + inactive) — fct_orders may reference old SKUs
    -- Gold queries filter is_active = true where current catalog needed

)

select * from final
