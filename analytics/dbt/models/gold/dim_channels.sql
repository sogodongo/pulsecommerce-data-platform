-- =============================================================================
-- gold/dim_channels.sql
-- =============================================================================
-- Marketing channel dimension — derived from ad attribution + session referrer.
-- Collapsed into standard channel groups for BI reporting.
-- Materialisation: table (full refresh)
-- =============================================================================

{{
    config(
        materialized    = 'table',
        file_format     = 'iceberg',
        on_schema_change= 'sync_all_columns'
    )
}}

with ad_attribution_sources as (

    -- Paid channels from ad attribution data
    select distinct
        lower(source)           as source,
        'cpc'                   as medium,
        campaign_name           as campaign,
        ad_set_name             as ad_set,
        cast(null as string)    as ab_cohort
    from {{ source('bronze', 'ad_attribution_raw') }}
    where source is not null

),

session_referrer_sources as (

    -- Organic / direct channels inferred from session entry referrer
    select distinct
        case
            when entry_referrer like '%google%'     then 'google'
            when entry_referrer like '%bing%'       then 'bing'
            when entry_referrer like '%facebook%'
              or entry_referrer like '%fb.com%'     then 'facebook'
            when entry_referrer like '%instagram%'  then 'instagram'
            when entry_referrer like '%email%'
              or entry_referrer like '%newsletter%' then 'email'
            when entry_referrer is null
              or trim(entry_referrer) = ''          then 'direct'
            else 'referral'
        end                     as source,
        case
            when entry_referrer like '%google%'     then 'organic'
            when entry_referrer like '%email%'      then 'email'
            when entry_referrer is null             then 'direct'
            else 'referral'
        end                     as medium,
        cast(null as string)    as campaign,
        cast(null as string)    as ad_set,
        ab_cohort
    from {{ ref('silver_user_sessions') }}
    where session_id is not null

),

combined as (
    select * from ad_attribution_sources
    union all
    select * from session_referrer_sources
),

deduplicated as (

    select distinct
        source,
        medium,
        campaign,
        ad_set,
        ab_cohort
    from combined
    where source is not null

),

final as (

    select
        {{ generate_surrogate_key(['source', 'medium', 'coalesce(campaign, "none")', 'coalesce(ab_cohort, "none")']) }}
                                    as channel_key,
        source,
        medium,
        campaign,
        ad_set,
        ab_cohort,

        -- Collapsed channel group for executive dashboards
        case
            when source in ('google','bing') and medium = 'cpc'  then 'Paid Search'
            when source in ('facebook','instagram') and medium = 'cpc' then 'Paid Social'
            when source in ('facebook','instagram') and medium != 'cpc' then 'Organic Social'
            when medium = 'email'                                 then 'Email'
            when source = 'direct'                                then 'Direct'
            when source = 'referral'                              then 'Referral'
            when medium = 'organic'                               then 'Organic Search'
            else 'Other'
        end                         as channel_group,

        current_timestamp()         as dbt_updated_at

    from deduplicated

)

select * from final
