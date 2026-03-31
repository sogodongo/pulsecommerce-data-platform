-- =============================================================================
-- gold/dim_geography.sql
-- =============================================================================
-- Country-level geography dimension with region + GDPR scope flag.
-- Seeded from a static country list; enriched with regional mapping.
-- Materialisation: table (full refresh — static reference data)
-- =============================================================================

{{
    config(
        materialized    = 'table',
        file_format     = 'iceberg',
        on_schema_change= 'sync_all_columns'
    )
}}

-- Build country list from observed countries in Silver
with observed_countries as (

    select distinct
        geo_country         as country
    from {{ source('silver', 'enriched_events') }}
    where geo_country is not null
      and length(geo_country) = 2   -- valid ISO 2-char codes only

),

country_metadata as (

    -- Static country attribute mapping
    -- In production this would be a dbt seed (CSV) — inlined here for portability
    select country, country_name, region, subregion, is_gdpr_scope, currency_code
    from (
        values
            ('US','United States',   'AMER',  'Northern America',   false, 'USD'),
            ('CA','Canada',          'AMER',  'Northern America',   false, 'CAD'),
            ('MX','Mexico',          'LATAM', 'Central America',    false, 'MXN'),
            ('BR','Brazil',          'LATAM', 'South America',      false, 'BRL'),
            ('AR','Argentina',       'LATAM', 'South America',      false, 'ARS'),
            ('CO','Colombia',        'LATAM', 'South America',      false, 'COP'),
            ('GB','United Kingdom',  'EMEA',  'Northern Europe',    true,  'GBP'),
            ('DE','Germany',         'EMEA',  'Western Europe',     true,  'EUR'),
            ('FR','France',          'EMEA',  'Western Europe',     true,  'EUR'),
            ('IT','Italy',           'EMEA',  'Southern Europe',    true,  'EUR'),
            ('ES','Spain',           'EMEA',  'Southern Europe',    true,  'EUR'),
            ('NL','Netherlands',     'EMEA',  'Western Europe',     true,  'EUR'),
            ('SE','Sweden',          'EMEA',  'Northern Europe',    true,  'SEK'),
            ('NO','Norway',          'EMEA',  'Northern Europe',    true,  'NOK'),
            ('DK','Denmark',         'EMEA',  'Northern Europe',    true,  'DKK'),
            ('FI','Finland',         'EMEA',  'Northern Europe',    true,  'EUR'),
            ('PL','Poland',          'EMEA',  'Eastern Europe',     true,  'PLN'),
            ('AT','Austria',         'EMEA',  'Western Europe',     true,  'EUR'),
            ('CH','Switzerland',     'EMEA',  'Western Europe',     false, 'CHF'),
            ('ZA','South Africa',    'EMEA',  'Southern Africa',    false, 'ZAR'),
            ('NG','Nigeria',         'EMEA',  'West Africa',        false, 'NGN'),
            ('KE','Kenya',           'EMEA',  'East Africa',        false, 'KES'),
            ('EG','Egypt',           'EMEA',  'North Africa',       false, 'EGP'),
            ('GH','Ghana',           'EMEA',  'West Africa',        false, 'GHS'),
            ('TZ','Tanzania',        'EMEA',  'East Africa',        false, 'TZS'),
            ('IN','India',           'APAC',  'South Asia',         false, 'INR'),
            ('CN','China',           'APAC',  'East Asia',          false, 'CNY'),
            ('JP','Japan',           'APAC',  'East Asia',          false, 'JPY'),
            ('AU','Australia',       'APAC',  'Oceania',            false, 'AUD'),
            ('SG','Singapore',       'APAC',  'Southeast Asia',     false, 'SGD'),
            ('ID','Indonesia',       'APAC',  'Southeast Asia',     false, 'IDR'),
            ('TH','Thailand',        'APAC',  'Southeast Asia',     false, 'THB'),
            ('MY','Malaysia',        'APAC',  'Southeast Asia',     false, 'MYR'),
            ('PH','Philippines',     'APAC',  'Southeast Asia',     false, 'PHP'),
            ('VN','Vietnam',         'APAC',  'Southeast Asia',     false, 'VND'),
            ('KR','South Korea',     'APAC',  'East Asia',          false, 'KRW'),
            ('NZ','New Zealand',     'APAC',  'Oceania',            false, 'NZD')
    ) as t(country, country_name, region, subregion, is_gdpr_scope, currency_code)

),

-- Left join: keep all observed countries even if not in our static list
final as (

    select
        {{ generate_surrogate_key(['oc.country']) }}     as geo_key,
        oc.country,
        coalesce(cm.country_name, concat('Unknown (', oc.country, ')'))
                                                        as country_name,
        coalesce(cm.region,       'OTHER')              as region,
        coalesce(cm.subregion,    'Unknown')            as subregion,
        coalesce(cm.is_gdpr_scope, false)               as is_gdpr_scope,
        coalesce(cm.currency_code, 'USD')               as currency_code,
        current_timestamp()                             as dbt_updated_at

    from observed_countries   oc
    left join country_metadata cm using (country)

)

select * from final
