{{
    config(
        materialized    = 'table',
        file_format     = 'iceberg',
        on_schema_change= 'sync_all_columns'
    )
}}

with date_spine as (

    select
        explode(
            sequence(
                to_date('2020-01-01'),
                to_date('2030-12-31'),
                interval 1 day
            )
        ) as full_date

),

final as (

    select
        -- Surrogate key: YYYYMMDD integer
        cast(date_format(full_date, 'yyyyMMdd') as int)     as date_key,
        full_date,

        -- Calendar attributes
        year(full_date)                                     as year,
        quarter(full_date)                                  as quarter,
        month(full_date)                                    as month,
        date_format(full_date, 'MMMM')                     as month_name,
        weekofyear(full_date)                               as week_of_year,
        dayofmonth(full_date)                               as day_of_month,

        -- ISO: 1=Monday ... 7=Sunday
        dayofweek(full_date)                               as day_of_week,
        date_format(full_date, 'EEEE')                     as day_name,

        -- Flags
        case when dayofweek(full_date) in (1, 7)
            then true else false
        end                                                 as is_weekend,

        case when dayofmonth(full_date) = 1
            then true else false
        end                                                 as is_month_start,

        case when full_date = last_day(full_date)
            then true else false
        end                                                 as is_month_end,

        case when full_date = last_day(
                make_date(year(full_date), ceil(month(full_date) / 3.0) * 3, 1)
             )
            then true else false
        end                                                 as is_quarter_end,

        -- PulseCommerce fiscal calendar: year starts Feb 1
        -- Fiscal year = calendar year if month >= Feb, else calendar year - 1
        case
            when month(full_date) >= {{ var('fiscal_year_start_month') }}
            then year(full_date)
            else year(full_date) - 1
        end                                                 as fiscal_year,

        -- Fiscal quarter: Q1 starts Feb
        case
            when month(full_date) >= 2  and month(full_date) <= 4  then 1
            when month(full_date) >= 5  and month(full_date) <= 7  then 2
            when month(full_date) >= 8  and month(full_date) <= 10 then 3
            else 4
        end                                                 as fiscal_quarter

    from date_spine

)

select * from final
order by full_date
