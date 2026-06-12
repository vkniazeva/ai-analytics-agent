with dates as (
    {{dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2024-01-01' as date)",
        end_date="cast(current_date + interval '1 year' as date)"
        )}}
)

select
    {{ date_to_key('date_day') }} as date_key,
    date_day as date,
    {{ year_from_date('date_day') }} as year,
    {{ month_from_date('date_day') }} as month,
    {{ day_from_date('date_day')}} as day,
    {{ quarter_from_date('date_day') }} as quarter,
    {{ day_of_week('date_day') }} as week_day,
    to_char(date_day, 'Month') as month_name,
    to_char(date_day, 'Day') as weekday_name,
    {{ is_weekend('date_day') }} as is_weekend,
    {{ season_from_month(month_from_date('date_day')) }} as season
from dates