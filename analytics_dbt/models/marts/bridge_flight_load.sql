with propagate_load_id as (
    select
        line_id,
        flight_no as flight_number,
        {{ date_to_key('date') }} as date,
        {{ round_hour_from_time('time') }} as hour_of_departure,
        max(load_id) over (partition by line_id) as load_id
    from {{ ref('stg_schedule') }}
),
with_key as (
    select distinct
        line_id,
        {{dbt_utils.generate_surrogate_key([
            'flight_number', 'date', 'hour_of_departure']) }} as flight_key,
        load_id
    from propagate_load_id
    where load_id is not null
)
select
    wk.line_id,
    wk.flight_key,
    wk.load_id
from with_key wk
inner join {{ ref('dim_flights') }} df
    on wk.flight_key = df.flight_key