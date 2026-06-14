with tmp as (
select
    line_id,
    flight_number,
    {{ date_to_key('date') }} as date,
    {{ round_hour_from_time('time') }} as hour_of_departure,
    load_id
from {{ ref('int_flight_load')}}
),
with_key as (
select distinct
    line_id,
    {{dbt_utils.generate_surrogate_key([
        'flight_number', 'date', 'hour_of_departure']) }} as flight_key,
    load_id
from tmp
)
select
    wk.line_id,
    wk.flight_key,
    wk.load_id
from with_key wk
inner join {{ ref('dim_flights') }} df
    on wk.flight_key = df.flight_key