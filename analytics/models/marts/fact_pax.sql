with flights as (
    select
        flight_no as flight_number,
        {{ date_to_key('date') }} as date,
        {{ round_hour_from_time('time') }} as hour_of_departure,
        class as travel_class,
        pax_quantity as number_of_passengers
from {{ ref('stg_pax') }}
),
replaced as
( select
    {{dbt_utils.generate_surrogate_key([
    'flight_number', 'date', 'hour_of_departure']) }} as flight_key,
    flight_number as flight_number,
    date as date,
    hour_of_departure as hour_of_departure,
    travel_class as travel_class,
    sum(number_of_passengers) as number_of_passengers
from flights
group by  flight_key, flight_number, date, hour_of_departure, travel_class
)
select
     {{dbt_utils.generate_surrogate_key([
     'flight_number', 'date', 'hour_of_departure', 'travel_class' ]) }} as passenger_count_key,
    flight_key as flight_key,
    date as date,
    hour_of_departure as hour_of_departure,
    travel_class as travel_class,
    number_of_passengers as number_of_passengers
from replaced
