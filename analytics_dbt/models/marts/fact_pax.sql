with flights as (
    select
        p.flight_no as flight_number,
        {{ date_to_key('p.date') }} as date,
        {{ round_hour_from_time('f.time') }} as hour_of_departure,
        p.class as travel_class,
        p.pax_quantity as number_of_passengers
    from {{ ref('stg_pax') }} p
    inner join {{ ref('int_flights')}} f
        on p.flight_no = f.flight_no
        and p.date = f.date
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
    number_of_passengers as number_of_passengers,
        case
            when number_of_passengers = 0 then 'zero_pax_count'
            else null
        end as potential_error
from replaced
