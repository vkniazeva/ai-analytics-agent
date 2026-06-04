select
    {{dbt_utils.generate_surrogate_key([
    'flight_no', 'date', 'time']) }} as flight_key,
    flight_no as flight_number,
    origin as origin,
    destination as destination,
    {{ date_to_key('date') }} as date,
    {{ round_hour_from_time('time') }} as hour_of_departure,
    time as time
from {{ ref('int_flights')}}
