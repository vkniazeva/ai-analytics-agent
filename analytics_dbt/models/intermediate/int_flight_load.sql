select distinct
    line_id,
    flight_no as flight_number,
    origin,
    destination,
    date,
    time,

    max(load_id) over (
        partition by line_id
        ) as load_id

from {{ ref('stg_schedule')}}
where load_id is not null