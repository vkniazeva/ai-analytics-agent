with ordered as (
    select
        line_id,
        flight_no,
        date,
        time,
        origin,
        destination,
        load_id,
        row_number() over (
            partition by line_id
            order by date, time
            ) as rn
    from {{ ref('stg_line') }}
),

     routes as (
         select
             line_id,
             date,

             max(load_id) as load_id,

             array_agg(
                     flight_no
                     order by date, time
             ) as connected_flights,

             array[
                 max(case when rn = 1 then origin end)
                 ]
                 ||
             array_agg(
                     destination
                     order by date, time
             ) as route

         from ordered
         group by
             line_id,
             date
     )

select
    *,
    case
        when cardinality(connected_flights) <> 2
            then 'potentially_incorrect_route'
        when cardinality(route) <> 3
            then 'potentially_incorrect_route'
        else null
        end as potential_error
from routes
