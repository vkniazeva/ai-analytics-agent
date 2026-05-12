with source as (
    select * from {{ source('raw', 'pax') }}
),

cities as (
    select * from {{ ref('cities_mapping') }}
),

renamed as (
    select
        "Flight Number"                         as flight_no,
        "Scheduled Date"                        as scheduled_date_raw,
        "Scheduled Time"                        as scheduled_time_raw,
        "Origin"                                as origin_raw,
        "Destination"                           as destination_raw,
        "Class"                                 as class,
        "PAX"                                   as pax_quantity
    from source
),

transformed as (
    select
        'AB' || substring(r.flight_no from 3)   as flight_no,
        to_date(r.scheduled_date_raw, 'DD/MM/YY') as date,
        r.scheduled_time_raw::time              as time,
        coalesce(co.city_id, 'UNKNOWN')         as origin,
        coalesce(cd.city_id, 'UNKNOWN')         as destination,
        r.class,
        r.pax_quantity::int                     as pax_quantity
    from renamed r
    left join cities co on r.origin_raw = co.iata_code
    left join cities cd on r.destination_raw = cd.iata_code
),

cleaned as (
    select distinct *
    from transformed
    where flight_no is not null
      and date is not null
      and pax_quantity is not null
      and pax_quantity >= 0
      and date between '2026-01-01' and '2026-03-31'
)

select * from cleaned
