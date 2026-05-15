with source as (
    select * from {{source('raw', 'schedule')}}
), cities as (
    select * from {{ref('cities_mapping')}}
),
renamed as (
    select
        "line_id" as line_id,
        "flight_no" as flight_no,
        "iata_departure" as origin,
        "iata_destination" as destination,
        "scheduled_datetime" as scheduled_date,
        "order_no" as load_id
    from source
),
transformed as (
    select
        r.line_id as line_id,
        'AB'||substring(r.flight_no from 3) as flight_no,
        coalesce(co.city_id, 'UNKNOWN') as origin,
        coalesce(cd.city_id, 'UNKNOWN') as destination,
        to_date(split_part(r.scheduled_date, ' ', 1), 'DD/MM/YY') as date,
        split_part(r.scheduled_date, ' ', 2)::time as time,
        r.load_id as load_id
    from renamed r
    left join cities as co on co.iata_code = r.origin
    left join cities as cd on cd.iata_code = r.destination
), cleaned as (
    select distinct *
    from transformed
    where line_id is not null
    and flight_no is not null
    and date is not null
    and date between '{{var("start_date")}}' and '{{var("end_date")}}'
    )
select * from cleaned