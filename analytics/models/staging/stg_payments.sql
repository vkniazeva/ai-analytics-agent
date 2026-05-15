with source as (
   select * from {{ source('raw', 'payments')}}
),
cities as (
    select * from {{ref('cities_mapping')}}
),
renamed as (
    select
        "Session No"            as session_id,
        "Order No"              as load_id,
        "Ticket ID"             as slip_id,
        "Flight No"             as flight_no,
        "Flight Origin"         as origin_raw,
        "Flight Destination"    as destination_raw,
        "Scheduled Date"        as scheduled_date_raw,
        "Offline"               as is_offline_mode,
        "Sales Type"            as sales_type,
        "Payment Type"          as payment_type,
        "Amount Tendered"       as purchase_amount,
        "Card Digits"           as card_number_prefix,
        "Card Type"             as card_type
    from source
),
transformed as (
    select
        substring(r.session_id from 8) as session_id,
        r.load_id,
        r.slip_id,
        'AB' || substring(r.flight_no from 3) as flight_no,
        coalesce(co.city_id, 'UNKNOWN') as origin,
        coalesce(cd.city_id, 'UNKNOWN') as destination,
        split_part(r.scheduled_date_raw, 'T', 1)::date as date,
        split_part(r.scheduled_date_raw, 'T', 2)::time as time,
        coalesce(cast(r.is_offline_mode as boolean), false) as is_offline_mode,
        r.sales_type,
        r.payment_type,
        r.purchase_amount::int as purchase_amount,
        substring(r.card_number_prefix from 1 for 6) as card_number_prefix,
        r.card_type
    from renamed r
    left join cities co on r.origin_raw = co.iata_code
    left join cities cd on r.destination_raw = cd.iata_code
),
cleaned as (
        select distinct *
        from transformed
        where session_id is not null
            and load_id is not null
            and slip_id is not null
            and flight_no is not null
            and sales_type is not null
            and payment_type is not null
            and purchase_amount is not null
            and date between '{{var("start_date")}}' and '{{var("end_date")}}'
    )
select * from cleaned



