with source as (
    select * from {{ source('raw', 'sales') }}
),
cities as (
    select * from {{ ref('cities_mapping') }}
),
renamed as (
    select
        "Session No" as session_id,
        "Order No" as load_id,
        "Flight No" as flight_no,
        "Flight Origin" as origin,
        "Flight Destination" as destination,
        "Scheduled Date" as scheduled_date,
        "Ticket ID" as slip_id,
        "Sales Type" as sales_type,
        "Item Category" as item_category,
        "Item Reference" as item_id,
        "Item Price" as price,
        "Qty Sold" as sold_quantity,
        "Sale Amount" as purchase_amount,
        "Promotion Discount" as discount_amount
    from source
), transformed as (
    select
        substring(r.session_id from 7) as session_id,
        r.load_id as load_id,
        'AB'||substring(r.flight_no from 3) as flight_no,
        coalesce(co.city_id, 'UNKNOWN') as origin,
        coalesce(cd.city_id, 'UNKNOWN') as destination,
        split_part(r.scheduled_date, 'T', 1)::date as date,
        split_part(r.scheduled_date, 'T', 2)::time as time,
        r.slip_id as slip_id,
        r.sales_type as sales_type,
        r.item_id as item_id,
        coalesce(r.item_category, 'UNKNOWN') as item_category,
        coalesce(r.price, 0)::decimal as price,
        r.sold_quantity::int as sold_quantity,
        r.purchase_amount::decimal as purchase_amount,
        coalesce(r.discount_amount, 0)::decimal as discount_amount
    from renamed r
    left join cities as co on co.iata_code = r.origin
    left join cities as cd on cd.iata_code = r.destination
), cleaned as (
    select distinct *
    from transformed
    where session_id is not null
    and load_id is not null
    and flight_no is not null
    and date is not null
    and slip_id is not null
    and sales_type is not null
    and item_id is not null
    and sold_quantity is not null
    and purchase_amount is not null
    and sold_quantity >= 0
    and price >= 0
    and discount_amount >= 0
    and date between '{{var("start_date")}}' and '{{var("end_date")}}'
    )
select * from cleaned

