with source as (
    select * from {{source('raw', 'wastage')}}
),
cities as (
    select * from {{ref('cities_mapping')}}
),
renamed as (
    select
        "Order No" as load_id,
        "Flight No" as flight_no,
        "Scheduled Route" as route,
        "Scheduled Date" as scheduled_date,
        "Item Category" as item_category,
        "Item Reference" as item_id,
        "Item Type" as item_type,
        "Ordered Qty" as load_quantity,
        "Sold Qty" as sold_quantity,
        "Damaged Waste Qty" as wastage_quantity,
        "QTY Fresh Waste" as fresh_wastage_quantity
    from source
), transformed as (
    select
        r.load_id as load_id,
        'AB' || substring(r.flight_no from 3) as flight_no,
        coalesce(co.city_id, 'UNKNOWN') as origin,
        coalesce(cd.city_id, 'UNKNOWN') as destination,
        to_date(r.scheduled_date, 'DD/MM/YY') as date,
        coalesce(r.item_category, 'UNKNOWN') as item_category,
        r.item_id as item_id,
        coalesce(r.item_type, 'UNKNOWN') as item_type,
        r.load_quantity::int as load_quantity,
        r.sold_quantity::int as sold_quantity,
        r.wastage_quantity::int as wastage_quantity,
        r.fresh_wastage_quantity::int as fresh_wastage_quantity
    from renamed r
    left join cities as co on co.iata_code = split_part(r.route, '-', 1)
    left join cities as cd on cd.iata_code = split_part(r.route, '-', 2)
),
    cleaned as(
        select distinct *
        from transformed
        where load_id is not null
        and flight_no is not null
        and date is not null
        and item_id is not null
        and load_quantity is not null
        and sold_quantity is not null
        and load_quantity >= 0
        and sold_quantity >= 0
        and wastage_quantity >= 0
        and fresh_wastage_quantity >= 0
        and date between '{{var("start_date")}}' and '{{var("end_date")}}'
    )
select * from cleaned

