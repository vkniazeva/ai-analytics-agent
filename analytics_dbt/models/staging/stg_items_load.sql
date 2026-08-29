with source as (
    select *
    from {{source('raw', 'orders')}}
),
mapping as (
    select * from {{ref('product_id_mapping')}}
),
renamed as (
    select
          "flight_number" as flight_no,
          "departure_date" as date,
          "order_num" as load_id,
          "item_reference" as original_item_id,
          "total_qty_loaded" as total_loaded_quantity
    from source
),
transformed as (
    select
        'AB' || substring(r.flight_no from 3) as flight_no,
        to_date(split_part(r.date, ' ', 1), 'YYYY/MM/DD') as date,
        split_part(r.date, ' ', 2)::time as time,
        r.load_id as load_id,
        m.anonymized_reference as item_id,
        r.total_loaded_quantity as total_loaded_quantity
    from renamed r
    inner join mapping m on r.original_item_id::text = m.original_reference
),
cleaned as (
    select distinct *
    from transformed
    where flight_no is not null
        and date is not null
        and time is not null
        and load_id is not null
        and item_id is not null
        and total_loaded_quantity is not null
        and total_loaded_quantity >= 0
        and date between '{{var("start_date")}}' and '{{var("end_date")}}'
)
select * from cleaned

