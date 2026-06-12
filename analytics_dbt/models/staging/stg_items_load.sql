with source as (
    select *
    from {{source('raw', 'orders')}}
),
renamed as (
    select
          "flight_number" as flight_no,
          "departure_date" as date,
          "order_num" as load_id,
          "item_reference" as item_id,
          "total_qty_loaded" as total_loaded_quantity
    from source
),
transformed as (
    select
        'AB' || substring(flight_no from 3) as flight_no,
        to_date(split_part(date, ' ', 1), 'YYYY/MM/DD') as date,
        split_part(date, ' ', 2)::time as time,
        load_id as load_id,
        item_id as item_id,
        total_loaded_quantity as total_loaded_quantity
    from renamed
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

