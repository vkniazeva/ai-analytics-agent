with tmp as (
    select
    load_id as load_id,
    flight_no as flight_number,
    {{date_to_key('date')}} as date,
    {{dbt_utils.generate_surrogate_key(['item_id'])}} as product_key,
    load_quantity as load_quantity,
    sold_quantity as sold_quantity,
    wastage_quantity as wastage_quantity,
    fresh_wastage_quantity as fresh_wastage_quantity
from {{ref('stg_wastage')}}
),
replaced as(
    select
        {{dbt_utils.generate_surrogate_key(['flight_number', 'date', 'product_key', 'load_id'])}} as wastage_record_key,
        load_id as load_id,
        {{dbt_utils.generate_surrogate_key(['flight_number', 'date'])}} as flight_key_with_date,
        product_key as product_key,
        load_quantity as load_quantity,
        sold_quantity as sold_quantity,
        wastage_quantity as wastage_quantity,
        fresh_wastage_quantity as fresh_wastage_quantity
    from tmp)

select
    wastage_record_key as wastage_record_key,
    load_id as load_id,
    flight_key_with_date as flight_key_with_date,
    product_key as product_key,
    sum (load_quantity) as load_quantity,
    sum (sold_quantity) as sold_quantity,
    sum (wastage_quantity) as wastage_quantity,
    sum (fresh_wastage_quantity) as fresh_wastage_quantity
from replaced
group by wastage_record_key, load_id, flight_key_with_date, product_key