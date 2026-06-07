with initial as (
    select
        flight_no as flight_number,
        {{ date_to_key('date') }} as date,
        {{ round_hour_from_time('time') }} as hour_of_departure,
        load_id as load_id,
        item_id as item_id,
        total_loaded_quantity as total_loaded_quantity
    from {{ ref('stg_items_load')}}
),
replaced as (
    select
    {{dbt_utils.generate_surrogate_key([
        'flight_number', 'date', 'hour_of_departure']) }} as flight_key,
    {{dbt_utils.generate_surrogate_key([
        'load_id', 'item_id']) }} as item_load_key,
    load_id as load_id,
    {{dbt_utils.generate_surrogate_key([
        'item_id']) }} as product_key,
    sum(total_loaded_quantity) as total_loaded_quantity
    from initial
    group by flight_key, item_load_key, load_id, product_key
)
select * from replaced