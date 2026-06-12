with initial as (
    select
        il.flight_no as flight_number,
        {{ date_to_key('il.date') }} as date,
        {{ round_hour_from_time('f.time') }} as hour_of_departure,
        il.load_id as load_id,
        il.item_id as item_id,
        il.total_loaded_quantity as total_loaded_quantity
    from {{ ref('stg_items_load')}} il
    inner join {{ ref('int_flights')}} f
        on il.flight_no = f.flight_no
        and il.date = f.date
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
    sum(total_loaded_quantity) as total_loaded_quantity,
    case
        when sum(total_loaded_quantity) = 0 then 'zero_load_error'
        else null
    end as potential_error
    from initial
    group by flight_key, item_load_key, load_id, product_key
)
select * from replaced