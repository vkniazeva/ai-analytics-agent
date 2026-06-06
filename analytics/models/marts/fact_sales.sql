with tmp as (
    select
        flight_no as flight_number,
        {{ date_to_key('date') }} as date,
        {{ round_hour_from_time('time') }} as hour_of_departure,
        session_id as sales_session_id,
        load_id as load_id,
        slip_id as slip_id,
        sales_type as sales_type,
        currency as currency,
        {{dbt_utils.generate_surrogate_key( [
        'item_id'] ) }} as product_key,
        sold_quantity as sold_quantity,
        purchase_amount as purchase_amount,
        discount_amount as discount_amount
from {{ ref('stg_sales') }}
),
replaced as (
    select
        {{dbt_utils.generate_surrogate_key([
         'flight_number', 'date', 'hour_of_departure', 'product_key', 'sales_type' ]) }} as sale_transaction_key,
        {{dbt_utils.generate_surrogate_key([
        'flight_number', 'date', 'hour_of_departure']) }} as flight_key,
        sales_session_id as sales_session_id,
        load_id as load_id,
        sales_type as sales_type,
        currency as currency,
        product_key as product_key,
        sold_quantity as sold_quantity,
        purchase_amount as purchase_amount,
        discount_amount as discount_amount
    from tmp
)
select
    sale_transaction_key as sale_transaction_key,
    flight_key as flight_key,
    sales_session_id as sales_session_id,
    load_id as load_id,
    sales_type as sales_type,
    currency as currency,
    product_key as product_key,
    sum(sold_quantity) as sold_quantity,
    sum(purchase_amount) as purchase_amount,
    sum(discount_amount) as discount_amount
from replaced
group by sale_transaction_key, flight_key, sales_session_id, load_id, sales_type, currency, product_key
