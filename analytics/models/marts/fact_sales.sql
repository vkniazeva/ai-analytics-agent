with tmp as (
    select
        s.flight_no as flight_number,
        {{ date_to_key('s.date') }} as date,
        {{ round_hour_from_time('f.time') }} as hour_of_departure,
        s.session_id as sales_session_id,
        s.load_id as load_id,
        s.slip_id as slip_id,
        s.sales_type as sales_type,
        s.currency as currency,
        {{dbt_utils.generate_surrogate_key( [
        's.item_id'] ) }} as product_key,
        s.sold_quantity as sold_quantity,
        s.purchase_amount as purchase_amount,
        s.discount_amount as discount_amount
    from {{ ref('stg_sales') }} s
    inner join {{ ref('int_flights')}} f
        on s.flight_no = f.flight_no
        and s.date = f.date
),
replaced as (
    select
        {{dbt_utils.generate_surrogate_key([
         'slip_id', 'product_key', 'sales_type' ]) }} as sale_transaction_key,
        {{dbt_utils.generate_surrogate_key([
        'flight_number', 'date', 'hour_of_departure']) }} as flight_key,
        sales_session_id as sales_session_id,
        slip_id as slip_id,
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
    slip_id as slip_id,
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
group by sale_transaction_key, slip_id, flight_key, sales_session_id, load_id, sales_type, currency, product_key
