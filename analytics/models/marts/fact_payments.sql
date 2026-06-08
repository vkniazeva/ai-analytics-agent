with tmp as (
    select
        flight_no as flight_number,
        {{ date_to_key('date') }} as date,
        {{ round_hour_from_time('time') }} as hour_of_departure,
        session_id as session_id,
        load_id as load_id,
        slip_id as slip_id,
        sales_type as sales_type,
        payment_type as payment_type,
        card_number_prefix as card_number_prefix,
        purchase_amount_main as purchase_amount_main
    from {{ ref('stg_payments') }}
),
replaced as (
    select
        {{dbt_utils.generate_surrogate_key([
         'slip_id', 'flight_number', 'date', 'hour_of_departure', 'sales_type', 'payment_type' ]) }} as payment_transaction_key,
        {{dbt_utils.generate_surrogate_key([
        'flight_number', 'date', 'hour_of_departure']) }} as flight_key,
        session_id as session_id,
        load_id as load_id,
        slip_id as slip_id,
        sales_type as sales_type,
        payment_type as payment_type,
        card_number_prefix as card_number_prefix,
        purchase_amount_main as purchase_amount_main
    from tmp
)
select
    payment_transaction_key as payment_transaction_key,
    flight_key as flight_key,
    session_id as session_id,
    load_id as load_id,
    slip_id as slip_id,
    sales_type as sales_type,
    payment_type as payment_type,
    card_number_prefix as card_number_prefix,
    sum(purchase_amount_main) as purchase_amount_main
from replaced
group by payment_transaction_key, flight_key, session_id, load_id, slip_id, sales_type, payment_type, card_number_prefix
