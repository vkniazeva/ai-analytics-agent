with tmp as (
    select
        p.flight_no as flight_number,
        {{ date_to_key('p.date') }} as date,
        {{ round_hour_from_time('f.time') }} as hour_of_departure,
        p.session_id as sales_session_id,
        p.load_id as load_id,
        p.slip_id as slip_id,
        p.sales_type as sales_type,
        p.payment_type as payment_type,
        p.card_number_prefix as card_number_prefix,
        p.purchase_amount_main as purchase_amount_main
    from {{ ref('stg_payments') }} p
    inner join {{ ref('int_flights')}} f
        on p.flight_no = f.flight_no
        and p.date = f.date
),
replaced as (
    select
        {{dbt_utils.generate_surrogate_key([
         'slip_id', 'sales_type', 'payment_type', 'card_number_prefix' ]) }} as payment_transaction_key,
        {{dbt_utils.generate_surrogate_key([
        'flight_number', 'date', 'hour_of_departure']) }} as flight_key,
        sales_session_id as sales_session_id,
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
    sales_session_id,
    load_id as load_id,
    slip_id as slip_id,
    sales_type as sales_type,
    payment_type as payment_type,
    card_number_prefix as card_number_prefix,
    sum(purchase_amount_main) as purchase_amount_main
from replaced
group by payment_transaction_key, flight_key, sales_session_id, load_id, slip_id, sales_type, payment_type, card_number_prefix
