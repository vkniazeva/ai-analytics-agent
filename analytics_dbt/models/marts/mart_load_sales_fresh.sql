with pax_by_line as (
    select
        bfl.line_id,
        sum(fp.number_of_passengers) as total_passengers
    from {{ ref('bridge_flight_load') }} bfl
    join {{ ref('dim_flights') }} f on bfl.flight_key = f.flight_key
    join {{ ref('fact_pax') }} fp on fp.flight_key = f.flight_key
    group by bfl.line_id
),
load_with_date as (
    select
        bfl.load_id,
        bfl.line_id,
        min(f.date) as date,
        array_agg(f.flight_number order by f.date, f.hour_of_departure) as flight_numbers,
        array_agg(f.origin order by f.date, f.hour_of_departure) as origins,
        array_agg(f.destination order by f.date, f.hour_of_departure) as destinations
    from {{ ref('bridge_flight_load') }} bfl
    join {{ ref('dim_flights') }} f on bfl.flight_key = f.flight_key
    group by bfl.load_id, bfl.line_id
),
sales_by_load as (
    select
        load_id,
        product_key,
        count(slip_id) as tickets_count,
        sum(sold_quantity) as sold_quantity
    from {{ ref('fact_sales') }}
    group by load_id, product_key
)
select
    lwd.load_id,
    lwd.line_id,
    lwd.flight_numbers,
    lwd.origins,
    lwd.destinations,
    dd.date,
    dd.month_name,
    dd.year,
    pax.total_passengers,
    fil.total_loaded_quantity,
    dp.item_id,
    dp.category,
    dp.price,
    coalesce(s.sold_quantity, 0) as sold_quantity,
    coalesce(s.tickets_count, 0) as tickets_count
from load_with_date lwd

join {{ ref('dim_date') }} dd
    on lwd.date = dd.date_key

join {{ ref('fact_items_load') }} fil
    on fil.load_id = lwd.load_id
    and fil.potential_error is null

join {{ ref('dim_products') }} dp
    on dp.product_key = fil.product_key
    and dp.item_type = 'Fresh Product'
    and dp.category != 'BOL Products'
    and dp.status = 'Active'

left join pax_by_line pax
    on pax.line_id = lwd.line_id

left join sales_by_load s
    on s.load_id = lwd.load_id
    and s.product_key = fil.product_key
