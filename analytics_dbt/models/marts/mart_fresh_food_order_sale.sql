select
    f.flight_key,
    f.flight_number,
    f.origin,
    f.destination,
    dd.date,
    dd.month_name,
    dd.year,
    dd.weekday_name,
    dd.is_weekend,
    f.hour_of_departure,
    dt.day_period,
    dt.is_night,
    dt.am_pm,
    dp.item_id,
    dp.price,
    coalesce(s.sold_quantity, 0) as sold_quantity
from {{ ref('bridge_flight_load') }} bfl

join {{ ref('dim_flights') }} f
    on f.flight_key = bfl.flight_key

join {{ ref('dim_date') }} dd on f.date = dd.date_key
join {{ ref('dim_time') }} dt on f.hour_of_departure = dt.time_key

join {{ ref('fact_items_load') }} fil
    on fil.load_id = bfl.load_id
    and fil.potential_error is null

join {{ ref('dim_products') }} dp
    on dp.product_key = fil.product_key
        and dp.item_type = 'Fresh Product'
        and dp.category != 'BOL Products'
        and dp.status = 'Active'

left join (
    select
        load_id,
        product_key,
        count(slip_id) as tickets_count,
        sum(sold_quantity) as sold_quantity
    from {{ ref('fact_sales') }}
    group by load_id, product_key
) s
    on s.load_id = bfl.load_id
        and s.product_key = fil.product_key