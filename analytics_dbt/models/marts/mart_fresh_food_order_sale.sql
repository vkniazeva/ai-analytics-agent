with pax_by_flight as (
    select
        fp.flight_key,
        sum(fp.number_of_passengers) as total_passengers
    from {{ ref('fact_pax') }} fp
    group by fp.flight_key
),
avg_pax_by_flight_number as (
    select
        f.flight_number,
        avg(pax.total_passengers) as avg_passengers
    from pax_by_flight pax
    join {{ ref('dim_flights') }} f on pax.flight_key = f.flight_key
    group by f.flight_number
)
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
    coalesce(pax.total_passengers, avg_pax.avg_passengers) as number_of_passengers,
    dp.item_id,
    dp.category,
    dp.price,
    coalesce(s.sold_quantity, 0) as sold_quantity,
    case
        when pax.total_passengers is null and avg_pax.avg_passengers is null then 'no_pax_data'
        when coalesce(pax.total_passengers, avg_pax.avg_passengers) = 0 then 'zero_pax_count'
        else null
    end as potential_error
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

left join pax_by_flight pax
    on pax.flight_key = f.flight_key

left join avg_pax_by_flight_number avg_pax
    on avg_pax.flight_number = f.flight_number

left join (
    select
        flight_key,
        product_key,
        count(slip_id) as tickets_count,
        sum(sold_quantity) as sold_quantity
    from {{ ref('fact_sales') }}
    group by flight_key, product_key
) s
    on s.flight_key = bfl.flight_key
        and s.product_key = fil.product_key