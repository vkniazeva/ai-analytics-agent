select
    il.flight_key as flight_key,
    df.flight_number as flight_number,
    df.hour_of_departure as hour_of_departure,
    dt.is_night as is_night,
    dt.day_period as day_period,
    dt.am_pm as am_pm,
    df.origin as origin,
    df.destination as destination,
    dd.date_key as date,
    dd.is_weekend as is_weekend,
    dd.week_day as week_day,
    il.load_id as load_id,
    fs.slip_id as slip_id,
    dp.item_id as item_id,
    dp.category as item_category,
    dp.price as item_price,
    il.total_loaded_quantity as loaded_quantity,
    fs.sold_quantity as sold_quantity,
    case
        when il.total_loaded_quantity = 0 and fs.sold_quantity > 0 then 'zero_load_positive_sale'
        else null
    end as potential_error
from {{ ref('fact_items_load') }} il
left join {{ ref('dim_flights') }} df on il.flight_key = df.flight_key
left join {{ ref('dim_products') }} dp on il.product_key = dp.product_key
left join {{ ref('fact_sales') }} fs on il.load_id = fs.load_id and il.product_key = fs.product_key
left join {{ ref('dim_date') }} dd on df.date = dd.date_key
left join {{ ref('dim_time') }} dt on df.hour_of_departure = dt.time_key
where dp.item_type='Fresh Product'
    and dp.status = 'Active'
    and fs.sales_type = 'Sale'
