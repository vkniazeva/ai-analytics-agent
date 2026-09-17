select
    dd.date,
    dd.month,
    dd.is_weekend,
    sum(fs.purchase_amount) as total_sales,
    count(distinct fs.slip_id) as total_transactions,
    sum(fs.sold_quantity) as total_items,
    sum(fs.purchase_amount)::numeric
        / nullif(sum(fs.sold_quantity), 0) as avg_item_price,
    sum(fs.purchase_amount)::numeric
        / nullif(count(distinct fs.slip_id), 0) as avg_check
from {{ ref('fact_sales') }} fs
join {{ ref('dim_flights') }} f
    on fs.flight_key = f.flight_key
join {{ ref('dim_date') }} dd
    on f.date = dd.date_key
group by dd.date, dd.month, dd.is_weekend
