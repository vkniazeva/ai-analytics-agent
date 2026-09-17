select
    dp.item_id,
    dp.category,
    dp.item_type,
    dp.is_food,
    sum(fs.purchase_amount) as total_sales,
    sum(fs.sold_quantity) as quantity_sold,
    count(distinct fs.slip_id) as total_transactions,
    sum(fs.sold_quantity)::numeric
        / nullif(count(distinct fs.slip_id), 0) as attach_rate
from {{ ref('fact_sales') }} fs
join {{ ref('dim_products') }} dp
    on fs.product_key = dp.product_key
group by dp.item_id, dp.category, dp.item_type, dp.is_food
