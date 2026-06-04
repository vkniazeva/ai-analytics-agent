select
    {{dbt_utils.generate_surrogate_key( [
        'item_id',
        'price'
    ] ) }} as product_key,
    item_id as item_id,
    status as status,
    item_category as category,
    is_food as is_food,
    item_type as item_type,
    price as price
from {{ ref ('stg_catalog')}}