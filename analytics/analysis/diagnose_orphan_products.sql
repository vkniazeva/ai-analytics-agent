-- Check which products are missing from dim_products

with orphan_items as (
    select distinct
        product_key,
        load_id
    from {{ ref('fact_items_load') }}
    where product_key not in (select product_key from {{ ref('dim_products') }})
    limit 52
),
check_source as (
    select
        o.product_key,
        s.item_id,
        c.item_id as catalog_item_id,
        c.status
    from orphan_items o
    left join {{ ref('stg_items_load') }} s
        on {{ dbt_utils.generate_surrogate_key(['s.item_id']) }} = o.product_key
    left join {{ ref('stg_catalog') }} c
        on s.item_id = c.item_id
)
select
    product_key,
    item_id,
    catalog_item_id,
    status,
    case
        when catalog_item_id is null then '❌ Item not in catalog at all'
        when status is not null then '✓ Item exists in catalog (status: ' || status || ')'
        else 'Unknown'
    end as issue
from check_source
