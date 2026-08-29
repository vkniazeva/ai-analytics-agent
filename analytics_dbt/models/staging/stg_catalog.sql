with source as (
    select * from {{ source('raw', 'product_catalog') }}
),
mapping as (
    select * from {{ ref('product_id_mapping') }}
),
renamed as (
    select
        "Reference" as original_item_id,
        "Status" as status,
        "Family" as item_category,
        "Food" as is_food,
        "Type" as item_type,
        "Selling Price" as price
    from source
),
    transformed as (
        select
            m.anonymized_reference as item_id,
            coalesce(r.status, 'UNKNOWN') as status,
            r.item_category as item_category,
            (r.is_food = 'YES') as is_food,
            coalesce(r.item_type, 'UNKNOWN') as item_type,
            r.price::decimal as price
        from renamed r
        inner join mapping m on r.original_item_id::text = m.original_reference
),
    cleaned as (
        select distinct *
        from transformed
        where item_id is not null
        and item_category is not null
        and price is not null
        and price > 0
    )
select * from cleaned