select *
from {{ ref('stg_sales')}}
where sales_type = 'Sale'
    and sold_quantity < 0