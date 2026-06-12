select *
from {{ref ('stg_sales')}}
where sales_type = 'Sale'
    and purchase_amount < 0