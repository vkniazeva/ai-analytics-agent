select *
from {{ref ('stg_payments')}}
where sales_type = 'Sale'
    and purchase_amount < 0