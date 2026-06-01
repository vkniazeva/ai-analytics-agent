select *
from {{ref ('stg_payments')}}
where sales_type = 'Change Given'
    and payment_type = 'Cash'
    and purchase_amount != - purchase_amount_main