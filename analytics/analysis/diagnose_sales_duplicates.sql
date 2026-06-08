-- Find fact_sales duplicate keys and understand why

select
    sale_transaction_key,
    count(*) as duplicate_count,
    count(distinct flight_key) as unique_flights,
    count(distinct product_key) as unique_products,
    count(distinct sales_type) as unique_sales_types,
    count(distinct sales_session_id) as unique_sessions
from {{ ref('fact_sales') }}
group by sale_transaction_key
having count(*) > 1
order by duplicate_count desc
limit 20
