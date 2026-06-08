-- Quick check: what's missing?

-- 1. How many flights in each source vs dim_flights?
select 'stg_items_load' as source, count(distinct flight_no || date::text) as flight_count
from stg.stg_items_load
union all
select 'stg_sales', count(distinct flight_no || date::text)
from stg.stg_sales
union all
select 'stg_pax', count(distinct flight_no || date::text)
from stg.stg_pax
union all
select 'int_flights', count(distinct flight_no || date::text)
from int.int_flights
union all
select 'dim_flights', count(distinct flight_number || date::text)
from mart.dim_flights;

-- 2. Sample orphan products
select distinct
    f.product_key,
    'fact_items_load' as source
from mart.fact_items_load f
where not exists (select 1 from mart.dim_products p where p.product_key = f.product_key)
limit 10;

-- 3. Check fact_sales duplicates
select
    sale_transaction_key,
    count(*) as cnt
from mart.fact_sales
group by sale_transaction_key
having count(*) > 1
limit 10;
