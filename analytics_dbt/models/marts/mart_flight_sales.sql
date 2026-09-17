with pax_by_flight as (
    select
        flight_key,
        sum(number_of_passengers) as total_pax
    from {{ ref('fact_pax') }}
    group by flight_key
)
select
    f.flight_number,
    f.origin || '-' || f.destination as route,
    dd.date,
    dd.is_weekend,
    sum(fs.purchase_amount) as total_sales,
    max(p.total_pax) as total_pax,
    sum(fs.purchase_amount)::numeric
        / nullif(max(p.total_pax), 0) as revenue_per_pax
from {{ ref('fact_sales') }} fs
join {{ ref('dim_flights') }} f
    on fs.flight_key = f.flight_key
join {{ ref('dim_date') }} dd
    on f.date = dd.date_key
left join pax_by_flight p
    on p.flight_key = f.flight_key
group by f.flight_number, f.origin, f.destination, dd.date, dd.is_weekend
