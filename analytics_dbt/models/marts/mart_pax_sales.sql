with sales as (
    select
    flight_key,
    count(distinct slip_id) as number_of_tickets,
    sum(sold_quantity) as sold_quantity,
    sum(purchase_amount) as purchase_amount
    from {{ ref('fact_sales') }}
    group by flight_key)
select
    df.flight_key,
    df.flight_number,
    dd.date,
    dd.year,
    dd.month,
    dd.day,
    dd.week_day,
    dd.weekday_name,
    dd.is_weekend,

    df.origin as origin,
    df.destination as destination,
    df.origin || ' _ ' || df.destination as route,
    dt.day_period,
    dt.am_pm,
    dt.is_night,
    sum(fp.number_of_passengers) as passenger_count,
    sum(sales.number_of_tickets) as number_of_tickets,
    sum(sales.sold_quantity) as sold_quantity,
    sum(sales.purchase_amount) as purchase_amount
from sales
         join {{ ref('fact_pax') }} fp on fp.flight_key = sales.flight_key
         join {{ ref('dim_flights') }} df on df.flight_key = fp.flight_key
         join {{ ref('dim_time') }} dt on dt.time_key = df.hour_of_departure
         join {{ ref('dim_date') }} dd on dd.date_key = df.date
group by df.flight_key, df.flight_number, dd.date, df.origin, df.destination, dd.year, dd.month, dd.day, dd.week_day, dd.weekday_name, dd.is_weekend, dt.day_period, dt.am_pm, dt.is_night



