-- Check why flights are missing from dim_flights
-- Sample from fact_items_load orphans

with orphan_items_load as (
    select distinct
        f.flight_key,
        -- Reverse engineer the flight details from the source
        s.flight_no,
        s.date,
        {{ round_hour_from_time('s.time') }} as items_load_hour
    from {{ ref('fact_items_load') }} f
    inner join {{ ref('stg_items_load') }} s
        on {{ dbt_utils.generate_surrogate_key(['s.flight_no', "{{ date_to_key('s.date') }}", "{{ round_hour_from_time('s.time') }}"]) }} = f.flight_key
    left join {{ ref('dim_flights') }} d on f.flight_key = d.flight_key
    where d.flight_key is null
    limit 100
),
check_int_flights as (
    select
        o.flight_no,
        o.date,
        o.items_load_hour,
        i.flight_no as int_flight_no,
        i.date as int_date,
        {{ round_hour_from_time('i.time') }} as int_hour,
        i.source,
        i.rn
    from orphan_items_load o
    left join {{ ref('int_flights') }} i
        on o.flight_no = i.flight_no
        and o.date = i.date
),
check_dim_flights as (
    select
        c.*,
        d.flight_number as dim_flight_no,
        d.date as dim_date,
        d.hour_of_departure as dim_hour
    from check_int_flights c
    left join {{ ref('dim_flights') }} d
        on c.flight_no = d.flight_number
        and {{ date_to_key('c.date') }} = d.date
)
select
    flight_no,
    {{ date_to_key('date') }} as date_key,
    items_load_hour,
    int_flight_no,
    int_hour,
    dim_flight_no,
    dim_hour,
    case
        when int_flight_no is null then '❌ Flight not in int_flights at all'
        when dim_flight_no is null then '❌ Flight not in dim_flights at all'
        when items_load_hour != dim_hour then '⚠️ Hour mismatch: items=' || items_load_hour::text || ' vs dim=' || dim_hour::text
        when items_load_hour is null then '⚠️ NULL hour in items_load'
        else 'Unknown'
    end as issue
from check_dim_flights
order by issue
limit 50
