-- Summary of why orphan flights exist
-- Quick overview before diving into details

with orphan_flight_keys as (
    select distinct fs.flight_key
    from {{ ref('fact_sales') }} fs
    left join {{ ref('dim_flights') }} df on fs.flight_key = df.flight_key
    where df.flight_key is null
),

sales_with_orphans as (
    select
        s.flight_no,
        {{ date_to_key('s.date') }} as date_key,
        {{ round_hour_from_time('s.time') }} as sales_hour,
        {{ dbt_utils.generate_surrogate_key(['s.flight_no', "{{ date_to_key('s.date') }}", "{{ round_hour_from_time('s.time') }}"]) }} as flight_key
    from {{ ref('stg_sales') }} s
    inner join orphan_flight_keys o on
        {{ dbt_utils.generate_surrogate_key(['s.flight_no', "{{ date_to_key('s.date') }}", "{{ round_hour_from_time('s.time') }}"]) }} = o.flight_key
)

select
    case
        when s.sales_hour >= 24 then 'Hour >= 24 (time rounding issue)'
        when s.sales_hour is null then 'NULL hour'
        when i.flight_no is null then 'Flight missing from int_flights'
        when d.flight_number is not null then 'Exists in dim but wrong hour'
        else 'Other issue'
    end as issue_category,
    count(*) as orphan_count,
    count(distinct s.flight_no || '-' || s.date_key) as unique_flights

from sales_with_orphans s
left join {{ ref('int_flights') }} i
    on s.flight_no = i.flight_no
    and s.date_key = {{ date_to_key('i.date') }}
    and i.rn = 1
left join {{ ref('dim_flights') }} d
    on s.flight_no = d.flight_number
    and s.date_key = d.date

group by 1
order by 2 desc
