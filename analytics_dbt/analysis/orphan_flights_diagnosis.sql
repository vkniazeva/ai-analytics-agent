-- Find flights in fact_sales that don't exist in dim_flights
-- Shows why orphans exist: missing flights vs hour mismatches

with orphan_flight_keys as (
    -- Get the orphan flight_keys from fact_sales
    select distinct fs.flight_key as orphan_key
    from {{ ref('fact_sales') }} fs
    left join {{ ref('dim_flights') }} df on fs.flight_key = df.flight_key
    where df.flight_key is null
    limit 50
),

sales_source as (
    -- Reconstruct what fact_sales did
    select
        flight_no,
        {{ date_to_key('date') }} as date_key,
        date,
        time,
        {{ round_hour_from_time('time') }} as calculated_hour,
        {{ dbt_utils.generate_surrogate_key(['flight_no', "{{ date_to_key('date') }}", "{{ round_hour_from_time('time') }}"]) }} as reconstructed_flight_key
    from {{ ref('stg_sales') }}
),

matched_sales as (
    -- Match orphans back to their source sales records
    select
        o.orphan_key,
        s.flight_no,
        s.date_key,
        s.time as sales_time,
        s.calculated_hour as sales_hour
    from orphan_flight_keys o
    inner join sales_source s on o.orphan_key = s.reconstructed_flight_key
),

int_flights_check as (
    -- Check if this flight exists in int_flights at all
    select
        flight_no,
        {{ date_to_key('date') }} as date_key,
        time as int_time,
        {{ round_hour_from_time('time') }} as int_hour,
        source,
        rn
    from {{ ref('int_flights') }}
),

dim_flights_check as (
    -- Check what hours exist in dim_flights for this flight+date
    select
        flight_number,
        date as date_key,
        hour_of_departure,
        time as dim_time,
        flight_key
    from {{ ref('dim_flights') }}
)

select
    ms.flight_no,
    ms.date_key,
    ms.sales_time,
    ms.sales_hour,

    -- From int_flights
    ifc.int_time,
    ifc.int_hour,
    ifc.source as int_source,

    -- From dim_flights
    dfc.dim_time,
    dfc.hour_of_departure as dim_hour,

    -- Diagnosis
    case
        when ifc.flight_no is null then '❌ Missing from int_flights entirely'
        when dfc.flight_number is null and ifc.flight_no is not null then '❌ In int_flights but missing from dim_flights'
        when ms.sales_hour != dfc.hour_of_departure then '⚠️  Hour mismatch (sales: ' || coalesce(ms.sales_hour::text, 'NULL') || ' vs dim: ' || coalesce(dfc.hour_of_departure::text, 'NULL') || ')'
        when ms.sales_hour >= 24 then '⚠️  Invalid hour >= 24'
        when ms.sales_hour is null then '⚠️  NULL hour from sales'
        else 'Unknown'
    end as issue_type,

    ms.orphan_key

from matched_sales ms
left join int_flights_check ifc
    on ms.flight_no = ifc.flight_no
    and ms.date_key = ifc.date_key
    and ifc.rn = 1
left join dim_flights_check dfc
    on ms.flight_no = dfc.flight_number
    and ms.date_key = dfc.date_key

order by issue_type, ms.flight_no, ms.date_key
