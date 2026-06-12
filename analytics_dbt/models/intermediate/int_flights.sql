with schedule as (
    select distinct
        flight_no,
        origin,
        destination,
        date,
        time,
        load_id,
        'SCHEDULE' as source
    from {{ ref('stg_schedule')}}
    ),

wastage as (
    select distinct
        flight_no,
        origin,
        destination,
        date,
        cast (null as time) as time,
        load_id,
        'WASTAGE' as source
    from {{ ref ('stg_wastage')}}
    ),

sales as (
        select distinct
        flight_no,
        origin,
        destination,
        date,
        time,
        load_id,
        'SALES' as source
    from {{ ref ('stg_sales')}}
        ),

payments as (
        select distinct
        flight_no,
        origin,
        destination,
        date,
        time,
        load_id,
        'PAYMENTS' as source
     from {{ ref ('stg_payments')}}
        ),

pax as (
        select distinct
        flight_no,
        origin,
        destination,
        date,
        time,
        cast(null as int) as load_id ,
        'PAX' as source
     from {{ ref ('stg_pax')}}
        ),

items_load as (
    select distinct
        il.flight_no,
        coalesce(s.origin, 'UNKNOWN') as origin,
        coalesce(s.destination, 'UNKNOWN') as destination,
        il.date,
        il.time,
        il.load_id,
        'ITEMS_LOAD' as source
    from {{ ref('stg_items_load')}} il
    left join {{ ref('stg_schedule')}} s
        on il.flight_no = s.flight_no
        and il.date = s.date
        and il.load_id = s.load_id
    ),

    unioned as (
        select * from schedule
        union all
        select * from items_load
        union all
        select * from wastage
        union all
        select * from sales
        union all
        select * from payments
        union all
        select * from pax
    ),

    ranked as (
        select *,
            row_number() over (
                partition by flight_no, date
                order by
                    case when source='SCHEDULE' then 1
                         when origin != 'UNKNOWN' and destination != 'UNKNOWN' then 2
                         else 3
                    end,
                    case when time is not null then 1 else 2 end
            ) as rn
    from unioned
    )

    select *
    from ranked
    where rn = 1
