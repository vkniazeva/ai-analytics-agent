with payments as (
    select distinct
        session_id, is_offline_mode
    from {{ ref( 'stg_payments')}}
),
sales as (
    select distinct
        session_id,
        cast (null as boolean) as is_offline_mode
    from {{ ref( 'stg_sales')}}
),
unioned as (
    select * from payments
    union all
    select * from sales
),
ranked as (
    select *,
           row_number() over (
               partition by session_id
               order by
                    case
                     when is_offline_mode=true then 1
                     when is_offline_mode=false then 2
                     else 3
                    end
               ) as rn
    from unioned)

select
        session_id,
       is_offline_mode
from ranked
where rn=1


