{{ config(materialization='table') }}

with series as (
    {{ dbt_utils.generate_series(upper_bound=24) }}
)

select
    generated_number - 1 as hour
from series
