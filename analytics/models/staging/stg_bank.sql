with source       as (
    select * from {{ source('raw', 'bank') }}
),

renamed as (
   select
       "bin"            as card_number_prefix,
       "brand"          as brand,
       "type"           as type,
       "category"       as category,
       "issuer"         as issuer,
       "alpha_3"        as country_short,
       "country"        as country
    from source
),
transformed as (
    select
       r.card_number_prefix,
       COALESCE(r.brand, 'UNKNOWN') as brand,
       COALESCE(r.type, 'UNKNOWN') as type,
       COALESCE(r.category, 'UNKNOWN') as category,
       COALESCE(r.issuer, 'UNKNOWN') as issuer,
       COALESCE(r.country_short, 'UNKNOWN') as country_short,
       COALESCE(r.country, 'UNKNOWN') as country
    from renamed  r
),
cleaned as (
   select distinct *
   from transformed
   where  card_number_prefix   is not null
)

select * from cleaned
