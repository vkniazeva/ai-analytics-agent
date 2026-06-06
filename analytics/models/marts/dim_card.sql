select distinct
    p.card_number_prefix as card_number_prefix,
       b.brand as card_brand,
       b.type as reference_card_type,
       b.category as card_category,
       b.issuer as card_issuer,
       b.country_short as issuer_country
from {{ ref('stg_payments') }} p
         left join {{ ref('stg_bank') }} b on cast(p.card_number_prefix as varchar) = cast(b.card_number_prefix as varchar)
where p.card_number_prefix is not null
