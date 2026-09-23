{{ config(severity='warn', warn_if='>0', error_if='>2000', description='salt approximately equals sodium x 2.5, within tolerance') }}

-- Salt is chemically ~2.5x sodium by mass. Products report both
-- independently, so some mismatch is expected upstream noise (baseline:
-- 699 of 48,994 products with both values, at a 15%-or-0.02g tolerance) -
-- not a sign our own pipeline is wrong.

with pivoted as (

    select
        product_key,
        max(value_per_100) filter (where nutrient = 'salt') as salt,
        max(value_per_100) filter (where nutrient = 'sodium') as sodium
    from {{ ref('fct_nutrition') }}
    where nutrient in ('salt', 'sodium')
    group by product_key

)

select *
from pivoted
where salt is not null
  and sodium is not null
  and abs(salt - sodium * 2.5) > greatest(0.15 * salt, 0.02)
