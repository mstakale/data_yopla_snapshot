{{ config(severity='warn', warn_if='>0', error_if='>600', description='fat + carbohydrates + proteins + fiber + salt per 100g <= 105g') }}

-- fat + carbohydrates + proteins + fiber + salt per 100g can't physically
-- exceed 100g by much (baseline: 212 of 34,754 products with all five
-- present go over 105g) - a looser bound than 100 to tolerate normal
-- rounding across independently-reported label values.

with pivoted as (

    select
        product_key,
        sum(value_per_100) filter (
            where nutrient in ('fat', 'carbohydrates', 'proteins', 'fiber', 'salt')
        ) as macro_sum,
        count(distinct nutrient) filter (
            where nutrient in ('fat', 'carbohydrates', 'proteins', 'fiber', 'salt')
        ) as n_present
    from {{ ref('fct_nutrition') }}
    group by product_key

)

select *
from pivoted
where n_present = 5
  and macro_sum > 105
