-- Grain: one row per product_key + nutrient.
--
-- Unit standardisation: for mass nutrients (is_mass=true in the seed) the
-- value is already gram-equivalent regardless of what the source's raw unit
-- label said (verified during planning - e.g. a sodium row can say
-- unit='mg' while the number is already in grams), so standardising unit is
-- just a relabel to nutrient_reference.standard_unit, not a conversion. The
-- one real exception is the generic 'energy' nutrient, which genuinely
-- appears in both kJ and kcal as different numbers for the same concept -
-- that one gets an actual kcal-to-kJ conversion (x4.184).
--
-- Nutrients not listed in nutrient_reference are dropped via the inner join
-- (see verification query for the exact count). Negative values are
-- excluded entirely, not nulled - a fact row is supposed to represent a real
-- measurement, and a physically-impossible negative value isn't one.

select
    dp.product_key,
    spn.nutrient,
    case
        when spn.nutrient = 'energy' and lower(spn.unit) = 'kcal' then spn.value_per_100 * 4.184
        else spn.value_per_100
    end as value_per_100,
    nr.standard_unit as unit,
    dp.nutrition_basis as basis,
    dp.category_key,
    dp.brand_key
from {{ ref('stg_product_nutrients') }} spn
inner join {{ ref('nutrient_reference') }} nr on lower(nr.nutrient) = lower(spn.nutrient)
inner join {{ ref('dim_product') }} dp on dp.product_key = spn.barcode
where spn.value_per_100 >= 0
