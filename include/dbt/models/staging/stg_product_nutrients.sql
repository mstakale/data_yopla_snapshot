-- Long format, one row per barcode + nutrient, built on stg_products (not the
-- raw source directly) so "latest load of deduplicated products" is inherited
-- for free instead of recomputed here.
--
-- The nutriments array also carries non-nutrient entries (nova-group,
-- nutrition-score-fr, *-estimate-from-ingredients, alcohol as "% vol") and a
-- few inconsistent/unusable units (%, IU, mcg, empty, null). Restricting unit
-- to g/mg/kcal/kJ keeps only real per-100 mass/energy nutrients and drops all
-- of those naturally, rather than relying on an implicit side effect.

with unnested as (

    select
        barcode,
        elem->>'name' as nutrient,
        (elem->>'100g')::numeric as value_per_100,
        case
            when lower(elem->>'unit') = 'kj' then 'kJ'
            else elem->>'unit'
        end as unit
    from {{ ref('stg_products') }}
    cross join lateral jsonb_array_elements(nutriments) as elem
    where nutriments is not null

)

select
    barcode,
    nutrient,
    value_per_100,
    unit
from unnested
where value_per_100 is not null
  and lower(unit) in ('g', 'mg', 'kcal', 'kj')
