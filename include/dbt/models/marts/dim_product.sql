-- Grain: one row per barcode.
--
-- brand_key is recomputed here with the exact same normalisation pipeline as
-- dim_brand.sql (trim, lowercase, collapse whitespace, 'unknown' fallback),
-- so the surrogate key hash matches deterministically without needing
-- dim_brand to expose its internal normalised text.

with brand_lookup as (

    select
        barcode,
        lower(coalesce(
            nullif(trim(regexp_replace(split_part(brands, ',', 1), '\s+', ' ', 'g')), ''),
            'unknown'
        )) as brand_key_source
    from {{ ref('stg_products') }}

)

select
    sp.barcode as product_key,
    sp.product_name,
    sp.quantity,
    sp.nutriscore_grade,
    -- nutrition_data_per (Open Food Facts' own field, added Day 3C) tells us
    -- the basis directly for products where it's '100g' or '100ml'. For the
    -- rest (null or 'serving' - the majority, since '100ml' is stated
    -- explicitly on almost no rows) this is a documented HEURISTIC, not
    -- verified ground truth: infer 100ml for products whose own quantity is
    -- reported in a liquid unit, else assume 100g.
    case
        when sp.nutrition_data_per = '100ml' then '100ml'
        when sp.nutrition_data_per = '100g' then '100g'
        when lower(sp.product_quantity_unit) in ('ml', 'cl', 'l') then '100ml'
        else '100g'
    end as nutrition_basis,
    sp.last_modified_at,
    {{ dbt_utils.generate_surrogate_key(['bl.brand_key_source']) }} as brand_key,
    dc.category_key,
    ipc.mapping_status
from {{ ref('stg_products') }} sp
inner join brand_lookup bl on bl.barcode = sp.barcode
left join {{ ref('int_products_categorised') }} ipc on ipc.barcode = sp.barcode
left join {{ ref('dim_category') }} dc on dc.category_l2 = ipc.category_l2
