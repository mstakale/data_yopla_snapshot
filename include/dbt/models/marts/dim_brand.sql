-- Grain: one row per normalised primary brand, plus one 'unknown' row for
-- products with no brand at all.
--
-- Primary brand = the first entry in the comma-separated brands field.
-- Normalised (trim, lowercase, collapse internal whitespace) for the key, so
-- e.g. "Albert Heijn" / "Albert heijn" / "albert heijn" collapse to one row;
-- the display name is whichever original spelling was most common for that
-- key (ties broken alphabetically).

with primary_brand as (

    select
        barcode,
        nullif(trim(regexp_replace(split_part(brands, ',', 1), '\s+', ' ', 'g')), '') as brand_raw
    from {{ ref('stg_products') }}

),

normalized as (

    select
        barcode,
        coalesce(brand_raw, 'unknown') as brand_raw,
        lower(coalesce(brand_raw, 'unknown')) as brand_key_source
    from primary_brand

),

spelling_counts as (

    select
        brand_key_source,
        brand_raw,
        count(*) as spelling_count
    from normalized
    group by brand_key_source, brand_raw

),

best_spelling as (

    select
        brand_key_source,
        brand_raw,
        row_number() over (
            partition by brand_key_source
            order by spelling_count desc, brand_raw asc
        ) as rn
    from spelling_counts

)

select
    {{ dbt_utils.generate_surrogate_key(['n.brand_key_source']) }} as brand_key,
    bs.brand_raw as brand_name,
    count(*) as product_count
from normalized n
inner join best_spelling bs
    on bs.brand_key_source = n.brand_key_source
    and bs.rn = 1
group by bs.brand_raw, n.brand_key_source
