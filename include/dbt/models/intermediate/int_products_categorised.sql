-- One row per barcode. Resolves each product's category_tags to a single
-- category_l2/category_l1 using seeds/category_mapping.csv.
--
-- Tie-break rule when a product carries multiple mapped tags: lowest
-- priority number wins (a more specific tag, e.g. en:gouda -> cheese, beats
-- a broad umbrella tag, e.g. en:dairies -> milk_cream); if two candidate
-- tags share the same priority, the alphabetically first tag wins - a
-- deterministic, reproducible rule rather than "whichever the database
-- happens to return first."
--
-- mapping_status distinguishes three real, different situations rather than
-- collapsing them into one "other" bucket silently:
--   'no_categories' - the product has no category tags at all (raw data gap)
--   'unmapped'      - it has tags, but none are in category_mapping yet
--   'mapped'        - resolved via category_mapping (even if that maps to
--                     the 'other' category_l2 on purpose, e.g. en:groceries)

with candidates as (

    select
        sp.barcode,
        lower(tag) as tag,
        cm.category_l2,
        cm.priority
    from {{ ref('stg_products') }} sp
    cross join lateral jsonb_array_elements_text(sp.categories_tags) as tag
    inner join {{ ref('category_mapping') }} cm on lower(tag) = lower(cm.off_category_tag)
    where sp.categories_tags is not null

),

ranked as (

    select
        barcode,
        tag,
        category_l2,
        row_number() over (
            partition by barcode
            order by priority asc, tag asc
        ) as rn
    from candidates

),

best_match as (

    select barcode, tag as matched_tag, category_l2
    from ranked
    where rn = 1

)

select
    p.barcode,
    bm.matched_tag,
    coalesce(ct.category_l2, 'other') as category_l2,
    coalesce(ct.category_l1, 'other') as category_l1,
    case
        when p.categories_tags is null or p.categories_tags = '[]'::jsonb then 'no_categories'
        when bm.matched_tag is not null then 'mapped'
        else 'unmapped'
    end as mapping_status
from {{ ref('stg_products') }} p
left join best_match bm on p.barcode = bm.barcode
left join {{ ref('category_taxonomy') }} ct on bm.category_l2 = ct.category_l2
