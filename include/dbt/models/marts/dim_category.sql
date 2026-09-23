-- Grain: one row per category_l2.
select
    {{ dbt_utils.generate_surrogate_key(['category_l2']) }} as category_key,
    category_l2,
    category_l1,
    description
from {{ ref('category_taxonomy') }}
