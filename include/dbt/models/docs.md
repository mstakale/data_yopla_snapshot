{% docs nutrition_basis %}
Whether a product's per-100 nutrition figures are per 100g or per 100ml.

Resolved from Open Food Facts' own `nutrition_data_per` field when it's
unambiguous (`'100g'` or `'100ml'`) - this covers about 44,000 of the
109,767 products. For the rest (null, or `'serving'` - the majority, since
almost nothing in the dataset states `'100ml'` explicitly), this is a
**documented heuristic, not verified ground truth**: infer `100ml` when the
product's own quantity is reported in a liquid unit (ml/cl/l), otherwise
assume `100g`.
{% enddocs %}

{% docs category_priority_rule %}
How a product's `categories_tags` resolve to one `category_l2`.

Each Open Food Facts tag mapped in `category_mapping` carries a `priority`:
lower wins. A specific tag (e.g. `en:gouda` -> `cheese`, priority 1) beats a
broad umbrella tag (e.g. `en:dairies` -> `milk_cream`, priority 4) when a
product carries both. Ties (equal priority) are broken alphabetically by
tag, so the result is deterministic and reproducible rather than depending
on row order. Products with no matching tag resolve to `category_l2='other'`
(`mapping_status='unmapped'`); products with no tags at all also resolve to
`'other'` but are distinguished via `mapping_status='no_categories'`.
{% enddocs %}
