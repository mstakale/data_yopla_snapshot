# Data contracts

ODCS v3 data contracts for the marts layer, documenting schema, quality
thresholds, ownership and service levels for consumers - independent of
dbt/Postgres specifics. See `fct_nutrition.yml` and the enforced dbt model
contract on `fct_nutrition` in `include/dbt/models/marts/_marts_models.yml`.

## Handling a breaking change

A breaking change (dropping a column, changing a type, changing the grain)
would bump this contract's `version` major number (e.g. `1.0.0` -> `2.0.0`)
and the corresponding dbt model's `contract.enforced` config would move to a
new dbt model version (`versions: [{v: 1, ...}, {v: 2, ...}]` in
`_marts_models.yml`), so `v1` and `v2` of `fct_nutrition` build and serve
side by side rather than the old shape disappearing outright. `v1` would be
marked with a `deprecation_date` giving consumers advance notice before it's
removed, rather than breaking them without warning. This is a plan for how a
future breaking change would be handled, not something implemented now.
