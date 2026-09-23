{{ validate_load_date_var() }}

with target_load_date as (

    -- dbt var load_date lets a specific partition be (re)built - e.g. for a
    -- backfill, or from the Airflow DAG, which always passes the run's date
    -- explicitly. With no var, defaults to the latest load_date so local
    -- `dbt build` still works unchanged.
    {% if var('load_date', none) is not none %}
    select '{{ var("load_date") }}'::date as load_date
    {% else %}
    select max(load_date) as load_date from {{ source('raw', 'products') }}
    {% endif %}

),

deduped as (

    select
        p.*,
        row_number() over (
            partition by p.barcode
            order by p.last_modified_t desc, p.loaded_at desc
        ) as rn
    from {{ source('raw', 'products') }} p
    inner join target_load_date using (load_date)

)

select
    barcode,
    nullif(trim(coalesce(
        (select elem->>'text' from jsonb_array_elements(product_name) elem where elem->>'lang' = 'nl' limit 1),
        (select elem->>'text' from jsonb_array_elements(product_name) elem where elem->>'lang' = 'en' limit 1),
        (select elem->>'text' from jsonb_array_elements(product_name) elem limit 1)
    )), '') as product_name,
    nullif(trim(brands), '') as brands,
    categories_tags,
    nutriments,
    nullif(trim(quantity), '') as quantity,
    nullif(trim(product_quantity_unit), '') as product_quantity_unit,
    nutrition_data_per,
    case
        when lower(trim(nutriscore_grade)) in ('a', 'b', 'c', 'd', 'e')
            then lower(trim(nutriscore_grade))
        else null
    end as nutriscore_grade,
    to_timestamp(last_modified_t) as last_modified_at,
    load_date
from deduped
where rn = 1
