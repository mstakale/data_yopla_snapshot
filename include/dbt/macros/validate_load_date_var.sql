{#
    If the load_date var is set, fail the compile clearly when it doesn't
    exist in raw.products, instead of letting the model silently build empty
    marts for a date with no data.
#}
{% macro validate_load_date_var() %}
    {%- set load_date_var = var('load_date', none) -%}
    {%- if load_date_var is not none and execute -%}
        {%- set check_sql -%}
            select count(*) as n from {{ source('raw', 'products') }} where load_date = '{{ load_date_var }}'::date
        {%- endset -%}
        {%- set results = run_query(check_sql) -%}
        {%- set row_count = results.columns[0].values()[0] -%}
        {%- if row_count == 0 -%}
            {{ exceptions.raise_compiler_error(
                "dbt var load_date='" ~ load_date_var ~ "' does not exist in raw.products - "
                "check the value, or omit --vars to use the latest load_date."
            ) }}
        {%- endif -%}
    {%- endif -%}
{% endmacro %}
