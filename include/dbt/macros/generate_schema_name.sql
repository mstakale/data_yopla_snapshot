{#
    dbt's default generate_schema_name concatenates target.schema with the
    model's configured schema (e.g. "dev_marts"), which exists so multiple
    developers can share one database without colliding. We have one
    dedicated warehouse with four fixed schemas (raw/staging/intermediate/
    marts, created in warehouse/init/01_schemas.sql), so the concatenated
    name would put every model in the wrong place. This override makes the
    model's configured schema authoritative.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
