{#
    Use the schema configured on the model verbatim.

    dbt's default prefixes the target schema onto the configured one, which would
    turn `staging` into `gold_staging` and scatter the layers under whatever the
    profile's schema happens to be. The medallion layout is the point here, so the
    configured name wins and models land in `staging`, `intermediate` and `gold`.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
