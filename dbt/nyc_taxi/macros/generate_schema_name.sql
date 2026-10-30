-- Use a folder's +schema verbatim (staging, marts, ...) instead of dbt's default
-- <target.schema>_<custom> (public_staging). Nodes without +schema fall back
-- to the profile's schema.
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
