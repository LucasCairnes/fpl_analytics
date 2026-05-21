{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    
    {%- if custom_schema_name is none -%}
        {# If no schema is defined in dbt_project.yml, use the default target #}
        {{ default_schema }}
    {%- else -%}
        {# If a custom schema IS defined, use EXACTLY that name and nothing else #}
        {{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}