{% macro usd_to_gbp(column_name) -%}
ROUND(({{ column_name }} * 0.8),2 )
{%- endmacro %}