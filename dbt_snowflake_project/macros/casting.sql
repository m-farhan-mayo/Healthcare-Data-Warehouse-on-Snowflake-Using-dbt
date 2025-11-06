{% macro cast_string(column) %}
    cast({{ column }} as string)
{% endmacro %}

{% macro cast_numeric(column) %}
    cast({{ column }} as numeric(18,2))
{% endmacro %}