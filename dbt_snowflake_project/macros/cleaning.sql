{% macro text_clean(column) %}
    initcap(trim({{ column }}))
{% endmacro %}

{% macro phone_clean(column) %}
    regexp_replace({{ column }}, '[^0-9+]', '')
{% endmacro %}

{% macro convert_date(column) %}
    try_cast({{ column }} as date)
{% endmacro %}