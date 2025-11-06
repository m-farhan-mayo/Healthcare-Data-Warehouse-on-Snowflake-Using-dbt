{% macro scd2_rn(key='id', start_col='record_start') %}
    row_number() over(partition by {{ key }} order by {{ start_col }} desc) as rn
{% endmacro %}