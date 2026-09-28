{% macro is_weekend(timestamp_column) %}
lower(dayname({{ timestamp_column }})) in ('saturday', 'sunday')
{% endmacro %}
