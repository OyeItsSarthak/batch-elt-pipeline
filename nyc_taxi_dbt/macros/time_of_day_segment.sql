{% macro time_of_day_segment(timestamp_column) %}
case
    when extract(hour from {{ timestamp_column }}) between 7 and 9 then 'Morning Rush'
    when extract(hour from {{ timestamp_column }}) between 10 and 15 then 'Midday'
    when extract(hour from {{ timestamp_column }}) between 16 and 19 then 'Evening Rush'
    when extract(hour from {{ timestamp_column }}) between 20 and 23 then 'Night'
    else 'Late Night / Early Morning'
end
{% endmacro %}
