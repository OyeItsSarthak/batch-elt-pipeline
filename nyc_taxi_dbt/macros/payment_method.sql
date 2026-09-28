{% macro payment_method(payment_type_column) %}
case {{ payment_type_column }}
    when 1 then 'Credit Card'
    when 2 then 'Cash'
    when 3 then 'No Charge'
    when 4 then 'Dispute'
    when 5 then 'Unknown'
    when 6 then 'Voided Trip'
    else 'Other'
end
{% endmacro %}
