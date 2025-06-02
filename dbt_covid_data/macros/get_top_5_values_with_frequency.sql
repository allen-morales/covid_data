{% macro get_top_5_values_with_frequency(table_name) %}
{% set columns = adapter.get_columns_in_relation(ref(table_name)) %}

{% for column in columns %}
SELECT
    '{{ column.name }}' AS column_name,
    {{ column.name }} AS column_value,
    COUNT(*) AS value_count
FROM {{ ref(table_name) }}
GROUP BY {{ column.name }}
ORDER BY value_count DESC, {{ column.name }} DESC
LIMIT 5
{% if not loop.last %} UNION ALL {% endif %}
{% endfor %}
{% endmacro %}