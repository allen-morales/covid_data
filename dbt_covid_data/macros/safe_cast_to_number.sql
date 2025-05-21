{% macro safe_cast_to_number(column_name) %}
    CASE 
        WHEN typeof({{ column_name }}) = 'INTEGER' OR typeof({{ column_name }}) = 'BIGINT' THEN CAST({{ column_name }} AS INT)
        WHEN {{ column_name }} IS NOT NULL AND {{ column_name }} != 'undefined' AND {{ column_name }} != '' THEN CAST({{ column_name }} AS INT)
        ELSE NULL
    END
{% endmacro %}