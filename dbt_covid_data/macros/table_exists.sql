{% macro table_exists(relation) %}
    {% if adapter.get_relation(database=relation.database, schema=relation.schema, identifier=relation.identifier) is not none %}
        True
    {% else %}
        False
    {% endif %}
{% endmacro %}