{% macro clean_text(col) %}
    UPPER(TRIM(REGEXP_REPLACE({{ col }}, '\s+', ' ')))
{% endmacro %}




{% macro clean_links(col) %}
    LOWER(TRIM(REGEXP_REPLACE({{ col }}, '\s+', ' ')))
{% endmacro %}