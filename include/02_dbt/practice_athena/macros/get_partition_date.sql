{% macro get_partition_date(y='year', m='month', d='day') %}
    cast(date_parse(concat(cast({{ y }} as varchar), '-', cast({{ m }} as varchar), '-', cast({{ d }} as varchar)), '%Y-%m-%d') as date)
{% endmacro %}