{% macro get_partition_date(y='year', m='month', d='day') %}
    cast(date_parse(concat({{ y }}, '-', {{ m }}, '-', {{ d }}), '%Y-%m-%d') as date) as partition_date
{% endmacro %}