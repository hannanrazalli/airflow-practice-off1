{% macro audit_columns(layer) %}
    {% if layer == 'staging' %}
        cast(current_timestamp as timestamp) as _staged_at,
        '{{ invocation_id }}' as _batch_id_stage
    {% elif layer == 'marts' %}
        cast(current_timestamp as timestamp) as _refined_at,
        '{{ invocation_id }}' as _batch_id_marts
    {% endif %}
{% endmacro %}