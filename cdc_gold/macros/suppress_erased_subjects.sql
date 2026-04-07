{% macro suppress_erased_subjects(subject_id_col, subject_type) %}
    {{ subject_id_col }} NOT IN (
        SELECT subject_id
        FROM {{ source('monitoring', 'erasure_registry') }}
        WHERE subject_type = '{{ subject_type }}'
    )
{% endmacro %}
