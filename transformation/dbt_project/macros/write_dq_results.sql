{#
  on-run-end macro: emit dbt test results to monitoring.dq_results.
  Add to dbt_project.yml:
    on-run-end:
      - "{{ write_dq_results() }}"
#}
{% macro write_dq_results() %}
    {% if execute %}
        {% set run_id = invocation_id %}
        {% for result in results %}
            {% if result.node.resource_type == 'test' %}
                {% set status    = result.status | upper %}
                {% set tbl_name  = result.node.attached_node | default('unknown') %}
                {% set chk_name  = result.node.name %}
                {% set obs_val   = result.failures | float if result.failures is not none else 0 %}

                {% set insert_sql %}
                    INSERT INTO workspace.monitoring.dq_results
                    (run_id, layer, table_name, check_name, status, observed_value, threshold, message, checked_at)
                    VALUES (
                        '{{ run_id }}',
                        'gold',
                        '{{ tbl_name }}',
                        '{{ chk_name }}',
                        '{{ status }}',
                        {{ obs_val }},
                        0,
                        {% if status != 'PASS' %}'{{ chk_name }} {{ status }}: {{ obs_val }} failures'{% else %}NULL{% endif %},
                        current_timestamp()
                    )
                {% endset %}
                {% do run_query(insert_sql) %}
            {% endif %}
        {% endfor %}
    {% endif %}
{% endmacro %}
