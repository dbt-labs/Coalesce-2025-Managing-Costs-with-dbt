{% macro suggest_warehouse_size(model_name) %}
  {#--
    This macro analyzes a model's complexity and input data size to suggest
    a starting Snowflake warehouse size.

    How to run:
    1. First, compile your project: dbt compile
    2. Then, run the operation: dbt run-operation suggest_warehouse_size --args '{model_name: your_model_name}'
  --#}

  {% if execute %}
    {{ log("--- Analyzing warehouse requirements for model '" ~ model_name ~ "' ---", info=true) }}

    {# Step 1: Find the model in the project graph #}
    {% set model_node = [] %}
    {% for node in graph.nodes.values() | selectattr('resource_type', 'equalto', 'model') %}
      {% if node.name == model_name %}
        {% do model_node.append(node) %}
      {% endif %}
    {% endfor %}

    {% if not model_node %}
      {{ log("Error: Could not find model '" ~ model_name ~ "' in the project.", info=true) }}
      {{ return('') }}
    {% endif %}

    {# Step 2: Calculate Complexity Score from compiled SQL #}
    {% set compiled_sql = model_node[0].compiled_sql | upper %}
    {% if not compiled_sql or '--placeholder--' in compiled_sql %}
        {{ log("Warning: Could not get compiled SQL for this model. Run 'dbt compile' first for a more accurate analysis.", info=true) }}
        {{ return('') }}
    {% endif %}

    {% set joins = compiled_sql.count('JOIN ') %}
    {% set group_bys = compiled_sql.count('GROUP BY') %}
    {% set window_functions = compiled_sql.count('OVER (') %}
    {% set complexity_score = (joins * 2) + (group_bys * 3) + (window_functions * 5) %}

    {{ log("  - Complexity Score: " ~ complexity_score ~ " (based on " ~ joins ~ " joins, " ~ group_bys ~ " aggregations, " ~ window_functions ~ " window functions)", info=true) }}

    {# Step 3: Calculate total size of upstream tables #}
    {% set total_input_gb = 0 %}
    {% if model_node.depends_on.nodes | length > 0 %}
      {% set upstream_tables_sql %}
        SELECT
          SUM(bytes) / POWER(1024, 3) AS total_gb
        FROM snowflake.account_usage.tables
        WHERE DELETED IS NULL
          AND table_name IN (
            {% for upstream_node_unique_id in model_node.depends_on.nodes %}
              {% set upstream_node = graph.nodes[upstream_node_unique_id] %}
              '{{ upstream_node.name | upper }}'
              {% if not loop.last %},{% endif %}
            {% endfor %}
          )
      {% endset %}
      {% set upstream_size_result = run_query(upstream_tables_sql) %}
      {% set total_input_gb = upstream_size_result.columns[0].values()[0] if upstream_size_result else 0 %}
    {% endif %}
    
    {% set total_input_gb = total_input_gb | round(2) %}
    {{ log("  - Total Input Size: " ~ total_input_gb ~ " GB (from upstream tables)", info=true) }}

    {# Step 4: Recommend warehouse size based on scores #}
    {% set recommendation = 'XSMALL' %}
    {% if complexity_score > 30 or total_input_gb > 50 %}
      {% set recommendation = 'LARGE' %}
    {% elif complexity_score > 15 or total_input_gb > 10 %}
      {% set recommendation = 'MEDIUM' %}
    {% elif complexity_score > 5 or total_input_gb > 1 %}
      {% set recommendation = 'SMALL' %}
    {% endif %}

    {{ log("\n--- Recommendation ---", info=true) }}
    {{ log("Based on the model's complexity and input data size, the suggested starting warehouse size is: " ~ recommendation, info=true) }}
    {{ log("NOTE: This is a heuristic. Always test performance on a sample of data.", info=true) }}

  {% endif %}
{% endmacro %}
