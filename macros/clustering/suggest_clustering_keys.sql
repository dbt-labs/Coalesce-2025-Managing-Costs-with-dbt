{% macro suggest_clustering_keys(model_name) %}
  {#--
    Orchestrates the analysis to suggest a clustering key for a given model.

    1. Calls get_clustering_cardinality_stats() to find structurally good candidates.
    2. For each candidate, calls get_column_usage_count() to find query history usage.
    3. For each candidate, calls calculate_clustering_score() to get a weighted score.
    4. Prints the top 3 recommendations.

    How to run:
    dbt run-operation suggest_clustering_keys --args '{model_name: your_model_name}'
  --#}

  {% if execute %}

    {% set model_relation = ref(model_name) %}

    {{ log("--- Step 1: Analyzing column cardinality for '" ~ model_relation ~ "' ---", info=true) }}

    {% set cardinality_results = get_clustering_cardinality_stats(model_relation) %}

    {% if not cardinality_results or cardinality_results | length == 0 %}
      {{ log("Could not generate any cardinality suggestions. All columns may have very low (<10) or very high (unique) cardinality.", warning=true) }}
      {{ return('') }}
    {% endif %}

    {% set total_rows = cardinality_results[0]['TOTAL_ROWS'] | int %}
    {{ log(model_relation ~ " has a total row count of " ~ total_rows, info=true )}}
    {{ log("--- Step 2: Analyzing column usage from Snowflake's query history (last 7 days) ---", info=true) }}

    {% set column_recommendations = [] %}

    {% for cand_row in cardinality_results %}
      {% set column_name = cand_row['COLUMN_NAME'] %}
      {{ log("... analyzing usage for " ~ column_name, info=true) }}

      {# Call macro to get usage #}
      {% set usage_count = get_column_usage_count(column_name, model_relation, days_to_check=7) %}

      {% set avg_rows = cand_row['AVG_ROWS_PER_VALUE'] | string %}

      {# Call macro to get score #}
      {% set recommendation_score = calculate_clustering_score(avg_rows, total_rows, usage_count) %}

      {% do column_recommendations.append({
          'column_name': column_name,
          'distinct_values': cand_row['DISTINCT_VALUES'],
          'usage_count': usage_count,
          'score': recommendation_score
      }) %}
    {% endfor %}

    {% set sorted_recommendations = column_recommendations | sort(attribute='score', reverse=true) %}

    {{ log("\n--- Top 3 Clustering Key Candidates for " ~ model_relation ~ " ---", info=true) }}
    {{ log("Sorted by a score combining cardinality and actual query usage.", info=true) }}

    {% for rec in sorted_recommendations %}
      {% if loop.index <= 3 %}
        {{ log("  - Candidate " ~ loop.index ~ ": " ~ rec.column_name ~ " (Score: " ~ (rec.score | round(2)) ~ ", Distinct: " ~ rec.distinct_values ~ ", Uses: " ~ rec.usage_count ~ ")", info=true) }}
      {% endif %}
    {% endfor %}

  {% endif %}

{% endmacro %}