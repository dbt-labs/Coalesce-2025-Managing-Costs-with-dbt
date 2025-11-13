{% macro calculate_clustering_score(avg_rows, total_rows, usage_count) %}
  {#--
    Calculates a recommendation score based on cardinality and usage.
    Gives a heavy weighting to columns that are actually used in queries.
  --#}
  {% set recommendation_score = 0 %}
  {% set avg_rows = avg_rows | float %}
  {% set total_rows = total_rows | float %}
  {% set usage_count = usage_count | int %}

  {% if total_rows > 0 %}
      {# Calculate cardinality score as a percentage of total rows #}
      {% set cardinality_pct_score = (avg_rows / total_rows) * 100 %}

      {# Add weighted usage score. (Each use is worth 20 points) #}
      {% set recommendation_score = cardinality_pct_score + (usage_count * 20) %}
  {% endif %}

  {{ return(recommendation_score) }}

{% endmacro %}