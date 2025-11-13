{% macro suggest_clustering_keys_legacy(model_name) %}

  {#--

    This macro performs a two-step analysis to suggest a clustering key.

    1. It analyzes column cardinality to find structurally good candidates, looking specifically for cardinality that is not too high and not too low.
    2. It queries Snowflake's query_history to find columns specifically used in joins/filters throughout your query history.

    How to run:
    dbt run-operation suggest_clustering_keys --args '{model_name: your_model_name}'
    
    NOTE: The role running this macro must have privileges to query the
    SNOWFLAKE.ACCOUNT_USAGE schema. Data in these views can have some latency.

    NOTE 2: For performance reasons, we are only looking back at queries for the past 7 days. You can extend this further. 

  --#}

  {% if execute %}

    {{ log("--- Step 1: Analyzing column cardinality for '" ~ model_name ~ "' ---", info=true) }}

    {% set model_relation = ref(model_name) %}
    
    {# Query to get cardinality stats for each column #}
    {% set cardinality_sql %}
      with column_stats as (
        {% for column in adapter.get_columns_in_relation(model_relation) %}
          select
            '{{ column.name | upper }}' as column_name,
            approx_count_distinct({{ adapter.quote(column.name) }}) as distinct_values
          from {{ model_relation }}
          {% if not loop.last %}union all{% endif %}
        {% endfor %}
      ),
      table_stats as (
        select count(*) as total_rows from {{ model_relation }}
      )
      select
        cs.column_name,
        cs.distinct_values,
        ts.total_rows,
        DIV0(ts.total_rows, cs.distinct_values) as avg_rows_per_value
      from column_stats cs
      cross join table_stats ts
      where cs.distinct_values < ts.total_rows -- Exclude unique keys
        and cs.distinct_values > 10 -- Exclude very low cardinality columns
    {% endset %}

    {% set cardinality_results = run_query(cardinality_sql) %}

    {% if not cardinality_results or cardinality_results | length == 0 %}
      {{ log("Could not generate any cardinality suggestions. All columns may have very low (<10) or very high (unique) cardinality.", warning=true) }}
    {% else %}

      {{ log("--- Step 2: Analyzing column usage from Snowflake's query history ---", info=true) }}

      {% set column_recommendations = [] %}

      {% for cand_row in cardinality_results %}
        {% set column_name = cand_row['COLUMN_NAME'] %}
        
        {# For each candidate, query the history to find its usage #}
        {% set usage_sql %}
          select
            count(*) as usage_count
          from snowflake.account_usage.query_history
          where start_time >= dateadd('day', -7, current_timestamp())
            and (
              query_text ilike '%JOIN%ON%{{ column_name }}%'
              or query_text ilike '%WHERE%{{ column_name }}%'
            )
            and query_text ilike '%{{ model_relation.identifier }}%'
        {% endset %}

        {% set usage_results = run_query(usage_sql) %}
        {% set usage_count = usage_results.columns[0].values()[0] if usage_results else 0 %}

        {% set avg_rows = (cand_row['AVG_ROWS_PER_VALUE'] | string) | float %}
        {% set total_rows = (cand_row['TOTAL_ROWS'] | string) | float %}
        {% set recommendation_score = 0 %}

        {% if total_rows > 0 %}
            {% set cardinality_pct_score = (avg_rows / total_rows) * 100 %}
            {# Give a heavy weighting to columns that are actually used in queries #}
            {% set recommendation_score = cardinality_pct_score + (usage_count * 20) %}
        {% endif %}

        {% do column_recommendations.append({
            'column_name': column_name,
            'distinct_values': cand_row['DISTINCT_VALUES'],
            'usage_count': usage_count,
            'score': recommendation_score
        }) %}
      {% endfor %}

      {% set sorted_recommendations = column_recommendations | sort(attribute='score', reverse=true) %}

      {{ log("\n--- Top 3 Clustering Key Candidates for " ~ model_relation ~ " ---", info=true) }}
      {{ log("Sorted by a score combining cardinality and actual query usage from the last 30 days.", info=true) }}
      {{ log(model_relation ~ " has a total row count of " ~ total_rows, info=true )}}
      {% for rec in sorted_recommendations %}
        {% if loop.index <= 3 %}
          {{ log("  - Candidate " ~ loop.index ~ ": " ~ rec.column_name ~ " (Distinct Values: " ~ rec.distinct_values ~ ", Recent JOIN/WHERE Uses: " ~ rec.usage_count ~ ")", info=true) }}
        {% endif %}
      {% endfor %}

    {% endif %}
  {% endif %}

{% endmacro %}

