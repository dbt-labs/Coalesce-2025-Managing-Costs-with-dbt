{% macro get_column_usage_count(column_name, model_relation, days_to_check=7) %}
  {#--
    Queries SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY to find how many times
    a column was used in a JOIN or WHERE clause for a specific model.

    NOTE: The role running this macro must have privileges
    on SNOWFLAKE.ACCOUNT_USAGE.
  --#}

  {% set usage_sql %}
    select
      count(*) as usage_count
    from snowflake.account_usage.query_history
    where start_time >= dateadd('day', -{{ days_to_check }}, current_timestamp())
      and (
        query_text ilike '%JOIN%ON%{{ column_name }}%'
        or query_text ilike '%WHERE%{{ column_name }}%'
      )
      and query_text ilike '%{{ model_relation.identifier }}%'
  {% endset %}

  {% set usage_results = run_query(usage_sql) %}

  {% set usage_count = usage_results.columns[0].values()[0] if usage_results else 0 %}

  {{ return(usage_count | int) }}

{% endmacro %}