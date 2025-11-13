{% macro get_clustering_cardinality_stats(model_relation) %}
  {#--
    Queries the given relation to get cardinality statistics for each column.
    Filters out columns that are poor clustering candidates (e.g., unique keys
    or very low cardinality keys).

    Returns an Agate table with:
    - column_name
    - distinct_values
    - total_rows
    - avg_rows_per_value
  --#}

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
    order by distinct_values desc
  {% endset %}

  {% set cardinality_results = run_query(cardinality_sql) %}

  {{ return(cardinality_results) }}

{% endmacro %}