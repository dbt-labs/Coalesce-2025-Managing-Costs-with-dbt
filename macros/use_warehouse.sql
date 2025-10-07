{% macro use_warehouse(wh) %}

  {% if wh %}
    
    {% set use_warehouse_sql %}
      USE WAREHOUSE {{ wh }};
    {% endset %}

    {% do run_query(use_warehouse_sql) %}
    
    {{ log("Successfully switched warehouse to: " ~ wh, info=true) }}

  {% else %}
    
    {{ log("No warehouse name provided. Skipping warehouse change.", warning=true) }}

  {% endif %}

{% endmacro %}
