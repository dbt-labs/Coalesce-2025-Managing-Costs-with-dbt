{%- macro drop_project_models(dry_run=True, prefix_to_exclude='stg_') -%}
    {{ log("Starting macro drop_project_models...", info=True) }}

    {# First, get the database and schema for the current dbt target #}
    {% set current_target_database = target.database %}
    {% set current_target_schema = target.schema %}

    {{ log("Target database: " ~ current_target_database, info=True) }}
    {{ log("Target schema: " ~ current_target_schema, info=True) }}
    {{ log("Excluding models with prefix: '" ~ prefix_to_exclude ~ "'", info=True) }}


    {% if dry_run %}
        {{ log("--- DRY RUN --- No objects will be dropped.", info=True) }}
    {% else %}
        {{ log("--- LIVE RUN --- Objects will be dropped.", info=True) }}
    {% endif %}

    {% set models_to_drop = [] %}

    {# Loop through all models in the dbt project graph #}
    {% for node in graph.nodes.values() | selectattr("resource_type", "equalto", "model") %}

        {# Check 1: Ensure the model is in the current target database and schema #}
        {% if node.database == current_target_database and node.schema == current_target_schema %}

            {# Check 2: Ensure the model name does NOT start with the specified prefix #}
            {% if not node.name.startswith(prefix_to_exclude) %}
                {% do models_to_drop.append(node) %}
            {% else %}
                {{ log("Skipping model with excluded prefix: " ~ node.name, info=True) }}
            {% endif %}

        {% endif %}

    {% endfor %}

    {% if models_to_drop | length > 0 %}
        {% for node in models_to_drop %}
            {% set object_type = 'table' if node.config.materialized != 'view' else 'view' %}
            {% set relation = adapter.get_relation(database=node.database, schema=node.schema, identifier=node.name) %}

            {% if relation %}
                {{ log("Found object to drop: " ~ relation ~ " (" ~ object_type ~ ")", info=True) }}
                {% set drop_sql = "DROP " ~ object_type ~ " IF EXISTS " ~ relation ~ ";" %}
                {{ log(" -> " ~ drop_sql, info=True) }}

                {% if not dry_run %}
                    {% do run_query(drop_sql) %}
                    {{ log(" -> DROPPED", info=True) }}
                {% endif %}
            {% else %}
                 {{ log("Could not find relation: " ~ node.database ~ "." ~ node.schema ~ "." ~ node.name, info=True) }}
            {% endif %}
        {% endfor %}
    {% else %}
        {{ log("No models found to drop in the target schema (after exclusions).", info=True) }}
    {% endif %}

{%- endmacro -%}