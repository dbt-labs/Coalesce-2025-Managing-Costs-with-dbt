{% macro get_models_by_tag(target_tag, warehouse) %}
    {% set tagged_models = [] %}
    {% set pre_hook = "use warehouse " ~ warehouse ~ ";" %}

    {% if execute %}
    {# Iterate through all nodes in the dbt graph #}
    {% for node in graph.nodes.values() %}
        {# Check if the node is a model and if its tags include the target_tag #}
        {% if node.resource_type == "model" and target_tag in node.tags %}
            {# Add the model's identifier (e.g., package_name.model_name) to the list #}
            {% do tagged_models.append(node.name) %}
        {% endif %}
    {% endfor %}

    {% for model_name in tagged_models %}
        {# Perform actions on each model, e.g., print its name or execute a query against it #}
        {{ log("Processing model: " ~ model_name, info=True) }}
    {% endfor %}

    {% if this.name in tagged_models %}
        {{ log(pre_hook, info=True) }}
        {# Return the pre_hook statement #}
        {{ pre_hook }}
    {% endif %}
    {% endif %}

{% endmacro %}
