{% macro get_warehouse_by_tags() %}
    {%- set tag_to_warehouse = {
        'test': 'test_wh',
        'prod': 'prod_wh',
        'dev': 'dev_wh'
    } -%}

    {%- set model_name = this.name -%}
    {%- set model_tags = graph.nodes[model_name].tags -%}

    {%- for tag in model_tags -%}
        {%- if tag in tag_to_warehouse -%}
            use warehouse {{ tag_to_warehouse[tag] }};
            {%- break -%}
        {%- endif -%}
    {%- endfor -%}
{% endmacro %}