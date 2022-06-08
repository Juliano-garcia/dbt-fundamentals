{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.dataset -%}

    {% if target.name != 'prod' %}

        {{ default_schema }}

    {% else %}

        {%- if custom_schema_name is none -%}

            {# Check if the model is sitting on the `Models` root folder #}
            {% if node.fqn[1:-1]|length == 0 %}
                "_unassigned"
            {# Check if the model is sitting on any of the level 1 folders without a domain subfolder #}
            {% elif node.fqn[1:-1]|length == 1 and node.fqn[1] != "Core" %}
                "_unassigned"
            {% else %}
                {# Concat the subfolders names #}
                {% set prefix = node.fqn[1:3]|join('_') %}
                {{ prefix | trim }}
            {% endif %}

        {%- else -%}

            {{ custom_schema_name | trim }}

        {%- endif -%}

    {%- endif -%}

{%- endmacro %}