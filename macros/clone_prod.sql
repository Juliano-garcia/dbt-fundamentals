{% macro create_dev() %}

    {% if target.name != 'prod' %}

        {% set dataset %}
            {{ target.database }}.{{ target.schema }}
        {%- endset %}

        {% set sql_statement -%}
            CREATE SCHEMA IF NOT EXISTS `{{ target.database }}.{{ target.schema }}` OPTIONS (default_table_expiration_days=30)
        {%- endset %}

        {{ dbt_utils.log_info("Creating schema " ~ dataset) }}
        {% do run_query(sql_statement) %}
        {{ dbt_utils.log_info("Successfully created schema " ~ dataset) }}

    {% else %}

        {{ dbt_utils.log_info("No-op: your current env is " ~ target.name ~ ". This is not a dev enviornment.", info=True) }}

    {% endif %}

{% endmacro %}


{% macro drop_dev() %}

    {% if target.name != 'prod' %}

        {% set dataset %}
            {{ target.database }}.{{ target.schema }}
        {%- endset %}

        {% set sql_statement -%}
            DROP SCHEMA IF EXISTS `{{ target.database }}.{{ target.schema }}` CASCADE
        {%- endset %}

        {{ dbt_utils.log_info("Dropping dev schema " ~ dataset) }}

        {% do run_query(sql_statement) %}

        {{ dbt_utils.log_info("Dropped dev schema" ~ dataset) }}

    {% else %}

        {{ dbt_utils.log_info("No-op: your current env is " ~ target.name ~ ". This enviornment cannot be dropped.", info=True) }}

    {% endif %}

{% endmacro %}


{% macro clone_prod() %}

    {% if target.name != 'prod' %}

        {{ drop_dev() }}
        {{ create_dev() }}

        {% set sql_get_schemas %}
            SELECT
                CONCAT("`",catalog_name,"`.`",schema_name,"`") as sch
            FROM {{ target.database }}.INFORMATION_SCHEMA.SCHEMATA
            WHERE schema_name NOT LIKE "_%"
                OR schema_name NOT LIKE "source_%"
                OR schema_name NOT LIKE "dev_%"
        {% endset %}

        {%- set schemas = dbt_utils.get_query_results_as_dict(sql_get_schemas) -%}

        {{ dbt_utils.log_info("Gathered production schemas") }}

        {% for sch in schemas['sch'] | unique -%}

            {{ dbt_utils.log_info("Cloning tables in schema " ~ sch) }}

            {# CLONE TABLES #}
            {% set sql_get_tables_in_schema %}
                SELECT table_name FROM {{sch}}.`INFORMATION_SCHEMA.TABLES` WHERE table_type = 'BASE TABLE'
            {% endset %}

            {%- set tables = dbt_utils.get_query_results_as_dict(sql_get_tables_in_schema) -%}

            {% for table_name in tables['table_name'] -%}
                {% set clone_table_statement %}
                    CREATE OR REPLACE TABLE {{ target.database }}.{{ target.schema }}.{{table_name}}
                    CLONE {{sch}}.{{table_name}}
                {% endset %}
                {% do run_query(clone_table_statement) %}
            {% endfor %}

            {{ dbt_utils.log_info("Copying views in schema " ~ sch) }}

            {# COPY VIEWS #}
            {% set sql_get_views_in_schema %}
                SELECT table_name FROM {{sch}}.`INFORMATION_SCHEMA.TABLES` WHERE table_type = 'VIEW'
            {% endset %}

            {%- set views = dbt_utils.get_query_results_as_dict(sql_get_views_in_schema) -%}

            {% for table_name in views['table_name'] -%}
                {% set copy_view_statement %}
                    CREATE OR REPLACE VIEW {{ target.database }}.{{ target.schema }}.{{table_name}}
                    AS SELECT * FROM {{sch}}.{{table_name}}
                {% endset %}
                {% do run_query(copy_view_statement) %}
            {% endfor %}

            {{ dbt_utils.log_info("Successfully cloned/copied schema " ~ sch) }}

        {% endfor %}
    
    {% else %}

        {{ dbt_utils.log_info("No-op: your current env is " ~ target.name ~ ". This enviornment cannot be replaced.", info=True) }}

    {% endif %}

{% endmacro %}