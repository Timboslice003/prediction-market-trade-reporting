{#-
  Rows (model_name, column_name) for every column of a mart that has no YAML
  description: the built relation's columns are compared with the model's
  documented columns, so a new undocumented column is caught.
  heavy=true checks the marts tagged `heavy`, heavy=false the others. The two
  halves are separate tests so each runs after the marts it checks (a ref to a
  heavy mart would drop the routine half from the routine selector).
-#}
{% macro undocumented_mart_columns(heavy) %}
    {%- set rows = [] -%}
    {%- if execute -%}
        {%- for node in graph.nodes.values()
             if node.resource_type == 'model' and node.package_name == project_name
                and node.fqn[1] == 'marts' and (('heavy' in node.tags) == heavy) -%}
            {%- set relation = adapter.get_relation(database=node.database, schema=node.schema, identifier=node.alias) -%}
            {%- if relation is none -%}
                {%- do rows.append((node.name, '<relation not built>')) -%}
            {%- else -%}
                {%- for col in adapter.get_columns_in_relation(relation) -%}
                    {%- set documented = node.columns.get(col.name) -%}
                    {%- if documented is none or (documented.description | trim) == '' -%}
                        {%- do rows.append((node.name, col.name)) -%}
                    {%- endif -%}
                {%- endfor -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endif %}
select * from (values ('', '')) as t(model_name, column_name) where false
{% for r in rows %}
union all select '{{ r[0] }}', '{{ r[1] }}'
{% endfor %}
{% endmacro %}
