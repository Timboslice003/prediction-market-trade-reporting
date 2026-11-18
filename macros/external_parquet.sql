{#-
  external_parquet: a model stored as Parquet files outside the DuckDB file.

  Used by the heavy models (stg_rfq_outcomes, stg_rfq_legs,
  stg_quote_submissions, fct_rfq_lifecycle). DuckDB never shrinks its
  database file, so rebuilding large tables inside it makes the file grow
  with every rebuild. These models live at
  $DBT_STAGING_PARQUET_ROOT/<model>/data_N.parquet (zstd, default
  work/staging_parquet), and the DuckDB file only holds a view over them:
      create view staging.<model> as select * from read_parquet('<dir>/*.parquet')
  so ref() works exactly as it did for the tables.

  Why not dbt-duckdb's built-in `external` materialization: it first builds
  the model as a regular table inside the DuckDB file and COPYs it out
  afterwards, which is the file growth this exists to avoid.

  Build steps (nothing is deleted until the new files are checked):
    1. COPY (<model sql>) TO '<dir>__dbt_tmp' -- streamed, never stored in the DB.
    2. Check: rows in the new Parquet files == rows COPY reported, and > 0.
    3. Plugin duckdb_plugins/parquet_dir_swap.py swaps <dir>__dbt_tmp into
       <dir> and deletes the old files (SQL cannot move directories).
    4. create or replace view over <dir>/*.parquet.
  A failure in 1-2 leaves the old files and view untouched.

  `dbt run-operation register_external_parquet` re-creates the views over the
  existing files without building anything (e.g. after a fresh DuckDB file).
-#}

{% macro external_parquet_location(identifier) -%}
  {{- (env_var('DBT_STAGING_PARQUET_ROOT', 'work/staging_parquet') | replace('\\', '/')).rstrip('/') ~ '/' ~ identifier -}}
{%- endmacro %}

{% macro external_parquet_view_sql(relation, location) -%}
  create or replace view {{ relation }} as
    select * from read_parquet('{{ location }}/*.parquet')
{%- endmacro %}

{% materialization external_parquet, adapter='duckdb' %}

  {%- set location = external_parquet_location(this.identifier) -%}
  {%- set tmp_location = location ~ '__dbt_tmp' -%}
  {%- set file_size_bytes = config.get('file_size_bytes', '1GB') -%}
  {%- set target_relation = this.incorporate(type='view') -%}
  {%- set existing_relation = load_cached_relation(this) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {#- 1. stream the model straight to Parquet -#}
  {% call statement('write_parquet', fetch_result=True) -%}
    copy (
      {{ compiled_code }}
    ) to '{{ tmp_location }}'
      (format parquet, compression zstd, file_size_bytes '{{ file_size_bytes }}', overwrite true)
  {%- endcall %}
  {%- set rows_written = load_result('write_parquet')['data'][0][0] | int -%}

  {#- 2. check the files before anything is replaced -#}
  {%- set rows_read = run_query("select count(*) from read_parquet('" ~ tmp_location ~ "/*.parquet')").columns[0].values()[0] | int -%}
  {%- if rows_written == 0 or rows_read != rows_written -%}
    {{ exceptions.raise_compiler_error(
        "external_parquet " ~ this ~ ": COPY wrote " ~ rows_written ~ " rows but "
        ~ tmp_location ~ " holds " ~ rows_read ~ "; refusing to replace " ~ location) }}
  {%- endif -%}
  {% do log("external_parquet " ~ this.identifier ~ ": " ~ rows_written ~ " rows -> " ~ location, info=true) %}

  {#- 3. swap the new files in (plugin; deletes the old files) -#}
  {% do adapter.store_relation('parquet_dir_swap', target_relation, [], location, 'parquet', {}) %}

  {#- 4. the view. A leftover table of the same name (an older layout) is
         replaced: its data now lives in the checked files. -#}
  {% if existing_relation is not none and existing_relation.type != 'view' %}
    {{ adapter.drop_relation(existing_relation) }}
  {% endif %}
  {% call statement('main') -%}
    {{ external_parquet_view_sql(target_relation, location) }}
  {%- endcall %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {% do persist_docs(target_relation, model) %}
  {{ adapter.commit() }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}


{#-
  dbt run-operation register_external_parquet
  Points each external_parquet model's view at its existing Parquet files.
  Builds nothing and moves no files. Refuses if a table (not a view) holds
  the model's name, so it can never drop data.
-#}
{% macro register_external_parquet() %}
  {% for node in graph.nodes.values()
       if node.resource_type == 'model' and node.config.materialized == 'external_parquet' %}
    {%- set relation = api.Relation.create(
          database=node.database, schema=node.schema, identifier=node.alias, type='view') -%}
    {%- set location = external_parquet_location(node.alias) -%}
    {%- set existing = adapter.get_relation(node.database, node.schema, node.alias) -%}
    {% if existing is not none and existing.type != 'view' %}
      {{ exceptions.raise_compiler_error(
          relation ~ " is a " ~ existing.type ~ ", not a view; rename or drop it first") }}
    {% endif %}
    {% do run_query("create schema if not exists " ~ relation.without_identifier()) %}
    {%- set n = run_query("select count(*) from read_parquet('" ~ location ~ "/*.parquet')").columns[0].values()[0] -%}
    {% do run_query(external_parquet_view_sql(relation, location)) %}
    {% do log("registered " ~ relation ~ " -> " ~ location ~ " (" ~ n ~ " rows)", info=true) %}
  {% endfor %}
  {% do adapter.commit() %}
{% endmacro %}
