{#-
  Row-count reconciliation against the extract manifest.

  For every source file of `table_name` (manifest status = 'ok'):
      manifest.rows_written == staged rows + excluded rows
  where excluded rows = the file's _parse_error rows (landed by the extract,
  dropped by staging) + true duplicates removed by staging (none exist).
  Returns one row per file that does not reconcile, so the
  test fails on any of them. A full outer join catches files present on only
  one side.

  per_file=false compares table totals instead (market_results: one file,
  fetched from the exchange's API, no lineage columns).
-#}
{% macro manifest_rowcount_reconciliation(table_name, staging_model, source_relation,
                                          has_parse_error_flag=true, per_file=true) %}

{%- set file_key = '_source_key' if per_file else "'(all files)'" -%}

with manifest as (

    select
        {{ 'source_key' if per_file else "'(all files)'" }} as file_key,
        sum(cast(rows_written as bigint))                   as rows_written
    from {{ source('extract_meta', 'manifest') }}
    where "table" = '{{ table_name }}'
      and status = 'ok'
    group by 1

),

staged as (

    select {{ file_key }} as file_key, count(*) as staged_rows
    from {{ staging_model }}
    group by 1

),

excluded as (

    {% if has_parse_error_flag -%}
    select {{ file_key }} as file_key, count(*) as excluded_rows
    from {{ source_relation }}
    where _parse_error
    group by 1
    {%- else -%}
    select null::varchar as file_key, 0::bigint as excluded_rows
    where false
    {%- endif %}

)

select
    coalesce(m.file_key, s.file_key, e.file_key)    as file_key,
    m.rows_written                                  as manifest_rows_written,
    coalesce(s.staged_rows, 0)                      as staged_rows,
    coalesce(e.excluded_rows, 0)                    as excluded_rows
from manifest as m
full outer join staged as s
    on s.file_key = m.file_key
full outer join excluded as e
    on e.file_key = coalesce(m.file_key, s.file_key)
where coalesce(m.rows_written, -1)
   <> coalesce(s.staged_rows, 0) + coalesce(e.excluded_rows, 0)

{% endmacro %}
