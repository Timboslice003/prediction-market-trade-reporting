{#-
  The synthetic generator's ground truth, for TESTS ONLY.

  scripts/generate_synthetic.py writes what it planted (counts, the P&L it
  computed independently) to _manifest/expectations.parquet. A test compares
  what dbt found with it:  where n_found <> {{ expected('doubled_rfqs') }}
  No model may call this macro: the models must find the breaks on their own.
  On real data these figures were pinned constants verified by hand.
-#}
{% macro expected(key) -%}
    (select value from {{ source('extract_meta', 'expectations') }} where key = '{{ key }}')
{%- endmacro %}
