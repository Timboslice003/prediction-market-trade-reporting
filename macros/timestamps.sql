{#-
  Epoch seconds (float) -> naive TIMESTAMP in UTC. Session-timezone independent
  (no TIMESTAMPTZ). 0 and NULL both mean "not recorded" in the bot's logs and
  map to NULL.
-#}
{% macro epoch_to_utc(col) -%}
    case
        when {{ col }} is null or {{ col }} = 0 then null
        else make_timestamp(cast(round({{ col }} * 1000000) as bigint))
    end
{%- endmacro %}

{#- UTC TIMESTAMP -> the reporting clock: a fixed offset, no daylight saving. -#}
{% macro utc_to_local(ts_expr) -%}
    ({{ ts_expr }} + to_hours({{ var('reporting_utc_offset_hours') }}))
{%- endmacro %}

{#- ISO-8601 string with Z / offset (the exchange's clock) -> naive UTC TIMESTAMP. -#}
{% macro iso_to_utc(col) -%}
    cast(timezone('UTC', cast(nullif({{ col }}, '') as timestamptz)) as timestamp)
{%- endmacro %}
