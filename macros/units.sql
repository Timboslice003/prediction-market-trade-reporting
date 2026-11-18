{#- Leg book fields (best_bid, best_ask, spreads) are logged in CENTS. -#}
{% macro cents_to_dollars(col) -%}
    ({{ col }} / 100.0)
{%- endmacro %}

{#-
  Exchange-ledger money (fill cost, fees, revenue, P&L) as an exact decimal.
  The logs carry float noise (4.521000000000001); 6 dp keeps every real digit.
-#}
{% macro to_money(col) -%}
    cast(round({{ col }}, 6) as decimal(18, 6))
{%- endmacro %}

{#-
  Money rounded DOWN to the whole cent, as an exact decimal: what the exchange
  pays on a settlement (it floors fractional payouts; see int_fills_graded).
-#}
{% macro floor_to_cent(col) -%}
    cast(floor(({{ col }}) * 100) / 100 as decimal(18, 6))
{%- endmacro %}
