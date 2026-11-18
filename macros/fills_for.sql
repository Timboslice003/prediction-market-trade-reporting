{#-
  Ad-hoc question support: every fill (or fill leg) matching a filter, as a SELECT.

  Arguments (each optional; a string or a list of strings; combined with AND):
    sport          dim_markets.sport        e.g. 'soccer', ['baseball', 'hockey']
    competition    dim_markets.competition  e.g. 'Soccer Continental Cup'
    market_ticker  a leg market ticker
    combo_ticker   the parlay ticker
    date_from      first trade date, inclusive, 'YYYY-MM-DD'
    date_to        last trade date, inclusive, 'YYYY-MM-DD'
    date_basis     which date the range applies to:
                     'accepted_date_local' (default; the reporting-day trade
                     date) or 'accepted_date_utc'
    grain          'fill' (default): one row per fill from fct_fills, every
                     fill with at least one leg matching the leg filters;
                   'fill_leg': one row per matching leg from fct_fill_legs.

  Leg filters (sport, competition, market_ticker) match if ANY leg matches,
  so a multi-sport parlay with one Continental Cup leg is a Continental Cup fill.

  Example:
    {{ fills_for(competition='Soccer Continental Cup', date_from='2025-03-11', date_to='2025-03-19') }}
-#}
{% macro fills_for(sport=none, competition=none, market_ticker=none, combo_ticker=none,
                   date_from=none, date_to=none, date_basis='accepted_date_local', grain='fill') %}

    {%- if date_basis not in ['accepted_date_local', 'accepted_date_utc'] -%}
        {{ exceptions.raise_compiler_error("fills_for: date_basis must be accepted_date_local or accepted_date_utc, got " ~ date_basis) }}
    {%- endif -%}
    {%- if grain not in ['fill', 'fill_leg'] -%}
        {{ exceptions.raise_compiler_error("fills_for: grain must be fill or fill_leg, got " ~ grain) }}
    {%- endif -%}

    {%- set conditions = [] -%}
    {%- do conditions.append(_fills_for_in('sport', sport)) if sport is not none -%}
    {%- do conditions.append(_fills_for_in('competition', competition)) if competition is not none -%}
    {%- do conditions.append(_fills_for_in('market_ticker', market_ticker)) if market_ticker is not none -%}
    {%- do conditions.append(_fills_for_in('combo_ticker', combo_ticker)) if combo_ticker is not none -%}
    {%- do conditions.append(date_basis ~ " >= date '" ~ date_from ~ "'") if date_from is not none -%}
    {%- do conditions.append(date_basis ~ " <= date '" ~ date_to ~ "'") if date_to is not none -%}
    {%- set where = conditions | join('\n      and ') if conditions else 'true' -%}

    {%- if grain == 'fill_leg' %}
select *
from {{ ref('fct_fill_legs') }}
where {{ where }}
    {%- else %}
select *
from {{ ref('fct_fills') }}
where fill_id in (
    select fill_id
    from {{ ref('fct_fill_legs') }}
    where {{ where }}
)
    {%- endif %}
{% endmacro %}


{#- column = 'value' or column in ('a', 'b'), with quotes escaped. -#}
{% macro _fills_for_in(column, value) -%}
    {%- set values = [value] if value is string else value -%}
    {%- set quoted = [] -%}
    {%- for v in values -%}
        {%- do quoted.append("'" ~ (v | string | replace("'", "''")) ~ "'") -%}
    {%- endfor -%}
    {{ column }} in ({{ quoted | join(', ') }})
{%- endmacro %}
