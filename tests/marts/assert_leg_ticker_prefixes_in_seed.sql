-- Upstream-change guard: every leg market ticker's prefix must be in the
-- ticker_prefixes seed. A new prefix is reported here (stored for review)
-- instead of silently becoming "other": a new series listed by the exchange
-- would otherwise be misclassified without anyone noticing. To fix a
-- failure, add the prefix to seeds/ticker_prefixes.csv.
{{ config(meta={'dagster': {'ref': {'name': 'ticker_prefixes'}}}, severity='warn', store_failures=true) }}
with prefixes as (
    select split_part(market_ticker, '.', 1) as ticker_prefix, market_ticker
    from {{ ref('stg_fill_legs') }}
    union
    select split_part(market_ticker, '.', 1), market_ticker
    from {{ ref('stg_market_results') }}
)
select ticker_prefix, count(distinct market_ticker) as n_markets, min(market_ticker) as example_ticker
from prefixes
where ticker_prefix not in (select ticker_prefix from {{ ref('ticker_prefixes') }})
group by ticker_prefix
