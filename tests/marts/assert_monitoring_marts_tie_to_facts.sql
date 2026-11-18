-- Dagster: shown as an asset check on mart_market_maker_monitoring (orchestration/, docs/ORCHESTRATION.md).
{{ config(meta={'dagster': {'ref': {'name': 'mart_market_maker_monitoring'}}}) }}
-- The two monitoring marts aggregate the same facts and must not lose any:
--   bot maker fills: mart_market_maker_monitoring = mart_requester_activity
--                    = fct_fills (fill_origin bot_maker, incl. the fills
--                    with no RFQ link);
--   every bot maker fill is either matched to a counted live quote or
--                    counted as unmatched: sum(n_matched_fills) +
--                    sum(n_unmatched_bot_maker_fills) = fct_fills bot_maker;
--   counted RFQ decisions: monitoring n_rfqs_decided = requester n_rfqs.
-- Refs heavy marts, so it runs with them, not in the routine selector.
with f as (
    select count(*) as n from {{ ref('fct_fills') }} where fill_origin = 'bot_maker'
),
m as (
    select sum(n_bot_maker_fills) as fills,
           sum(n_matched_fills) + sum(n_unmatched_bot_maker_fills) as matched_plus_unmatched,
           sum(n_rfqs_decided) as rfqs
    from {{ ref('mart_market_maker_monitoring') }}
),
r as (
    select sum(n_fills) as fills, sum(n_rfqs) as rfqs
    from {{ ref('mart_requester_activity') }}
)
select f.n as fct_fills_bot_maker, m.fills as monitoring_fills,
       m.matched_plus_unmatched as monitoring_matched_plus_unmatched,
       r.fills as requester_fills, m.rfqs as monitoring_rfqs, r.rfqs as requester_rfqs
from f, m, r
where m.fills <> f.n or m.matched_plus_unmatched <> f.n or r.fills <> f.n or m.rfqs <> r.rfqs
