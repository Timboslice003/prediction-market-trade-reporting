-- One row per reporting-day trade date x sport group x leg count x fill side:
-- fills, contracts, cost, revenue, gross P&L, known fees, net P&L, and how
-- many fills have an unknown fee. Dated by the fill's accepted (execution)
-- time on the reporting clock (UTC-5), never by the fill file's date.
--
-- sport_group is the combo's single sport, or 'multi_sport' when its legs
-- span sports (dim_combos). net_pnl sums only fills whose fee is known; the
-- unknown-fee fills are counted and their gross P&L shown separately, so
-- gross_pnl = net_pnl + fees_outside_cost + gross_pnl_fee_unknown.

{{ config(materialized='table') }}

select
    accepted_date_local || ':' || coalesce(sport_group, 'unclassified') || ':'
        || cast(n_legs as varchar) || ':' || side           as daily_pnl_id,
    accepted_date_local                                     as trade_date_local,
    coalesce(sport_group, 'unclassified')                   as sport_group,
    n_legs,
    side,
    count(*)                                                as n_fills,
    sum(contract_count)                                     as contracts,
    sum(fill_cost_dollars)                                  as fill_cost_dollars,
    sum(revenue_paid_dollars)                               as revenue_dollars,
    sum(gross_pnl_dollars)                                  as gross_pnl_dollars,
    coalesce(sum(fee_dollars), 0)                           as fees_known_dollars,
    coalesce(sum(fee_outside_cost_dollars), 0)              as fees_outside_cost_dollars,
    coalesce(sum(net_pnl_dollars), 0)                       as net_pnl_dollars,
    count(*) filter (where not is_fee_known)                as n_fills_fee_unknown,
    coalesce(sum(gross_pnl_dollars) filter (where not is_fee_known), 0)
                                                            as gross_pnl_fee_unknown_dollars,
    count(*) filter (where fill_origin = 'taker_script')    as n_taker_fills
from {{ ref('fct_fills') }}
group by all
