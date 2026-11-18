-- One row per position the bot's fill log saw: (combo_ticker, side), the
-- grain at which the exchange settles and the loss-budget ledger books
-- (settle_key = "settle:{combo}:{side}"). Aggregates int_fills_graded, split
-- by fill_origin (bot maker vs taker script).
--
-- cost_ex_fee_dollars is the cost on the exchange's basis: the exchange's
-- settlement cost excludes trading fees, but on settle_enriched + refetch
-- fills the legacy pipeline folded the fee into fill_cost, so that fee is
-- taken back out here.

{{ config(materialized='table') }}

with fills as (

    select
        g.*,
        o.fill_origin,
        o.is_self_trade_candidate
    from {{ ref('int_fills_graded') }} as g
    join {{ ref('int_fills_origin') }} as o using (fill_id)

)

select
    combo_ticker || ':' || side                             as position_id,
    combo_ticker,
    side,
    count(*)                                                as n_fills,
    count(*) filter (where fill_origin = 'bot_maker')       as n_bot_maker_fills,
    count(*) filter (where fill_origin = 'taker_script')    as n_taker_fills,
    case
        when count(*) filter (where fill_origin = 'taker_script') = 0 then 'bot_maker'
        when count(*) filter (where fill_origin = 'bot_maker') = 0    then 'taker_script'
        else 'mixed'
    end                                                     as position_origin,
    bool_or(is_self_trade_candidate)                        as has_self_trade_candidate,
    sum(contract_count)                                     as contract_count,
    sum(fill_cost_dollars)                                  as fill_cost_dollars,
    sum(case when is_fee_in_fill_cost then fee_dollars else 0 end)
                                                            as fee_in_fill_cost_dollars,
    sum(fill_cost_dollars)
        - sum(case when is_fee_in_fill_cost then fee_dollars else 0 end)
                                                            as cost_ex_fee_dollars,
    bool_or(cost_source = 'estimate')                       as has_estimate_cost,
    count(*) filter (where parlay_result = 'scalar')        as n_scalar_fills,
    bool_or(parlay_result = 'scalar')                       as is_scalar,
    sum(revenue_exact_dollars)                              as revenue_exact_dollars,
    sum(revenue_paid_dollars)                               as revenue_paid_dollars,
    sum(gross_pnl_exact_dollars)                            as gross_pnl_exact_dollars,
    sum(gross_pnl_dollars)                                  as gross_pnl_dollars,
    min(accepted_at_utc)                                    as first_accepted_at_utc,
    max(accepted_at_utc)                                    as last_accepted_at_utc
from fills
group by combo_ticker, side
