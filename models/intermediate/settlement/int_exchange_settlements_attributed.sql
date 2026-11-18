-- One row per exchange settlement row (stg_exchange_settlements: one per
-- ticker x side), attributed to exactly one source. The settlement feed
-- covers the whole account; the bot's fill log is the bot's (plus the taker
-- script's fills, see int_fills_origin).
--
-- What the row carries: the exchange's own count and cost (cost excludes
-- trading fees). Its revenue is NOT the exchange's: the bot's reconciler
-- computes it as count x $1 if the result is the held side, the cost back if
-- void, else 0. A scalar result is neither, so it logs 0.
--
-- The exchange does not net YES against NO on a ticker: it reports and
-- settles each side on its own. So the attribution is per row, side by side.
--
-- attribution_source, first matching rule wins:
--   manual_single_market     not a combo (CMB.) ticker; the bot only
--                            trades combos.
--   known_artifact           settlement_result = scalar: revenue logged 0
--                            whatever the exchange paid. underlying_source
--                            says whose position it was.
--   bot_taker                every fill on this ticker x side is a
--                            taker-script fill, and count and cost agree.
--   bot                      every fill is a bot maker fill, and count and
--                            cost agree. "Cost agrees" means one of:
--                              exact          exchange cost = fill_cost;
--                              exact_ex_fee   exchange cost = fill_cost minus
--                                             the fee folded into it;
--                              cost_estimated the position has estimate-cost
--                                             fills and the exchange charged
--                                             LESS than the bot's estimate
--                                             (same contracts, so no other
--                                             activity; quantified in
--                                             cost_diff_dollars).
--   bot_plus_other_activity  the bot (or taker) traded this ticker, but this
--                            row's count differs from the fills on this side
--                            (count_differs: manual contracts added), or
--                            there are no fills on this side at all
--                            (no_fills_this_side: e.g. a manual hedge on the
--                            opposite side). non_fill_* columns quantify the
--                            non-bot part.
--   bot_fill_log_only        a combo ticker with no fills in the settled fill
--                            file, but the bot's RAW fill log has fills on
--                            this ticker x side, not in the settled file,
--                            with this row's contract count, and at least
--                            one carries a bot quote_id: the bot's fills from
--                            before the settled file begins.
--   manual_combo             a combo ticker with no fills on either side
--                            (in either fill log).
--   unexplained              anything else (e.g. count agrees, cost differs
--                            and the bot's cost was not an estimate).

{{ config(materialized='table') }}

with settlements as (

    select * from {{ ref('stg_exchange_settlements') }}

),

positions as (

    select * from {{ ref('int_fill_positions') }}

),

tickers_with_fills as (

    select distinct combo_ticker from positions

),

raw_only_fills as (

    -- raw-log fills that never reached the settled file (a trade logged by
    -- both channels is counted once)
    select *
    from {{ ref('stg_fills_raw') }}
    where fill_id not in (select fill_id from {{ ref('stg_fills') }})
    qualify row_number() over (partition by fill_id order by _source_key, _line_no) = 1

),

raw_only_positions as (

    select
        combo_ticker,
        side,
        count(*)                                            as n_raw_only_fills,
        sum(contract_count)                                 as raw_only_contract_count,
        count(quote_id) > 0                                 as raw_only_has_bot_quote
    from raw_only_fills
    group by combo_ticker, side

),

raw_fill_tickers as (

    select distinct combo_ticker from {{ ref('stg_fills_raw') }}

),

joined as (

    select
        s.*,
        s.combo_ticker like 'CMB.%'                         as is_combo_ticker,
        t.combo_ticker is not null                          as ticker_has_fills,
        p.position_id is not null                           as side_has_fills,
        r.combo_ticker is not null                          as ticker_in_raw_fill_log,
        ro.n_raw_only_fills,
        ro.raw_only_contract_count,
        coalesce(ro.raw_only_has_bot_quote, false)          as raw_only_has_bot_quote,
        p.position_origin,
        p.n_fills,
        p.n_bot_maker_fills,
        p.n_taker_fills,
        p.has_self_trade_candidate,
        p.contract_count                                    as fill_contract_count,
        p.fill_cost_dollars,
        p.fee_in_fill_cost_dollars,
        p.cost_ex_fee_dollars                               as fill_cost_ex_fee_dollars,
        p.has_estimate_cost,
        p.is_scalar                                         as fills_are_scalar,
        p.revenue_paid_dollars                              as fill_revenue_paid_dollars,
        p.gross_pnl_dollars                                 as fill_gross_pnl_dollars
    from settlements as s
    left join positions as p
        on s.combo_ticker = p.combo_ticker and s.side = p.side
    left join tickers_with_fills as t
        on s.combo_ticker = t.combo_ticker
    left join raw_fill_tickers as r
        on s.combo_ticker = r.combo_ticker
    left join raw_only_positions as ro
        on s.combo_ticker = ro.combo_ticker and s.side = ro.side

),

matched as (

    select
        *,
        case
            when not side_has_fills and ticker_has_fills            then 'no_fills_this_side'
            when not side_has_fills                                 then 'no_fills_on_ticker'
            when abs(contract_count - fill_contract_count) >= 0.005 then 'count_differs'
            when abs(cost_dollars - fill_cost_dollars) < 0.005      then 'exact'
            when abs(cost_dollars - fill_cost_ex_fee_dollars) < 0.005 then 'exact_ex_fee'
            when has_estimate_cost
             and cost_dollars < fill_cost_ex_fee_dollars            then 'cost_estimated'
            else 'cost_differs'
        end                                                 as match_kind,
        -- What the reconciler would have logged for the fills' part alone:
        -- the same revenue rule on the fills' contracts, minus their cost on
        -- the exchange's basis (fee excluded).
        case
            when not side_has_fills then 0
            else case
                     when settlement_result = 'void' then fill_cost_ex_fee_dollars
                     when settlement_result = side   then cast(fill_contract_count as decimal(18, 6))
                     else 0
                 end - fill_cost_ex_fee_dollars
        end                                                 as fills_pnl_logged_basis_dollars
    from joined

),

sourced as (

    select
        *,
        case
            when not is_combo_ticker                                        then 'manual_single_market'
            when match_kind = 'no_fills_on_ticker'
             and raw_only_has_bot_quote
             and abs(contract_count - raw_only_contract_count) < 0.005      then 'bot_fill_log_only'
            when match_kind = 'no_fills_on_ticker'                          then 'manual_combo'
            when match_kind in ('no_fills_this_side', 'count_differs')      then 'bot_plus_other_activity'
            when match_kind in ('exact', 'exact_ex_fee', 'cost_estimated')
             and position_origin = 'taker_script'                           then 'bot_taker'
            when match_kind in ('exact', 'exact_ex_fee', 'cost_estimated')
             and position_origin = 'bot_maker'                              then 'bot'
            else 'unexplained'
        end                                                 as underlying_source
    from matched

)

select
    settlement_id,
    combo_ticker,
    combo_event_ticker,
    side,
    settlement_result,
    case
        when not is_combo_ticker          then 'manual_single_market'
        when settlement_result = 'scalar' then 'known_artifact'
        else underlying_source
    end                                                     as attribution_source,
    underlying_source,
    case when settlement_result = 'scalar' then 'scalar_revenue_logged_zero' end
                                                            as artifact_kind,
    match_kind,
    is_combo_ticker,
    ticker_has_fills,
    side_has_fills,
    ticker_in_raw_fill_log,
    n_raw_only_fills,
    raw_only_contract_count,
    raw_only_has_bot_quote,
    position_origin,
    has_self_trade_candidate,
    n_fills,
    n_bot_maker_fills,
    n_taker_fills,
    -- the exchange's side
    contract_count,
    cost_dollars,
    revenue_dollars                                         as revenue_logged_dollars,
    pnl_dollars                                             as pnl_logged_dollars,
    -- the fills' side
    fill_contract_count,
    fill_cost_dollars,
    fee_in_fill_cost_dollars,
    fill_cost_ex_fee_dollars,
    has_estimate_cost,
    fills_are_scalar,
    fill_revenue_paid_dollars,
    fill_gross_pnl_dollars,
    fills_pnl_logged_basis_dollars,
    -- exchange minus fills (positive cost_diff: the exchange charged more)
    contract_count - coalesce(fill_contract_count, 0)       as count_diff,
    cost_dollars - coalesce(fill_cost_ex_fee_dollars, 0)    as cost_diff_dollars,
    -- the part of this row that is not the bot's / taker's fills
    case when underlying_source in ('bot', 'bot_taker') then 0
         else contract_count - coalesce(fill_contract_count, 0) end
                                                            as non_fill_contract_count,
    case when underlying_source in ('bot', 'bot_taker') then 0
         else cost_dollars - coalesce(fill_cost_ex_fee_dollars, 0) end
                                                            as non_fill_cost_dollars,
    case when underlying_source in ('bot', 'bot_taker') then 0
         else pnl_dollars - fills_pnl_logged_basis_dollars end
                                                            as non_fill_pnl_logged_dollars,
    settled_at_utc,
    settled_date_local,
    recorded_at_utc,
    _source_key,
    _source_origin,
    _line_no
from sourced
