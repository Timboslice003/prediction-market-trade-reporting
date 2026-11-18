-- One row per position (combo_ticker x side) that is in the settled fill file
-- or in the loss-budget ledger: the drill-down behind recon_loss_budget_bridge.
-- Each row puts its dollars into exactly one bridge_class, and bot positions
-- in the window split their ledger-minus-bot difference into item columns.
--
-- Identity, per position:  ledger_pnl_in_window
--     = bot_gross_exact + item_cent_floor + item_outside_window
--       + item_revenue_logging + item_fee_in_cost + item_estimate_cost
--       + item_other_activity + item_residual
-- where item_outside_window = -bot_gross for bot positions the window does
-- not contain, and the ledger pnl is the whole amount for positions with no
-- fills. Summed, the left side is the ledger cumulative being bridged.
--
-- bridge_class:
--   bot_in_window                ledger entry inside the bridge window
--   bot_before_window            booked before the ledger last restarted at 0
--   bot_after_window             booked after the bridged entry
--   bot_ledger_write_lost        in the settlement log but not the ledger
--                                (the ledger write failed at a restart)
--   bot_settled_after_ledger_end every leg settled after the ledger's last entry
--   bot_never_reconciled         the exchange settled it, but the reconciler
--                                never booked it: no ledger entry, no
--                                settlement-log row (the pagination bug)
--   other_outside_window         no fills, ledger entry outside the window
--   manual_single_market         no fills, not a combo ticker
--   other_activity_opposite_side no fills on this side, bot fills on the
--                                other side of the same combo (a hedge)
--   bot_fill_log_only            no settled fills, but bot raw-log fills
--                                (with a bot quote_id) on this ticker x side
--   manual_combo                 no fills on the combo at all

{{ config(materialized='table') }}

with fills as (

    select * from {{ ref('int_fill_positions') }}

),

ledger as (

    select
        position_id,
        any_value(combo_ticker)                             as combo_ticker,
        any_value(side)                                     as side,
        count(*)                                            as n_ledger_entries,
        min(ledger_seq)                                     as first_ledger_seq,
        arg_min(bridge_window, ledger_seq)                  as ledger_window,
        sum(pnl_dollars) filter (where bridge_window = 'in_window')
                                                            as ledger_pnl_in_window_raw,
        arg_min(pnl_dollars, ledger_seq)                    as ledger_pnl_first_entry,
        min(recorded_at_utc)                                as first_recorded_at_utc
    from {{ ref('int_loss_budget_ledger') }}
    group by position_id

),

settlements as (

    select
        combo_ticker || ':' || side                         as position_id,
        *
    from {{ ref('int_exchange_settlements_attributed') }}

),

ledger_end as (

    select max(recorded_at_utc) as ledger_last_entry_at_utc
    from {{ ref('int_loss_budget_ledger') }}

),

leg_times as (

    select
        g.combo_ticker || ':' || g.side                     as position_id,
        min(m.settled_at_utc)                               as first_leg_settled_at_utc,
        max(m.settled_at_utc)                               as last_leg_settled_at_utc
    from {{ ref('int_fills_graded') }} as g
    join {{ ref('stg_fill_legs') }} as l using (fill_id)
    join {{ ref('stg_market_results') }} as m using (market_ticker)
    group by 1

),

tickers_with_fills as (

    select distinct combo_ticker from fills

),

raw_only_bot as (

    -- ticker x side with raw-log-only fills that carry a bot quote_id
    select distinct combo_ticker || ':' || side             as position_id
    from {{ ref('stg_fills_raw') }}
    where fill_id not in (select fill_id from {{ ref('stg_fills') }})
      and quote_id is not null

),

keys as (

    select position_id, combo_ticker, side from fills
    union
    select position_id, combo_ticker, side from ledger

),

joined as (

    select
        k.position_id,
        k.combo_ticker,
        k.side,
        k.combo_ticker like 'CMB.%'                         as is_combo_ticker,
        f.position_id is not null                           as has_fills,
        t.combo_ticker is not null                          as ticker_has_fills,
        rob.position_id is not null                         as has_raw_only_bot_fills,
        f.position_origin,
        f.n_fills,
        f.contract_count                                    as fill_contract_count,
        f.is_scalar,
        f.has_estimate_cost,
        f.has_self_trade_candidate,
        coalesce(f.gross_pnl_exact_dollars, 0)              as bot_gross_exact_dollars,
        coalesce(f.gross_pnl_dollars, 0)                    as bot_gross_dollars,
        f.revenue_paid_dollars                              as bot_revenue_paid_dollars,
        f.fee_in_fill_cost_dollars,
        f.cost_ex_fee_dollars                               as bot_cost_ex_fee_dollars,
        l.position_id is not null                           as in_ledger,
        l.n_ledger_entries,
        l.ledger_window,
        coalesce(l.ledger_pnl_in_window_raw, 0)             as ledger_pnl_in_window_dollars,
        l.ledger_pnl_first_entry                            as ledger_pnl_dollars,
        l.first_recorded_at_utc                             as ledger_recorded_at_utc,
        s.settlement_id is not null                         as has_settlement_row,
        s.attribution_source                                as settlement_attribution_source,
        s.contract_count                                    as settlement_contract_count,
        s.cost_dollars                                      as settlement_cost_dollars,
        s.revenue_logged_dollars                            as settlement_revenue_logged_dollars,
        s.recorded_at_utc                                   as settlement_recorded_at_utc,
        lt.first_leg_settled_at_utc,
        lt.last_leg_settled_at_utc,
        le.ledger_last_entry_at_utc
    from keys as k
    left join fills as f using (position_id)
    left join ledger as l using (position_id)
    left join settlements as s using (position_id)
    left join leg_times as lt using (position_id)
    left join tickers_with_fills as t on k.combo_ticker = t.combo_ticker
    left join raw_only_bot as rob using (position_id)
    cross join ledger_end as le

),

classified as (

    select
        *,
        case
            when has_fills and not in_ledger and has_settlement_row         then 'bot_ledger_write_lost'
            when has_fills and not in_ledger
             and first_leg_settled_at_utc > ledger_last_entry_at_utc        then 'bot_settled_after_ledger_end'
            when has_fills and not in_ledger                                then 'bot_never_reconciled'
            when has_fills and ledger_window = 'before_window'              then 'bot_before_window'
            when has_fills and ledger_window = 'after_window'               then 'bot_after_window'
            when has_fills                                                  then 'bot_in_window'
            when ledger_window <> 'in_window'                               then 'other_outside_window'
            when not is_combo_ticker                                        then 'manual_single_market'
            when ticker_has_fills                                           then 'other_activity_opposite_side'
            when has_raw_only_bot_fills                                     then 'bot_fill_log_only'
            else 'manual_combo'
        end                                                 as bridge_class,
        has_settlement_row
            and abs(settlement_contract_count - fill_contract_count) < 0.005 as is_count_match
    from joined

),

items as (

    select
        *,
        bot_gross_dollars - bot_gross_exact_dollars         as item_cent_floor_dollars,
        case
            when bridge_class like 'bot_%' and bridge_class <> 'bot_in_window'
             and bridge_class <> 'bot_fill_log_only'        then -bot_gross_dollars
            else 0
        end                                                 as item_outside_window_dollars,
        -- in-window bot positions: why the ledger differs from the bot
        case
            when bridge_class <> 'bot_in_window' or not is_scalar then 0
            when is_count_match then settlement_revenue_logged_dollars - bot_revenue_paid_dollars
            when not has_settlement_row then -bot_revenue_paid_dollars   -- the same code logs 0
            else 0
        end                                                 as item_revenue_logging_dollars,
        case
            when bridge_class = 'bot_in_window' and (is_count_match or not has_settlement_row)
                then fee_in_fill_cost_dollars
            else 0
        end                                                 as item_fee_in_cost_dollars,
        case
            when bridge_class = 'bot_in_window' and is_count_match and has_estimate_cost
                then bot_cost_ex_fee_dollars - settlement_cost_dollars
            else 0
        end                                                 as item_estimate_cost_dollars,
        case
            when bridge_class = 'bot_in_window' and has_settlement_row and not is_count_match
                then ledger_pnl_in_window_dollars - bot_gross_dollars
            when bridge_class in ('manual_single_market', 'other_activity_opposite_side',
                                  'bot_fill_log_only', 'manual_combo')
                then ledger_pnl_in_window_dollars
            else 0
        end                                                 as item_other_activity_dollars
    from classified

)

select
    *,
    -- what is left: ledger minus everything explained (0 outside bot_in_window
    -- by construction)
    ledger_pnl_in_window_dollars
        - (bot_gross_exact_dollars + item_cent_floor_dollars + item_outside_window_dollars
           + item_revenue_logging_dollars + item_fee_in_cost_dollars
           + item_estimate_cost_dollars + item_other_activity_dollars)
                                                            as item_residual_dollars
from items
