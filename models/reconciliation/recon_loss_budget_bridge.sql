-- The waterfall from the bot's gross P&L (exact revenue, from the graded
-- fills) to the loss-budget ledger's cumulative_realized_pnl at the bridged
-- entry. One row per bridge item, in order, with its dollar amount, the number
-- of positions in it, the running total, and the evidence. Amounts come from
-- recon_loss_budget_positions (one row per combo x side), so every item
-- drills down to positions. Rows with amount 0 are leads that were checked
-- and contribute nothing to this figure; the evidence says why.
-- The last item is the unexplained remainder, whatever it is. It is reported,
-- never forced to zero.

{{ config(materialized='table') }}

with p as (

    select * from {{ ref('recon_loss_budget_positions') }}

),

ledger_end as (

    select cumulative_realized_pnl_dollars as ledger_cumulative_dollars, ledger_entry_id
    from {{ ref('int_loss_budget_ledger') }}
    where is_bridge_end_entry

),

ledger_breaks as (

    select
        count(*) filter (where is_repeat_write)                         as n_repeat_writes,
        coalesce(sum(pnl_dollars) filter (where is_repeat_write), 0)     as repeat_pnl
    from {{ ref('int_loss_budget_ledger') }}

),

items as (

    select 1 as step_order, 'start' as row_kind,
           'Bot gross P&L, exact revenue' as bridge_item,
           sum(bot_gross_exact_dollars) as amount_dollars,
           count(*) filter (where has_fills) as n_positions,
           'int_fills_graded.gross_pnl_exact_dollars over every settled fill (bot maker and taker script), by position.' as evidence
    from p

    union all
    select 2, 'item', 'The exchange pays fractional payouts floored to the cent',
           coalesce(sum(item_cent_floor_dollars), 0), count(*) filter (where item_cent_floor_dollars <> 0),
           'revenue_paid = floor(revenue_exact, cent) on scalar fills (int_fills_graded).'
    from p

    union all
    select 3, 'item', 'Bot positions booked before the ledger restarted at $0',
           coalesce(sum(item_outside_window_dollars) filter (where bridge_class = 'bot_before_window'), 0),
           count(*) filter (where bridge_class = 'bot_before_window'),
           'The cumulative restarted from 0 at a deploy (int_loss_budget_ledger.is_restart_from_zero), dropping the earlier booked history; these positions are in that history, not in the bridged figure.'
    from p

    union all
    select 4, 'item', 'Bot positions whose ledger write was lost',
           coalesce(sum(item_outside_window_dollars) filter (where bridge_class = 'bot_ledger_write_lost'), 0),
           count(*) filter (where bridge_class = 'bot_ledger_write_lost'),
           'In the settlement log but not the ledger. The ledger is written before the settlement log, so the ledger write failed (one poll batch, just before the restart).'
    from p

    union all
    select 5, 'item', 'Bot positions the settlement reconciler never booked',
           coalesce(sum(item_outside_window_dollars) filter (where bridge_class = 'bot_never_reconciled'), 0),
           count(*) filter (where bridge_class = 'bot_never_reconciled'),
           'Graded bot positions with no ledger entry and no settlement-log row. The reconciler reads ONE page of settlements per poll and then advances its cursor past the rest (a pagination bug): busy settlement batches lose everything after the first page.'
    from p

    union all
    select 6, 'item', 'Bot positions whose legs all settled after the ledger''s last entry',
           coalesce(sum(item_outside_window_dollars) filter (where bridge_class = 'bot_settled_after_ledger_end'), 0),
           count(*) filter (where bridge_class = 'bot_settled_after_ledger_end'),
           'First leg settled after the last ledger write in the extract: they could not have been booked yet.'
    from p

    union all
    select 7, 'item', 'Bot positions booked after the bridged entry',
           coalesce(sum(item_outside_window_dollars) filter (where bridge_class = 'bot_after_window'), 0),
           count(*) filter (where bridge_class = 'bot_after_window'),
           'Booked after the entry being bridged (var loss_budget_bridge_end_before_utc).'
    from p

    union all
    select 8, 'item', 'Scalar settlements logged with revenue 0',
           coalesce(sum(item_revenue_logging_dollars), 0), count(*) filter (where item_revenue_logging_dollars <> 0),
           'The reconciler computes revenue as count x $1 if the result is the held side, the cost back if void, else 0, so a scalar settlement books a full loss. The bot''s graded revenue on the in-window ones is reversed here.'
    from p

    union all
    select 9, 'item', 'Fee folded into fill_cost (bot fills)',
           coalesce(sum(item_fee_in_cost_dollars) filter (where position_origin = 'bot_maker'), 0),
           count(*) filter (where position_origin = 'bot_maker' and item_fee_in_cost_dollars <> 0),
           'On settle_enriched + refetch fills fill_cost = count x price + fee. The exchange''s settlement cost excludes fees, so the ledger is higher by that fee. Otherwise neither figure deducts trading fees.'
    from p

    union all
    select 10, 'item', 'Fee folded into fill_cost (taker-script fills)',
           coalesce(sum(item_fee_in_cost_dollars) filter (where position_origin = 'taker_script'), 0),
           count(*) filter (where position_origin = 'taker_script' and item_fee_in_cost_dollars <> 0),
           'The taker script''s fills (int_fills_origin) are in both figures: the fill log records them and the exchange settled them on this account. The whole difference is their fee.'
    from p

    union all
    select 11, 'item', 'Estimate-cost fills: the exchange charged less than the bot estimated',
           coalesce(sum(item_estimate_cost_dollars), 0), count(*) filter (where item_estimate_cost_dollars <> 0),
           'Same contract count, the exchange''s cost lower than fill_cost on positions with cost_source = estimate fills.'
    from p

    union all
    select 12, 'item', 'Non-bot contracts on bot-traded positions (same side)',
           coalesce(sum(item_other_activity_dollars) filter (where bridge_class = 'bot_in_window'), 0),
           count(*) filter (where bridge_class = 'bot_in_window' and item_other_activity_dollars <> 0),
           'The exchange''s count differs from the bot''s fills on that ticker x side (manual adds). Whole ledger-minus-bot difference of those positions; non-bot part per row in int_exchange_settlements_attributed.non_fill_*.'
    from p

    union all
    select 13, 'item', 'Non-bot positions on the opposite side of bot combos (hedges)',
           coalesce(sum(item_other_activity_dollars) filter (where bridge_class = 'other_activity_opposite_side'), 0),
           count(*) filter (where bridge_class = 'other_activity_opposite_side'),
           'Ledger positions with no fills on this side while the bot filled the other side of the same combo.'
    from p

    union all
    select 14, 'item', 'Manual combos (no bot or taker fills)',
           coalesce(sum(item_other_activity_dollars) filter (where bridge_class = 'manual_combo'), 0),
           count(*) filter (where bridge_class = 'manual_combo'),
           'Combo tickers with no fills in either fill log, booked in the window.'
    from p

    union all
    select 15, 'item', 'Manual single-market trades',
           coalesce(sum(item_other_activity_dollars) filter (where bridge_class = 'manual_single_market'), 0),
           count(*) filter (where bridge_class = 'manual_single_market'),
           'Non-combo tickers: the bot only trades combos.'
    from p

    union all
    select 16, 'item', 'Bot fills in the raw log only (before the settled file begins)',
           coalesce(sum(item_other_activity_dollars) filter (where bridge_class = 'bot_fill_log_only'), 0),
           count(*) filter (where bridge_class = 'bot_fill_log_only'),
           'History before the settled fill file begins (int_exchange_settlements_attributed.bot_fill_log_only). All were booked before the restart, so they add 0 to this figure.'
    from p

    union all
    select 17, 'item', 'Doubled dual-instance ledger entries',
           cast(0 as decimal(18, 6)), (select n_repeat_writes from ledger_breaks),
           'The settle_keys written twice add 0: the second instance re-booked them from the same base, so the running total counts them once (the negative cumulative_jump in int_loss_budget_ledger), and they are before the restart anyway.'

    union all
    select 18, 'item', 'Unexplained remainder',
           coalesce(sum(item_residual_dollars), 0), count(*) filter (where abs(item_residual_dollars) >= 0.000001),
           'Ledger minus every item above. In-window bot positions booked while the settlement log was missing cannot be checked for count or cost; a remainder there is most likely the estimate-cost effect of item 11 (recon_loss_budget_positions where item_residual_dollars <> 0).'
    from p

),

ordered as (

    select
        *,
        sum(amount_dollars) over (order by step_order rows between unbounded preceding and current row)
                                                            as running_total_dollars
    from items

)

select * from ordered

union all

select
    99                                                      as step_order,
    'end'                                                   as row_kind,
    'Loss-budget cumulative_realized_pnl at the bridged entry' as bridge_item,
    e.ledger_cumulative_dollars                             as amount_dollars,
    (select count(*) from p where ledger_pnl_in_window_dollars <> 0) as n_positions,
    'int_loss_budget_ledger, entry ' || e.ledger_entry_id || '. The running total above must equal it (assert_loss_budget_bridge_ties).'
                                                            as evidence,
    e.ledger_cumulative_dollars                             as running_total_dollars
from ledger_end as e

order by step_order
