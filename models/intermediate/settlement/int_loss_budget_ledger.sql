-- One row per loss-budget ledger write, in write order, with what the running
-- total did. The ledger adds each booked settlement's pnl to an in-memory
-- cumulative and logs it; on start-up it restores the cumulative from the
-- last line of the latest file. So cumulative = previous cumulative + pnl,
-- except where a process started from a different base. Two such breaks are
-- detected here, by rule, not by date:
--   a REPLAY (dual-instance window): the second of two concurrent instances
--     re-booked the same settle_keys from the same base. Its running total
--     replays the first instance's, so the repeats are counted ONCE in the
--     cumulative (the negative cumulative_jump cancels the repeated pnl).
--   a RESTART FROM ZERO: a deploy reset the cumulative to 0, dropping the
--     earlier history from the running total.
--
-- Bridge window: from the last restart-from-zero up to and including the
-- entry being bridged (the last one before var loss_budget_bridge_end_before_utc).
-- Over that window the cumulative is exactly the sum of pnl (tested).

{{ config(materialized='table') }}

with ledger as (

    select
        *,
        row_number() over (order by recorded_at_utc, _source_key, _line_no) as ledger_seq
    from {{ ref('stg_loss_budget') }}

),

stepped as (

    select
        *,
        lag(cumulative_realized_pnl_dollars) over (order by ledger_seq)    as prev_cumulative_dollars,
        row_number() over (partition by settle_key order by ledger_seq)   as key_write_no
    from ledger

),

flagged as (

    select
        *,
        cumulative_realized_pnl_dollars
            - coalesce(prev_cumulative_dollars, 0) - pnl_dollars          as cumulative_jump_dollars,
        abs(cumulative_realized_pnl_dollars
            - coalesce(prev_cumulative_dollars, 0) - pnl_dollars) >= 0.005 as is_cumulative_break,
        abs(cumulative_realized_pnl_dollars - pnl_dollars) < 0.005
            and abs(cumulative_realized_pnl_dollars
                    - coalesce(prev_cumulative_dollars, 0) - pnl_dollars) >= 0.005
                                                                          as is_restart_from_zero
    from stepped

),

window_bounds as (

    select
        max(ledger_seq) filter (
            where recorded_at_utc < timestamp '{{ var("loss_budget_bridge_end_before_utc") }}'
        )                                                                 as end_seq
    from flagged

),

window_start as (

    select
        coalesce(max(f.ledger_seq) filter (where f.is_restart_from_zero and f.ledger_seq <= w.end_seq), 1)
                                                                          as start_seq,
        any_value(w.end_seq)                                              as end_seq
    from flagged as f
    cross join window_bounds as w

)

select
    f.ledger_entry_id,
    f.ledger_seq,
    f.settle_key,
    f.combo_ticker,
    f.side,
    f.combo_ticker || ':' || f.side                                       as position_id,
    f.pnl_dollars,
    f.daily_realized_pnl_dollars,
    f.cumulative_realized_pnl_dollars,
    f.prev_cumulative_dollars,
    f.cumulative_jump_dollars,
    f.is_cumulative_break,
    f.is_restart_from_zero,
    f.key_write_no,
    f.key_write_no > 1                                                    as is_repeat_write,
    f.ledger_seq = s.end_seq                                              as is_bridge_end_entry,
    case
        when f.ledger_seq < s.start_seq then 'before_window'
        when f.ledger_seq > s.end_seq   then 'after_window'
        else 'in_window'
    end                                                                   as bridge_window,
    f.recorded_at_utc,
    f.recorded_at_local,
    f.recorded_date_local,
    f._source_key,
    f._source_origin,
    f._line_no
from flagged as f
cross join window_start as s
