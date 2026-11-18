-- Inside the bridge window the ledger's cumulative is exactly the running sum
-- of pnl (no restart, no replay), so the bridge can work position by
-- position. Checked at the bridged entry, to the cent, and that entry's
-- cumulative is the target figure.
with w as (
    select sum(pnl_dollars) as window_pnl
    from {{ ref('int_loss_budget_ledger') }}
    where bridge_window = 'in_window'
),
e as (
    select cumulative_realized_pnl_dollars as cum
    from {{ ref('int_loss_budget_ledger') }}
    where is_bridge_end_entry
)
select w.window_pnl, e.cum
from w full outer join e on true
where e.cum is null
   or abs(w.window_pnl - e.cum) >= 0.005
   or round(e.cum, 2) <> round({{ expected('bridge_target') }}, 2)
