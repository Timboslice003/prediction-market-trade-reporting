-- Bot positions the exchange settled but the reconciler never booked (no
-- ledger entry, no settlement-log row), stored for review. Warn: this is the
-- pagination bug's footprint (the reconciler reads one page per poll and
-- skips the rest). The bridge accounts for every one of them (item
-- "never booked"); this keeps the list visible so a fix can be verified by
-- watching it go to zero.
{{ config(severity='warn', store_failures=true) }}
select position_id, combo_ticker, side, n_fills, fill_contract_count,
       bot_gross_dollars, first_leg_settled_at_utc, last_leg_settled_at_utc
from {{ ref('recon_loss_budget_positions') }}
where bridge_class = 'bot_never_reconciled'
