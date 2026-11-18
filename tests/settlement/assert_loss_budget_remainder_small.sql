-- Flags (warn) an unexplained remainder over $1. It is reported in the
-- bridge either way; this only makes a large one loud.
{{ config(severity='warn') }}
select * from {{ ref('recon_loss_budget_bridge') }}
where bridge_item = 'Unexplained remainder' and abs(amount_dollars) > 1
