-- The waterfall ties: start + every item (unexplained remainder included)
-- = the ledger cumulative at the bridged entry, to the cent. This does not
-- force the remainder to anything; it checks the arithmetic.
with t as (
    select
        sum(amount_dollars) filter (where row_kind in ('start', 'item')) as bridged,
        max(amount_dollars) filter (where row_kind = 'end')              as target
    from {{ ref('recon_loss_budget_bridge') }}
)
select * from t
where target is null or abs(bridged - target) >= 0.005
