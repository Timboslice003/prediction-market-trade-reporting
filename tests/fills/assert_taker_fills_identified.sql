-- int_fills_origin finds exactly the taker-script fills the ledgers hold, and
-- the one bot maker fill a ledger also claims (the self-trade lookalike).
with t as (
    select
        count(*) filter (where is_on_taker_ledger)          as on_ledger,
        count(*) filter (where fill_origin = 'taker_script') as taker,
        count(*) filter (where is_self_trade_candidate)     as self_trade
    from {{ ref('int_fills_origin') }}
)
select * from t
where on_ledger <> {{ expected('taker_fills_on_ledger') }}
   or taker <> {{ expected('taker_script_fills') }}
   or self_trade <> {{ expected('self_trade_candidates') }}
