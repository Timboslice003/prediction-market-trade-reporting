-- One row per loss-budget ledger write (loss_budget_v1, one per booked
-- settlement, with running daily / cumulative totals).
-- The log's `fill_id` is NOT a fill id: it is an idempotency key
-- "settle:{combo_ticker}:{side}" and does not join to stg_fills. Renamed to
-- settle_key. In the dual-instance window some settle_keys are written twice
-- (same pnl, seconds apart); they differ in ts, so they are not true
-- duplicates and both are kept (int_loss_budget_ledger explains them).

with source as (

    select * from {{ source('settlement', 'loss_budget') }}
    where not _parse_error

),

parsed as (

    select
        _source_key,
        _source_origin,
        _line_no,
        json_transform(raw_json, '{
            "schema": "VARCHAR", "ts": "DOUBLE", "fill_id": "VARCHAR",
            "market_ticker": "VARCHAR", "pnl": "DOUBLE",
            "daily_realized_pnl": "DOUBLE", "cumulative_realized_pnl": "DOUBLE"
        }') as j
    from source

)

select
    _source_key || ':' || cast(_line_no as varchar)         as ledger_entry_id,
    j.fill_id                                               as settle_key,
    j.market_ticker                                         as combo_ticker,
    regexp_extract(j.fill_id, ':(yes|no)$', 1)              as side,
    j.schema                                                as record_schema,
    {{ to_money('j.pnl') }}                                 as pnl_dollars,
    {{ to_money('j.daily_realized_pnl') }}                  as daily_realized_pnl_dollars,
    {{ to_money('j.cumulative_realized_pnl') }}             as cumulative_realized_pnl_dollars,
    {{ epoch_to_utc('j.ts') }}                              as recorded_at_utc,
    {{ utc_to_local(epoch_to_utc('j.ts')) }}                as recorded_at_local,
    cast({{ utc_to_local(epoch_to_utc('j.ts')) }} as date)  as recorded_date_local,
    _source_key,
    _source_origin,
    _line_no
from parsed
