-- One row per (ticker, side): the bot's poll of the account's settlements
-- (settlement_v1). Ticker-level, NOT fill-level: it covers the whole account
-- (manual trades included), and the bot computes the revenue itself. Use it
-- for reconciliation, never as the P&L source.

with source as (

    select * from {{ source('settlement', 'exchange_settlements') }}
    where not _parse_error

),

parsed as (

    select
        _source_key,
        _source_origin,
        _line_no,
        json_transform(raw_json, '{
            "schema": "VARCHAR", "ts": "DOUBLE", "market_ticker": "VARCHAR",
            "event_ticker": "VARCHAR", "side": "VARCHAR", "count": "DOUBLE",
            "cost": "DOUBLE", "revenue": "DOUBLE", "pnl": "DOUBLE",
            "settled_ts": "DOUBLE", "settlement_result": "VARCHAR"
        }') as j
    from source

)

select
    j.market_ticker || ':' || j.side                        as settlement_id,
    j.market_ticker                                         as combo_ticker,         -- a leg ticker on single-market trades
    j.event_ticker                                          as combo_event_ticker,
    j.side,
    j.schema                                                as record_schema,
    j.count                                                 as contract_count,
    {{ to_money('j.cost') }}                                as cost_dollars,
    {{ to_money('j.revenue') }}                             as revenue_dollars,
    {{ to_money('j.pnl') }}                                 as pnl_dollars,
    j.settlement_result,
    {{ epoch_to_utc('j.settled_ts') }}                      as settled_at_utc,
    {{ utc_to_local(epoch_to_utc('j.settled_ts')) }}        as settled_at_local,
    cast({{ utc_to_local(epoch_to_utc('j.settled_ts')) }} as date) as settled_date_local,
    {{ epoch_to_utc('j.ts') }}                              as recorded_at_utc,
    _source_key,
    _source_origin,
    _line_no
from parsed
