-- One row per line of the taker script's JSONL logs: kind = rfq (an RFQ the
-- script created), quote (a market maker's quote it saw: several makers
-- compete on each RFQ), accepted (a quote it accepted). Accepted is NOT
-- filled: some accepted quotes never executed (no fill, settlement or ledger
-- entry anywhere). quote / accepted rows carry the exchange's quote object.

with files as (

    select
        regexp_replace(replace(filename, chr(92), '/'), '^.*/taker/', 'taker/') as _source_key,
        regexp_extract(replace(filename, chr(92), '/'), '/taker/([^/]+)/', 1)  as strategy,
        string_split(rtrim(replace(content, chr(13), ''), chr(10)), chr(10))   as lines
    from {{ source('taker', 'taker_log_files') }}

),

lines as (

    select
        _source_key,
        strategy,
        unnest(lines)                                       as raw_json,
        unnest(generate_series(1, len(lines)))              as _line_no
    from files

),

parsed as (

    select
        *,
        json_transform(raw_json, '{
            "ts": "DOUBLE", "kind": "VARCHAR",
            "payload": {
                "combo": "VARCHAR", "rfq": {"id": "VARCHAR"}, "cost": "DOUBLE",
                "quote": {
                    "id": "VARCHAR", "rfq_id": "VARCHAR", "market_ticker": "VARCHAR",
                    "creator_id": "VARCHAR", "rfq_creator_id": "VARCHAR",
                    "yes_bid_dollars": "VARCHAR", "no_bid_dollars": "VARCHAR",
                    "yes_contracts_fp": "VARCHAR", "no_contracts_fp": "VARCHAR",
                    "status": "VARCHAR", "created_ts": "VARCHAR"
                }
            }
        }') as j
    from lines

)

select
    strategy || ':' || _source_key || ':' || cast(_line_no as varchar) as taker_log_id,
    strategy,
    j.kind                                                  as log_kind,
    {{ epoch_to_utc('j.ts') }}                              as logged_at_utc,
    coalesce(j.payload.quote.rfq_id, j.payload.rfq.id)      as rfq_id,
    j.payload.quote.id                                      as quote_id,
    coalesce(j.payload.quote.market_ticker, j.payload.combo) as combo_ticker,
    j.payload.quote.creator_id                              as quote_creator_id,
    j.payload.quote.rfq_creator_id                          as rfq_creator_id,
    cast(nullif(j.payload.quote.yes_bid_dollars, '') as decimal(18, 6)) as yes_bid_dollars,
    cast(nullif(j.payload.quote.no_bid_dollars, '') as decimal(18, 6))  as no_bid_dollars,
    cast(nullif(j.payload.quote.yes_contracts_fp, '') as double) as yes_contracts,
    cast(nullif(j.payload.quote.no_contracts_fp, '') as double)  as no_contracts,
    {{ iso_to_utc('j.payload.quote.created_ts') }}          as quote_created_at_utc,
    {{ to_money('j.payload.cost') }}                        as stake_dollars,
    raw_json,
    _source_key,
    'host_copy'                                             as _source_origin,
    _line_no
from parsed
