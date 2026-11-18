-- One row per pricing-decision record that got past the filters: every row the
-- extract landed, all schema eras unified into one shape. Effectively one row
-- per RFQ, except the RFQs decided twice by two concurrent bot instances
-- (see repeated_rfqs below). Key: rfq_outcome_id (the source line).
--
-- Schema eras (the old schemas wrote one row per decision type; after the
-- schema cutover rfq_outcome_v1 writes one row per RFQ):
--   rfq_outcome_v1        -> stage = its own terminal_stage
--   exposure_rejected_v2  -> stage = 'priced_rejected', exit_reason = exposure_layer
--   breaker_skip_v2       -> stage = 'breaker_skipped', exit_reason = 'breaker'
--   balance_guard_skip_v1 -> stage = 'balance_skipped', exit_reason = 'balance_guard'
--   would_quote_v2        -> stage = 'priced_presubmit'. These rows carry no
--                            quote_id, so submitted-vs-not cannot be decided
--                            here; that classification happens later by joining
--                            the submission log (fct_rfq_lifecycle).
-- (exit_reason literals for the old skip schemas are the values rfq_outcome_v1
-- uses for the same stage.)
--
-- _parse_error rows (truncated lines) are excluded here and counted by the
-- row-count test.
--
-- Legs stay here as a typed list (superset of both eras' leg shapes);
-- stg_rfq_legs unnests and unifies them, so the JSON is parsed only once.

{{ config(materialized='external_parquet', tags=['heavy']) }}

with source as (

    select * from {{ source('rfq', 'rfq_outcomes') }}
    where not _parse_error

),

parsed as (

    select
        _source_key,
        _source_origin,
        _line_no,
        json_transform(raw_json, '{
            "schema": "VARCHAR", "rfq_id": "VARCHAR", "creator_id": "VARCHAR",
            "combo_ticker": "VARCHAR", "collection_ticker": "VARCHAR",
            "ts": "DOUBLE", "received_ts": "DOUBLE", "exit_ts": "DOUBLE",
            "terminal_stage": "VARCHAR", "exit_reason": "VARCHAR",
            "target_cost_dollars": "DOUBLE", "contracts_fp": "DOUBLE",
            "n_legs": "INTEGER", "is_replacement": "BOOLEAN",
            "yes_bid": "DOUBLE", "no_bid": "DOUBLE", "max_payout": "DOUBLE",
            "submit_wire_ts": "DOUBLE", "quote_id": "VARCHAR", "http_status": "INTEGER",
            "error_kind": "VARCHAR",
            "request_bids": {"yes": "DOUBLE", "no": "DOUBLE"},
            "exposure_layer": "VARCHAR",
            "legs": [{
                "market_ticker": "VARCHAR", "side": "VARCHAR", "event_ticker": "VARCHAR",
                "sport": "VARCHAR", "metadata_sport": "VARCHAR",
                "market_type": "VARCHAR", "metadata_market_type": "VARCHAR",
                "game_slug": "VARCHAR", "metadata_status": "VARCHAR",
                "best_bid": "DOUBLE", "best_ask": "DOUBLE",
                "bid_depth": "DOUBLE", "ask_depth": "DOUBLE",
                "book_last_update_ts": "DOUBLE",
                "game_start_ts": "DOUBLE", "metadata_game_start_ts": "DOUBLE",
                "live_at_receipt": "BOOLEAN"
            }]
        }')                                         as j
    from source

),

unified as (

    select
        j.rfq_id,
        j.schema                                            as record_schema,
        case j.schema
            when 'rfq_outcome_v1'        then j.terminal_stage
            when 'exposure_rejected_v2'  then 'priced_rejected'
            when 'breaker_skip_v2'       then 'breaker_skipped'
            when 'balance_guard_skip_v1' then 'balance_skipped'
            when 'would_quote_v2'        then 'priced_presubmit'
        end                                                 as stage,
        j.terminal_stage                                    as terminal_stage_raw,
        case j.schema
            when 'rfq_outcome_v1'        then j.exit_reason
            when 'exposure_rejected_v2'  then j.exposure_layer
            when 'breaker_skip_v2'       then 'breaker'
            when 'balance_guard_skip_v1' then 'balance_guard'
        end                                                 as exit_reason,
        j.schema = 'rfq_outcome_v1'                         as is_v1_schema,

        -- who / what
        j.creator_id,
        j.combo_ticker,                                     -- null in the old schemas (not logged)
        j.collection_ticker,
        coalesce(j.is_replacement, false)                   as is_replacement,  -- key present only when true

        -- requested size: target_cost_dollars XOR contracts_fp
        j.target_cost_dollars,
        j.contracts_fp                                      as contracts_requested,
        coalesce(j.n_legs, len(j.legs))                     as n_legs,  -- old schemas: counted from legs[]

        -- the bot's prices (dollars per contract)
        j.yes_bid                                           as yes_bid_dollars,
        j.no_bid                                            as no_bid_dollars,
        j.max_payout                                        as max_payout_dollars,

        -- submit block (rfq_outcome_v1 'submitted' rows only)
        {{ epoch_to_utc('j.submit_wire_ts') }}              as submit_wire_at_utc,
        nullif(j.quote_id, '')                              as quote_id,
        j.http_status,
        j.error_kind,
        j.request_bids.yes                                  as request_yes_bid_dollars,
        j.request_bids.no                                   as request_no_bid_dollars,

        -- timestamps. The old schemas have no exit_ts; their `ts` is the
        -- record write time, which is the same moment.
        {{ epoch_to_utc('j.received_ts') }}                 as received_at_utc,
        {{ epoch_to_utc('coalesce(j.exit_ts, j.ts)') }}     as exit_at_utc,

        -- legs (unnested in stg_rfq_legs)
        j.legs,

        _source_key,
        _source_origin,
        _line_no

    from parsed

),

-- rfq_ids decided more than once: only in the dual-instance window, when two
-- bot instances priced the same RFQs concurrently (two independent decisions
-- each). Both rows are kept: they are real records, not duplicates.
-- Counted from the extract's typed rfq_id column in a separate narrow scan,
-- so the group-by never has to materialize the parsed JSON.
repeated_rfqs as (

    select rfq_id, count(*) as n_decisions_for_rfq
    from {{ source('rfq', 'rfq_outcomes') }}
    where not _parse_error
    group by rfq_id
    having count(*) > 1

)

select
    unified._source_key || ':' || cast(unified._line_no as varchar) as rfq_outcome_id,
    unified.*,
    coalesce(repeated_rfqs.n_decisions_for_rfq, 1)          as n_decisions_for_rfq,
    {{ utc_to_local('unified.received_at_utc') }}           as received_at_local,
    cast({{ utc_to_local('unified.received_at_utc') }} as date) as received_date_local
from unified
left join repeated_rfqs
    on repeated_rfqs.rfq_id = unified.rfq_id
