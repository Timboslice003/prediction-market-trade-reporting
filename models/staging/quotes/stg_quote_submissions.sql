-- One row per quote-submission record (quote_submission_v1 / v2):
-- phase 'pre' (before the POST), 'post' (the response), or 'skipped' (a
-- standalone record when the submitter declined to send). Grain: attempt_id x
-- phase. success is FALSE on every 'pre' row and NULL on 'skipped' rows:
-- filter on phase before success. Only a successful 'post' carries the
-- quote_id. Before var submission_log_complete_from_date only the archive copy
-- exists and it is missing rows (not fixable; flagged downstream).

{{ config(materialized='external_parquet', tags=['heavy']) }}

with source as (

    select * from {{ source('quotes', 'quote_submissions') }}
    where not _parse_error

),

parsed as (

    select
        _source_key,
        _source_origin,
        _line_no,
        json_transform(raw_json, '{
            "schema": "VARCHAR", "phase": "VARCHAR", "attempt_id": "VARCHAR",
            "ts": "DOUBLE", "rfq_id": "VARCHAR",
            "yes_bid_quantized": "VARCHAR", "no_bid_quantized": "VARCHAR",
            "max_payout_notional": "DOUBLE",
            "request_body": {"yes_bid": "VARCHAR", "no_bid": "VARCHAR",
                             "rest_remainder": "BOOLEAN"},
            "success": "BOOLEAN", "quote_id": "VARCHAR",
            "error_kind": "VARCHAR", "error_detail": "VARCHAR", "http_status": "INTEGER",
            "submitted_ts": "DOUBLE", "response_ts": "DOUBLE",
            "skip_reason": "VARCHAR", "age_ms": "DOUBLE", "max_age_ms": "DOUBLE",
            "in_flight": "INTEGER",
            "latency_sampled": "BOOLEAN", "token_wait_ms": "DOUBLE",
            "network_ms": "DOUBLE", "roundtrip_ms": "DOUBLE"
        }') as j
    from source

)

select
    j.attempt_id || ':' || j.phase                          as submission_id,
    j.attempt_id,
    j.phase,
    j.schema                                                as record_schema,
    j.rfq_id,
    nullif(j.quote_id, '')                                  as quote_id,
    j.success,

    -- what was POSTed (tick-quantized strings)
    cast(j.yes_bid_quantized as decimal(18, 6))             as yes_bid_quantized_dollars,
    cast(j.no_bid_quantized as decimal(18, 6))              as no_bid_quantized_dollars,
    cast(j.request_body.yes_bid as decimal(18, 6))          as request_yes_bid_dollars,
    cast(j.request_body.no_bid as decimal(18, 6))           as request_no_bid_dollars,
    j.request_body.rest_remainder                           as request_rest_remainder,
    {{ to_money('j.max_payout_notional') }}                 as max_payout_notional_dollars,

    -- outcome
    j.http_status,
    j.error_kind,
    j.error_detail,
    j.skip_reason,

    -- staleness guard (skipped paths)
    j.age_ms,
    j.max_age_ms,
    j.in_flight,

    -- sampled latency breakdown
    coalesce(j.latency_sampled, false)                      as is_latency_sampled,
    j.token_wait_ms,
    j.network_ms,
    j.roundtrip_ms,

    -- timestamps. submitted_ts is taken after the rate-limit token is
    -- acquired (a big gap to response_ts is scheduling delay, not network).
    {{ epoch_to_utc('j.ts') }}                              as recorded_at_utc,
    {{ utc_to_local(epoch_to_utc('j.ts')) }}                as recorded_at_local,
    cast({{ utc_to_local(epoch_to_utc('j.ts')) }} as date)  as recorded_date_local,
    {{ epoch_to_utc('j.submitted_ts') }}                    as submitted_at_utc,
    {{ epoch_to_utc('j.response_ts') }}                     as response_at_utc,

    _source_key,
    _source_origin,
    _line_no
from parsed
