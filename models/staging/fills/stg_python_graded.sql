-- The legacy Python settlement script's grading of each fill, keyed by
-- fill_id. Kept apart from stg_fills so dbt's own grading (int_fills_graded)
-- can be compared against it in recon_fills_python_vs_dbt and nowhere else.
-- Gate on parlay_result, never on `settled` (a dead flag).

with source as (

    select * from {{ source('fills', 'fills_settled') }}
    where not _parse_error

),

parsed as (

    select
        _source_key,
        _source_origin,
        _line_no,
        json_transform(raw_json, '{
            "fill_id": "VARCHAR", "parlay_result": "VARCHAR", "revenue": "DOUBLE",
            "realized_pnl": "DOUBLE", "settled": "BOOLEAN",
            "is_manually_fixed": "BOOLEAN", "manual_fix_note": "VARCHAR",
            "legs_settled": [{"market_ticker": "VARCHAR", "side": "VARCHAR",
                              "result": "VARCHAR", "won": "BOOLEAN"}]
        }') as j
    from source

)

select
    j.fill_id,
    j.parlay_result,
    {{ to_money('j.revenue') }}             as revenue_dollars,
    {{ to_money('j.realized_pnl') }}        as realized_pnl_dollars,
    j.settled                               as settled_legacy_flag,
    coalesce(j.is_manually_fixed, false)    as is_manually_fixed,
    j.manual_fix_note,
    j.legs_settled,
    len(j.legs_settled)                     as n_legs_settled,
    _source_key,
    _source_origin,
    _line_no
from parsed
