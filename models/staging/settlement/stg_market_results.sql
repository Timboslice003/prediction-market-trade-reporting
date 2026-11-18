-- One row per leg market: the exchange's result for every leg any fill touched
-- (plus the account's single-market trades). Some legs settle "scalar": the
-- exchange voided them and paid a fractional value. The extract buckets those
-- as void and leaves the typed `result` column null; here market_result keeps
-- the exchange's own value (yes / no / scalar) and is_void marks the bucket.
-- The file has no lineage columns, so _source_key is the file and _line_no
-- the Parquet row number (1-based).

with source as (

    select * from {{ source('settlement', 'market_results') }}

),

parsed as (

    select
        *,
        json_transform(raw_json, '{
            "ticker": "VARCHAR", "event_ticker": "VARCHAR", "market_type": "VARCHAR",
            "status": "VARCHAR", "result": "VARCHAR", "title": "VARCHAR",
            "yes_sub_title": "VARCHAR", "no_sub_title": "VARCHAR",
            "expiration_value": "VARCHAR", "settlement_value_dollars": "VARCHAR",
            "settlement_ts": "VARCHAR", "open_time": "VARCHAR", "close_time": "VARCHAR",
            "occurrence_datetime": "VARCHAR", "last_price_dollars": "VARCHAR",
            "volume_fp": "VARCHAR", "open_interest_fp": "VARCHAR"
        }') as j
    from source

)

select
    market_ticker,
    j.event_ticker,
    j.market_type,
    j.title,
    j.yes_sub_title,
    j.no_sub_title,
    j.status                                                as market_status,
    j.result                                                as market_result,
    bucket                                                  as result_bucket,
    bucket = 'void'                                         as is_void,
    j.expiration_value,
    cast(nullif(j.settlement_value_dollars, '') as decimal(18, 6)) as settlement_value_dollars,
    {{ iso_to_utc('j.settlement_ts') }}                     as settled_at_utc,
    {{ utc_to_local(iso_to_utc('j.settlement_ts')) }}       as settled_at_local,
    {{ iso_to_utc('j.open_time') }}                         as opened_at_utc,
    {{ iso_to_utc('j.close_time') }}                        as closed_at_utc,
    {{ iso_to_utc('j.occurrence_datetime') }}               as occurrence_at_utc,
    cast(nullif(j.last_price_dollars, '') as decimal(18, 6)) as last_price_dollars,
    cast(nullif(j.volume_fp, '') as double)                 as volume_contracts,
    cast(nullif(j.open_interest_fp, '') as double)          as open_interest_contracts,
    {{ epoch_to_utc('fetched_ts') }}                        as fetched_at_utc,
    in_original_cache                                       as is_from_original_cache,
    'market_results/' || parse_filename(filename)           as _source_key,
    file_row_number + 1                                     as _line_no
from parsed
