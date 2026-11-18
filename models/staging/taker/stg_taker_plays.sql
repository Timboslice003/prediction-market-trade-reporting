-- One row per attempt the taker script logged (TAKE or PASS), from each
-- strategy's plays.csv. Schema-era unification only: the two strategies'
-- files have different headers, so each value is looked up by its column name
-- in that file's header; strategy-specific columns are null for the other
-- strategy. The files contain no quote characters and every row has its
-- header's field count (is_field_count_ok, tested), so a plain comma split is
-- exact.

with files as (

    select
        regexp_replace(replace(filename, chr(92), '/'), '^.*/taker/', 'taker/') as _source_key,
        regexp_extract(replace(filename, chr(92), '/'), '/taker/([^/]+)/', 1)  as strategy,
        string_split(rtrim(replace(content, chr(13), ''), chr(10)), chr(10))   as lines
    from {{ source('taker', 'taker_plays_files') }}

),

lines as (

    select
        _source_key,
        strategy,
        string_split(lines[1], ',')                         as header,
        unnest(lines[2:])                                   as line,
        unnest(generate_series(2, len(lines)))              as _line_no
    from files

),

picked as (

    select
        _source_key,
        strategy,
        _line_no,
        len(string_split(line, ',')) = len(header)          as is_field_count_ok,
        map_from_entries(list_transform(
            list_zip(header, string_split(line, ',')),
            lambda x: {'k': x[1], 'v': nullif(x[2], '')}
        ))                                                  as r
    from lines

)

select
    strategy || ':' || _source_key || ':' || cast(_line_no as varchar) as taker_play_id,
    strategy,
    {{ iso_to_utc("r['ts_utc']") }}                         as evaluated_at_utc,
    {{ utc_to_local(iso_to_utc("r['ts_utc']")) }}           as evaluated_at_local,
    cast(r['date'] as date)                                 as game_date,
    r['game']                                               as game,
    r['combo_ticker']                                       as combo_ticker,
    r['rfq_id']                                             as rfq_id,
    r['quote_id']                                           as quote_id,
    lower(r['action'])                                      as action,
    cast(r['contracts'] as double)                          as contract_count,
    cast(r['cost'] as decimal(18, 6))                       as cost_dollars,
    -- taker_a only
    cast(r['n_quotes'] as integer)                          as n_quotes,
    -- taker_b only
    cast(r['slot'] as integer)                              as slot,
    cast(r['best_price'] as decimal(18, 6))                 as best_price_dollars,
    is_field_count_ok,
    _source_key,
    'host_copy'                                             as _source_origin,
    _line_no
from picked
