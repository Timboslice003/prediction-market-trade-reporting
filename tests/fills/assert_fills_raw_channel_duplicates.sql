-- Trades the raw fill log recorded twice (REST poll, then websocket), stored
-- for review (dbt_test__audit). Warn: they are expected (the settled file
-- dedupes on fill_id, and int_exchange_settlements_attributed counts raw-log
-- fills once), but a jump in their number points at a channel problem.
-- assert_fills_raw_repeats_are_channel_duplicates (error) checks that each one
-- is a clean two-channel pair.
{{ config(severity='warn', store_failures=true) }}
select fill_id, combo_ticker, side, contract_count,
       list(fill_source order by _source_key, _line_no)     as channels,
       list(reverify_path order by _source_key, _line_no)   as reverify_paths,
       min(received_at_utc)                                 as first_received_at_utc
from {{ ref('stg_fills_raw') }}
group by fill_id, combo_ticker, side, contract_count
having count(*) > 1
