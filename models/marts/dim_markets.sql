-- One row per leg market ticker (every market any fill leg touched, plus any
-- market in stg_market_results): the ticker parsed into structured fields,
-- classified by the ticker_prefixes seed, with the exchange's result.
--
-- Ticker shape: {SERIES}.{YYYYMMDD}[T{HHMM}].{MATCHUP}[.{SELECTION}]
--   BSBML.20250309T1310.HAWOWL.HAW          series, date, time 1310, matchup HAWOWL, selection HAW
--   SPDML.20250308.ALBBRV.TIE               no time segment
--   BSBFIR.20250309T1310.HAWOWL             no selection (one market per game)
--   BSBHR.20250309T1310.HAWOWL.HAWMORENO12_1   selection = subject HAWMORENO12, number 1
--   BSBSPR.20250309T1310.HAWOWL.HAW2        spread: subject HAW, number 2
-- Classification comes only from the seed (sport / competition / market_type).
-- A prefix missing from the seed leaves those columns NULL and
-- is_prefix_classified = false; it never becomes "other".
-- assert_leg_ticker_prefixes_in_seed (warn, store_failures) flags it.

{{ config(materialized='table') }}

with tickers as (

    select market_ticker from {{ ref('stg_fill_legs') }}
    union
    select market_ticker from {{ ref('stg_market_results') }}

),

bot_labels as (

    -- the bot's own labels, as logged on the fill legs (most common value)
    select
        market_ticker,
        mode(sport)                                         as bot_sport,
        mode(market_type)                                   as bot_market_type,
        mode(game_slug)                                     as bot_game_slug,
        count(*)                                            as n_fill_legs,
        count(distinct fill_id)                             as n_fills
    from {{ ref('stg_fill_legs') }}
    group by market_ticker

),

parsed as (

    select
        market_ticker,
        split_part(market_ticker, '.', 1)                   as ticker_prefix,
        split_part(market_ticker, '.', 2)                   as date_segment,
        split_part(market_ticker, '.', 3)                   as matchup_code,
        nullif(array_to_string(string_split(market_ticker, '.')[4:], '.'), '')
                                                            as selection
    from tickers

),

segmented as (

    select
        *,
        date_segment || '.' || matchup_code                                as event_segment,
        regexp_extract(date_segment, '^(\d{8})', 1)                        as date_code,
        nullif(regexp_extract(date_segment, '^\d{8}T(\d{4})$', 1), '')     as time_code
    from parsed

)

select
    s.market_ticker,
    s.ticker_prefix,
    seed.ticker_prefix is not null                          as is_prefix_classified,
    seed.sport,
    seed.competition,
    seed.market_type,
    -- event
    coalesce(r.event_ticker, s.ticker_prefix || '.' || s.event_segment) as event_ticker,
    s.event_segment,
    coalesce(seed.competition, s.ticker_prefix) || ':' || s.event_segment as game_key,
    try_strptime(s.date_code, '%Y%m%d')::date               as ticker_event_date,
    s.time_code                                             as ticker_event_time_code,
    s.matchup_code,
    r.occurrence_at_utc,
    {{ utc_to_local('r.occurrence_at_utc') }}               as occurrence_at_local,
    -- selection
    s.selection,
    case
        when s.selection like '%\_%' escape '\'  then regexp_extract(s.selection, '^(.*)_[^_]*$', 1)
        when regexp_full_match(s.selection, '\d+') then null
        when seed.market_type = 'spread'        then regexp_extract(s.selection, '^(.*?)\d+$', 1)
        else s.selection
    end                                                     as selection_subject,
    try_cast(case
        when s.selection like '%\_%' escape '\'  then regexp_extract(s.selection, '_(\d+)$', 1)
        when regexp_full_match(s.selection, '\d+') then s.selection
        when seed.market_type = 'spread'        then regexp_extract(s.selection, '(\d+)$', 1)
    end as integer)                                         as selection_number,
    r.title,
    r.yes_sub_title,
    r.no_sub_title,
    -- result (the exchange)
    r.market_result,
    r.result_bucket,
    r.market_result = 'scalar'                              as is_scalar,
    r.settlement_value_dollars,
    r.market_status,
    r.settled_at_utc,
    r.settled_at_local,
    r.closed_at_utc,
    -- the bot's own labels, for comparison
    b.bot_sport,
    b.bot_market_type,
    b.bot_game_slug,
    coalesce(b.n_fill_legs, 0)                              as n_fill_legs,
    coalesce(b.n_fills, 0)                                  as n_fills
from segmented as s
left join {{ ref('ticker_prefixes') }} as seed
    on s.ticker_prefix = seed.ticker_prefix
left join {{ ref('stg_market_results') }} as r
    on s.market_ticker = r.market_ticker
left join bot_labels as b
    on s.market_ticker = b.market_ticker
