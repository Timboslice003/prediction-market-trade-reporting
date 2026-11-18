-- One trade-report row per fill, no more, no less.
select
    (select count(*) from {{ ref('mart_daily_trade_report') }}) as report_rows,
    (select count(*) from {{ ref('fct_fills') }})               as fills
where (select count(*) from {{ ref('mart_daily_trade_report') }})
   <> (select count(*) from {{ ref('fct_fills') }})
