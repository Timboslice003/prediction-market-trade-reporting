-- Dagster: shown as an asset check on mart_daily_trade_report (orchestration/, docs/ORCHESTRATION.md).
{{ config(meta={'dagster': {'ref': {'name': 'mart_daily_trade_report'}}}) }}
-- notional = quantity x price on every trade (to the cent, as price is
-- stored at 6 dp), and the trade report's notional + folded fee = fill_cost.
select r.trade_id, r.quantity, r.price, r.notional
from {{ ref('mart_daily_trade_report') }} as r
join {{ ref('fct_fills') }} as f on f.fill_id = r.trade_id
where abs(r.quantity * r.price - r.notional) >= 0.005
   or abs(r.notional + case when f.is_fee_in_fill_cost then f.fee_dollars else 0 end - f.fill_cost_dollars) >= 0.000001
