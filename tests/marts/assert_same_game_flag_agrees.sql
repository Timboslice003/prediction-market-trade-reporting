-- Combos where the bot's own same-game flag disagrees with the ticker-based
-- one (dim_combos.is_same_game), stored for review. Warn: the ticker-based
-- flag is the one the marts use. A disagreement means the bot missed (or
-- invented) a same-game pair, e.g. because it logged a leg with no game slug,
-- so any same-game exposure limit of the bot would not have seen it.
{{ config(severity='warn', store_failures=true) }}
select combo_ticker, is_same_game, bot_is_same_game_parlay, n_legs, n_games, leg_market_tickers
from {{ ref('dim_combos') }}
where bot_is_same_game_parlay is not null
  and bot_is_same_game_parlay <> is_same_game
