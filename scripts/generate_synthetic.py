"""Generate the synthetic dataset this project runs on.

    python scripts/generate_synthetic.py [--out data/synthetic] [--seed 20250303]

Writes Parquet in exactly the layout the dbt sources read (models/staging/_sources.yml):

    <out>/<table>/month=YYYY-MM/<source-file-stem>.parquet   one file per source log file
    <out>/market_results/market_settlements.parquet         one file
    <out>/taker/<strategy>/plays.csv, log.jsonl              raw text ledgers
    <out>/_manifest/manifest.parquet                         one row per source file
    <out>/_manifest/expectations.parquet                     the generator's ground truth

Everything is deterministic from --seed. Nothing here is drawn from real data:
prices, margins, sizes, fees, volumes and team / player names are invented
with simple uniform or Poisson draws.

The scenario: an automated market maker answers RFQs (requests for quote) on
multi-leg parlays at an RFQ-based prediction market exchange, for four weeks
(2025-03-03 .. 2025-03-30 UTC). Its logs carry the breaks a real pipeline has
to survive; each one is planted on purpose and listed in README.md with the
test or model that catches it:

  1. duplicate fills over two channels      raw fill log repeats a trade (REST poll + websocket)
  2. scalar settlements, incl. a NO side    leg markets voided at a fractional value
  3. fee folded into cost / unknown fee     settle-time-enriched fills; estimate-cost fills
  4. pagination bug drops settlements       the reconciler reads one page per poll
  5. dual-instance window                   two bot processes decide the same RFQs
  6. end-of-hour decision loss              a logging bug drops the tail of some hours
  7. missing creator id                     the exchange hashes an empty id
  8. same-game combo the bot's flag missed  a leg with no game slug
  9. manual trades and opposite-side hedges in the account's settlement feed

The expectations file holds what the generator knows to be true (planted
counts, the independently computed P&L). Only tests read it; no model does.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import os
import random
import shutil
import tempfile
import uuid
from collections import defaultdict
from decimal import ROUND_FLOOR, Decimal

import duckdb

# --------------------------------------------------------------------------- timeline (UTC, naive)
START = dt.datetime(2025, 3, 3)
END = dt.datetime(2025, 3, 31)                       # exclusive: no RFQs / fills after this
LOCAL_OFFSET_H = -5                                   # the reporting day: a fixed UTC-5 clock
FILLS_SETTLED_FROM = dt.datetime(2025, 3, 5)          # the settled fill file starts here
SUBMISSION_LOG_COMPLETE_FROM = dt.datetime(2025, 3, 10)  # before: archive copy only, rows missing
DUAL_START = dt.datetime(2025, 3, 7, 18, 13)          # second bot instance running
DUAL_END = dt.datetime(2025, 3, 7, 18, 43)
ERA_CUTOVER = dt.datetime(2025, 3, 12, 12, 0)         # old decision schemas -> rfq_outcome_v1
LEDGER_WRITE_LOST_POLL = dt.datetime(2025, 3, 12, 6, 10)  # its batch reaches the settlement log, not the ledger
LEDGER_RESTART = dt.datetime(2025, 3, 12, 6, 30)      # a deploy restarts the loss-budget cumulative at 0
SETTLEMENT_LOG_GAP = (dt.datetime(2025, 3, 13, 3, 0), dt.datetime(2025, 3, 13, 7, 0))  # settlement log lost; ledger kept
TAIL_LOSS_UNTIL = dt.datetime(2025, 3, 24)            # end-of-hour loss bug fixed
WINDOW_START = dt.datetime(2025, 3, 17, 5)            # completeness window: local 03-17..03-23
WINDOW_END = dt.datetime(2025, 3, 24, 5)
BOT_OFF = (dt.datetime(2025, 3, 19, 8, 0), dt.datetime(2025, 3, 19, 11, 20))
PRICER_DOWN = (dt.datetime(2025, 3, 21, 20, 40), dt.datetime(2025, 3, 21, 22, 30))
RATE_LIMIT_BURST = (dt.datetime(2025, 3, 20, 19, 0), dt.datetime(2025, 3, 20, 20, 30))
LATENCY_SPIKE_LOCAL_DATE = dt.date(2025, 3, 25)
PRICE_CHECK_FLOOD_LOCAL_DATE = dt.date(2025, 3, 27)
BRIDGE_END_BEFORE = dt.datetime(2025, 3, 29)
LEDGER_LAST = dt.datetime(2025, 3, 30, 12, 0)         # the reconciler's last poll
POLL_MINUTES = 10
PAGE_SIZE = 12                                        # settlements read per poll (the bug)
BATCH_HOURS = 2                                       # the exchange settles combos in batches
FETCHED_AT = dt.datetime(2025, 4, 6, 9, 0)            # market results fetched

ARCHIVE, HOST = "archive", "host_copy"

# --------------------------------------------------------------------------- reference data (invented)
SPORTS = {
    # sport: (competition, teams, local start times, duration h, days-of-week (0=Mon), games per slot, has_time)
    "baseball": ("Baseball Premier", ["HAW", "OWL", "FOX", "BEA", "CRW", "ELK", "PIK", "RAM", "SKY", "TID", "WOL", "YAK"],
                 ["13:05", "16:10", "19:10"], 3.0, range(7), 2, True),
    "basketball": ("Basketball Premier", ["ARC", "BLZ", "CYC", "DRF", "EMB", "FLR", "GLD", "HVN"],
                   ["19:00", "21:30"], 2.5, range(7), 2, True),
    "hockey": ("Hockey Premier", ["ICR", "JLT", "KNG", "LNX", "MRA", "NVA"], ["19:00"], 2.5, range(7), 3, True),
    "football": ("Football Premier", ["ANV", "BRK", "CMT", "DST", "EGL", "FRG", "GRN", "HMR", "IRN", "JGR", "KRK", "LNC"],
                 ["13:00", "16:25"], 3.25, [6], 3, True),
    "soccer_pd": ("Soccer Premier Division", ["ALB", "BRV", "CAS", "DUN", "ESX", "FRN", "GLN", "HTF", "IVR", "JRD"],
                  ["07:30", "10:00"], 2.0, [5, 6], 2, False),
    "soccer_cc": ("Soccer Continental Cup", ["KOR", "LUM", "MAR", "NOR", "OSK", "PAV"], ["15:00"], 2.0, [1, 2], 3, False),
    "tennis": ("Tennis World Tour", ["AVERIN", "BOLDT", "CARVAL", "DUMONT", "ESPADA", "FERRO", "GALLO", "HALVOR",
                                     "IKONEN", "JARRET", "KOVAL", "LINDQ"], ["06:00", "09:30", "13:00"], 2.5, range(7), 2, False),
}
SPORT_LABEL = {"soccer_pd": "soccer", "soccer_cc": "soccer"}
SURNAMES = ["MORENO", "OKAFOR", "LINDBERG", "TANAKA", "ABARA", "VOSS", "QUINTERO", "HALE", "NAKAMURA", "PETROV",
            "SILVA", "DUARTE", "KESTER", "ROWE", "ADEYEMI", "BRANDT"]

# series prefix -> (sport key, market_type, selection kind)
SERIES = {
    "BSBML": ("baseball", "moneyline", "team"), "BSBTOT": ("baseball", "total", "number"),
    "BSBSPR": ("baseball", "spread", "team_number"), "BSBHR": ("baseball", "prop_hr", "player"),
    "BSBFIR": ("baseball", "prop_first_inning_run", "none"),
    "BKBML": ("basketball", "moneyline", "team"), "BKBTOT": ("basketball", "total", "number"),
    "BKBPTS": ("basketball", "prop_pts", "player"),
    "HCKML": ("hockey", "moneyline", "team"), "HCKTOT": ("hockey", "total", "number"),
    "FTBML": ("football", "moneyline", "team"), "FTBTOT": ("football", "total", "number"),
    "FTBSPR": ("football", "spread", "team_number"), "FTBTD": ("football", "prop_td", "player"),
    "SPDML": ("soccer_pd", "moneyline", "team_tie"), "SPDTOT": ("soccer_pd", "total", "number"),
    "SPDBTTS": ("soccer_pd", "btts", "btts"),
    "SCCML": ("soccer_cc", "moneyline", "team_tie"), "SCCADV": ("soccer_cc", "advance", "team"),
    "TNSML": ("tennis", "moneyline", "team"),
}
TOTAL_LINES = {"baseball": [7, 8, 9], "basketball": [210, 220], "hockey": [5, 6], "football": [41, 47],
               "soccer_pd": [2, 3]}

D = Decimal
CENT = D("0.01")


def money(x) -> Decimal:
    return D(x).quantize(D("0.000001"))


def floor_cent(x: Decimal) -> Decimal:
    return x.quantize(CENT, rounding=ROUND_FLOOR)


def epoch(t: dt.datetime) -> float:
    return round((t - dt.datetime(1970, 1, 1)).total_seconds(), 6)


def iso(t: dt.datetime) -> str:
    return t.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


def local(t: dt.datetime) -> dt.datetime:
    return t + dt.timedelta(hours=LOCAL_OFFSET_H)


def sha(s: str) -> str:
    return hashlib.sha256(s.encode()).hexdigest()


def stable_int(*parts) -> int:
    """Deterministic across runs (Python's hash() of a str is not)."""
    return int(hashlib.md5("|".join(map(str, parts)).encode()).hexdigest()[:12], 16)


class Gen:
    def __init__(self, seed: int):
        self.r = random.Random(seed)

    def uid(self) -> str:
        return str(uuid.UUID(int=self.r.getrandbits(128), version=4))

    def u(self, a, b):
        return self.r.uniform(a, b)

    def chance(self, p):
        return self.r.random() < p

    def pick(self, xs):
        return self.r.choice(xs)


# =========================================================================== markets
class Market:
    __slots__ = ("ticker", "series", "sport", "competition", "market_type", "game", "selection", "result",
                 "value", "settled_at", "title", "yes_sub", "no_sub", "event_ticker")


class Game:
    __slots__ = ("sport", "competition", "start", "home", "away", "date_code", "matchup", "markets", "end")


def build_markets(g: Gen):
    games, markets = [], {}
    day = START.date() - dt.timedelta(days=1)
    last = END.date() + dt.timedelta(days=2)
    while day <= last:
        for skey, (comp, teams, slots, dur, dows, per_slot, has_time) in SPORTS.items():
            if day.weekday() not in dows:
                continue
            pool = teams[:]
            g.r.shuffle(pool)
            for slot in slots:
                for _ in range(per_slot):
                    if len(pool) < 2:
                        break
                    home, away = pool.pop(), pool.pop()
                    hh, mm = map(int, slot.split(":"))
                    start_local = dt.datetime.combine(day, dt.time(hh, mm))
                    gm = Game()
                    gm.sport, gm.competition = skey, comp
                    gm.start = start_local - dt.timedelta(hours=LOCAL_OFFSET_H)
                    gm.end = gm.start + dt.timedelta(hours=dur)
                    gm.home, gm.away = home, away
                    gm.date_code = day.strftime("%Y%m%d") + (f"T{hh:02d}{mm:02d}" if has_time else "")
                    gm.matchup = (home + away) if skey != "tennis" else (home[:3] + away[:3])
                    gm.markets = []
                    games.append(gm)
                    for m in game_markets(g, gm):
                        markets[m.ticker] = m
                        gm.markets.append(m)
        day += dt.timedelta(days=1)
    return games, markets


def game_markets(g: Gen, gm: Game):
    out = []
    home_wins = g.chance(0.52)
    tie = gm.sport.startswith("soccer") and g.chance(0.25)

    def mk(series, selection, result, title):
        m = Market()
        m.series, m.selection = series, selection
        m.sport = SPORT_LABEL.get(gm.sport, gm.sport)
        m.competition = gm.competition
        m.market_type = SERIES[series][1]
        m.game = gm
        m.event_ticker = f"{series}.{gm.date_code}.{gm.matchup}"
        m.ticker = m.event_ticker + (f".{selection}" if selection else "")
        m.result, m.value = result, D(1) if result == "yes" else D(0)
        m.settled_at = gm.end + dt.timedelta(minutes=g.u(10, 60))
        m.title, m.yes_sub, m.no_sub = title, "Yes", "No"
        out.append(m)

    s = gm.sport
    for series, (sk, mtype, kind) in SERIES.items():
        if sk != s:
            continue
        vs = f"{gm.away} at {gm.home}"
        if kind == "team":
            if s == "tennis":
                mk(series, gm.home, "yes" if home_wins else "no", f"{gm.home} vs {gm.away}: {gm.home} wins")
                mk(series, gm.away, "no" if home_wins else "yes", f"{gm.home} vs {gm.away}: {gm.away} wins")
            elif mtype == "advance":
                mk(series, gm.home, "yes" if home_wins else "no", f"{vs}: {gm.home} advances")
            else:
                mk(series, gm.home, "yes" if home_wins and not tie else "no", f"{vs}: {gm.home} wins")
                mk(series, gm.away, "yes" if (not home_wins) and not tie else "no", f"{vs}: {gm.away} wins")
        elif kind == "team_tie":
            mk(series, gm.home, "yes" if home_wins and not tie else "no", f"{vs}: {gm.home} wins")
            mk(series, gm.away, "yes" if (not home_wins) and not tie else "no", f"{vs}: {gm.away} wins")
            mk(series, "TIE", "yes" if tie else "no", f"{vs}: draw")
        elif kind == "number":
            for n in TOTAL_LINES[s]:
                mk(series, str(n), "yes" if g.chance(0.48) else "no", f"{vs}: over {n - 0.5} total")
        elif kind == "team_number":
            n = g.pick([2, 3, 7])
            mk(series, f"{gm.home}{n}", "yes" if g.chance(0.45) else "no", f"{vs}: {gm.home} wins by over {n - 0.5}")
        elif kind == "player":
            for team in (gm.home, gm.away):
                name = g.pick(SURNAMES)
                mk(series, f"{team}{name}{g.r.randint(2, 44)}_1", "yes" if g.chance(0.3) else "no",
                   f"{vs}: {name.title()} ({team}) 1+")
        elif kind == "btts":
            mk(series, "BTTS", "yes" if g.chance(0.5) else "no", f"{vs}: both teams score")
        elif kind == "none":
            mk(series, None, "yes" if g.chance(0.5) else "no", f"{vs}: run in the first inning")
    return out


# =========================================================================== RFQs, decisions, quotes, fills
class Combo:
    __slots__ = ("ticker", "legs", "collection", "first_start", "settled_at", "no_slug_leg")

    # legs: list of [market, side]


def new_combo(g: Gen, legs):
    c = Combo()
    c.ticker = "CMB." + g.uid().replace("-", "")[:12].upper()
    c.legs = legs
    games = [m.game for m, _ in legs]
    c.collection = "COLL.SAMEGAME" if len(set(map(id, games))) < len(games) else "COLL.MULTI"
    c.first_start = min(gm.start for gm in games)
    c.settled_at = None
    c.no_slug_leg = None
    return c


def combo_outcome(c: Combo):
    """('yes'|'no'|'scalar', parlay YES value)."""
    value, any_lost, any_scalar = D(1), False, False
    for m, side in c.legs:
        if m.result == "scalar":
            any_scalar = True
            value *= m.value if side == "yes" else 1 - m.value
        elif m.result != side:
            any_lost = True
            value = D(0)
    if any_lost:
        return "no", D(0)
    if any_scalar:
        return "scalar", value
    return "yes", D(1)


class World:
    pass


def generate(seed: int):
    g = Gen(seed)
    w = World()
    w.games, w.markets = build_markets(g)
    games_by_start = sorted(w.games, key=lambda x: x.start)

    maker_id = sha("synthetic-maker-0")
    other_makers = [sha(f"synthetic-maker-{i}") for i in range(1, 4)]
    account_creator = sha("synthetic-account-taker")
    requesters = [sha(f"synthetic-requester-{i}") for i in range(1, 11)]
    price_checker = requesters[0]
    empty_creator = sha("")
    req_weights = [0.22] + [0.07] * 9 + [0.0]
    w.requesters = requesters

    def requester(day_local):
        x = g.r.random()
        if x < 0.05:
            return empty_creator
        if day_local == PRICE_CHECK_FLOOD_LOCAL_DATE and x < 0.55:
            return price_checker
        return g.r.choices(requesters, weights=req_weights[:10])[0]

    def pick_legs(t):
        upcoming = [gm for gm in games_by_start if t + dt.timedelta(minutes=5) < gm.start < t + dt.timedelta(hours=30)]
        if len(upcoming) < 3:
            return None
        n = g.r.choices([2, 3, 4, 5], weights=[0.35, 0.33, 0.2, 0.12])[0]
        legs, used = [], set()
        if g.chance(0.35):
            gm = g.pick(upcoming)
            for m in g.r.sample(gm.markets, min(2, len(gm.markets))):
                legs.append([m, "yes" if g.chance(0.75) else "no"])
                used.add(m.ticker)
        tries = 0
        while len(legs) < n and tries < 50:
            tries += 1
            gm = g.pick(upcoming)
            m = g.pick(gm.markets)
            if m.ticker in used or any(x.game is gm for x, _ in legs):
                continue
            legs.append([m, "yes" if g.chance(0.75) else "no"])
            used.add(m.ticker)
        return legs if len(legs) >= 2 else None

    # ---- exchange RFQ stream
    rfqs = []
    t = START
    open_combos = []
    while t < END:
        hour_local = local(t).hour
        diurnal = 0.35 + 1.25 * (1 if (hour_local >= 11 or hour_local < 1) else 0)
        lam = 17.0 * diurnal * (1.6 if local(t).date() == PRICE_CHECK_FLOOD_LOCAL_DATE else 1.0)
        n = poisson(g, lam)
        for _ in range(n):
            created = t + dt.timedelta(seconds=g.u(0, 3600))
            if created >= END:
                continue
            open_combos = [c for c in open_combos if c.first_start > created + dt.timedelta(minutes=5)]
            if open_combos and g.chance(0.22):
                combo = g.pick(open_combos)
            else:
                legs = pick_legs(created)
                if legs is None:
                    continue
                combo = new_combo(g, legs)
                open_combos.append(combo)
            rfq = World()
            rfq.rfq_id = g.uid()
            rfq.created = created
            rfq.creator = requester(local(created).date())
            rfq.combo = combo
            if g.chance(0.6):
                rfq.contracts, rfq.target_cost = money(round(g.u(10, 500), 2)), None
            else:
                rfq.contracts, rfq.target_cost = None, money(round(g.u(5, 200), 2))
            rfq.filtered = g.chance(0.2)
            rfqs.append(rfq)
        t += dt.timedelta(hours=1)
    rfqs.sort(key=lambda x: x.created)
    w.rfqs = rfqs

    def bot_state(t):
        if BOT_OFF[0] <= t < BOT_OFF[1]:
            return "off"
        if PRICER_DOWN[0] <= t < PRICER_DOWN[1]:
            return "pricer_down"
        return "on"

    # ---- decisions + quote submissions
    decisions, attempts = [], []   # dicts, written later
    live_quotes = []               # (decision, quote_id, submitted_at, yes_bid, no_bid, max_payout, rfq)
    for rfq in rfqs:
        st = bot_state(rfq.created)
        rfq.bot_state = st
        if st != "on" or rfq.filtered:
            continue
        instances = ["A"]
        if DUAL_START <= rfq.created < DUAL_END:
            instances = ["A", "B"]
        for inst in instances:
            dec = decide(g, rfq, inst, maker_id)
            decisions.append(dec)
            for att in dec["attempts"]:
                attempts.append(att)
            if dec.get("live_quote_id"):
                live_quotes.append(dec)

    # ---- fills on live quotes
    fills = []
    for dec in live_quotes:
        rfq = dec["rfq"]
        if rfq.combo.first_start <= dec["submitted_at"] + dt.timedelta(seconds=30):
            continue
        p = 0.002 if rfq.creator == price_checker else (0.2 if rfq.creator == empty_creator else 0.28)
        if rfq.created >= DUAL_START and rfq.created < DUAL_END:
            p = 0.0          # planted below: only the later instance fills, on purpose
        if not g.chance(p):
            continue
        n_fills = 2 if g.chance(0.04) else 1
        for k in range(n_fills):
            fills.append(make_fill(g, dec, k))
    # dual window: fill the LATER instance's quote on two RFQs
    dual = defaultdict(list)
    for dec in live_quotes:
        if DUAL_START <= dec["rfq"].created < DUAL_END:
            dual[dec["rfq"].rfq_id].append(dec)
    planted_dual = 0
    for rid, decs in sorted(dual.items(), key=lambda kv: kv[1][0]["received_at"]):
        if len(decs) == 2 and planted_dual < 2:
            later = max(decs, key=lambda d: d["received_at"])
            if later["rfq"].combo.first_start > later["submitted_at"] + dt.timedelta(minutes=1):
                fills.append(make_fill(g, later, 0))
                planted_dual += 1
    w.planted_dual_fills = planted_dual

    # ---- plant: scalar settlements (incl. NO-side fills); see module docstring
    plant_scalars(g, w, fills)

    # ---- plant: same-game combos whose bot flag missed (a leg logged with no game slug)
    sg = [f for f in fills if f["combo"].collection == "COLL.SAMEGAME" and f["quote_id"] and not f.get("scalar_spec")
          and f["accepted_at"] >= FILLS_SETTLED_FROM + dt.timedelta(days=1)]
    missed = []
    for f in sg:
        c = f["combo"]
        if c.ticker in {m.ticker for m in missed}:
            continue
        same = defaultdict(list)
        for i, (m, _) in enumerate(c.legs):
            same[id(m.game)].append(i)
        pair = next((v for v in same.values() if len(v) == 2), None)
        if pair and len(c.legs) == 2:
            c.no_slug_leg = pair[1]
            missed.append(c)
            for f2 in fills:                     # logged by the websocket path, so the bot's flag is set
                if f2["combo"] is c:
                    f2["poll_discovered"] = False
        if len(missed) == 2:
            break
    w.same_game_missed = [c.ticker for c in missed]

    # ---- fill attributes: channels, cost source, fee, ids
    for f in fills:
        finish_fill(g, f)
    fills.sort(key=lambda f: f["accepted_at"])

    # ---- taker-script fills (the account as requester) + the self-trade-looking fill
    taker = make_taker(g, w, games_by_start, fills, other_makers, account_creator)
    fills.extend(taker["fills"])
    fills.sort(key=lambda f: f["accepted_at"])

    w.decisions, w.attempts, w.fills = decisions, attempts, fills
    w.taker = taker
    w.maker_id, w.other_makers, w.empty_creator = maker_id, other_makers, empty_creator

    # ---- python settler's answer
    for f in fills:
        python_grade(f)

    # ---- settlements, ledger
    settle(g, w)
    return w


def poisson(g: Gen, lam):
    L, k, p = math.exp(-lam), 0, 1.0
    while True:
        p *= g.r.random()
        if p <= L:
            return k
        k += 1


def decide(g: Gen, rfq, inst, maker_id):
    """One pricing decision (+ its submission attempts) for an RFQ."""
    # the second instance is a cold, lagging process: it sees each RFQ 0.6-0.9 s after the first
    received = rfq.created + dt.timedelta(seconds=g.u(0.02, 0.3)) + (dt.timedelta(milliseconds=g.u(600, 900)) if inst == "B" else dt.timedelta())
    post_era = received >= ERA_CUTOVER
    legs = rfq.combo.legs
    k = len(legs)
    ref = min(0.9, max(0.01, 0.5 ** k * g.u(0.8, 1.25)))
    m = g.u(0.02, 0.06)
    yes_bid = max(D("0.01"), D(str(round(ref - m, 2))))
    no_bid = max(D("0.01"), D(str(round(1 - ref - m, 2))))
    if rfq.contracts is not None:
        max_payout = rfq.contracts
    else:
        max_payout = money((rfq.target_cost / max(no_bid, D("0.05"))).quantize(CENT))
    latency_ms = g.r.lognormvariate(math.log(90), 0.6)
    if local(received).date() == LATENCY_SPIKE_LOCAL_DATE:
        latency_ms *= 9
    dec = {"rfq": rfq, "instance": inst, "received_at": received, "post_era": post_era,
           "yes_bid": yes_bid, "no_bid": no_bid, "max_payout": max_payout, "attempts": [],
           "live_quote_id": None, "submitted_at": None}
    if post_era:
        stage = g.r.choices(["submitted", "priced_rejected", "gated_skipped", "priced_logged", "breaker_skipped",
                             "balance_skipped"], weights=[60, 14, 14, 6, 3, 3])[0]
    else:
        stage = g.r.choices(["priced_presubmit", "priced_rejected", "breaker_skipped", "balance_skipped"],
                            weights=[70, 20, 5, 5])[0]
    dec["stage"] = stage
    exit_reason = error_kind = http_status = quote_id = None
    submit_at = received + dt.timedelta(milliseconds=latency_ms)
    exit_at = submit_at + dt.timedelta(milliseconds=g.u(20, 120))
    if stage in ("submitted", "priced_presubmit"):
        in_burst = RATE_LIMIT_BURST[0] <= received < RATE_LIMIT_BURST[1]
        x = g.r.random()
        if in_burst and x < 0.35:
            kind = "rate_limited"
        elif x < 0.88:
            kind = "live"
        elif x < 0.95:
            kind = "dropped"
        elif x < 0.995:
            kind = g.r.choices(["client_error", "server_error", "rate_limited", "auth"], weights=[75, 15, 8, 2])[0]
        else:
            kind = "network"
        dec["send_kind"] = kind
        status = {"client_error": g.pick([409, 409, 409, 400]), "server_error": g.pick([500, 503, 504]),
                  "rate_limited": 429, "auth": g.pick([401, 403])}
        if kind == "live":
            quote_id = g.uid()
            http_status = 200
        elif kind == "dropped":
            exit_reason = g.pick(["drop_stale_predicted", "both_sides_gated", "drop_stale_age_exceeded", "subpenny_both_bids"])
        elif kind == "network":
            exit_reason = error_kind = "network"
        else:
            exit_reason = error_kind = kind
            http_status = status[kind]
        dec["attempts"] = attempts_for(g, dec, kind, quote_id, http_status, error_kind, exit_reason, submit_at)
        if kind == "live":
            dec["live_quote_id"] = quote_id
            dec["submitted_at"] = submit_at
    elif stage == "priced_rejected":
        exit_reason = g.pick(["limit_per_game", "limit_total_open", "limit_per_ticker", "limit_per_team"])
    elif stage == "gated_skipped":
        exit_reason = g.pick(["both_sides_gated", "one_side_gated"])
    elif stage == "breaker_skipped":
        exit_reason = "breaker"
    elif stage == "balance_skipped":
        exit_reason = "balance_guard"
    if not post_era:
        # old schemas do not log submit outcomes on the decision
        quote_id_on_row, http_on_row, err_on_row = None, None, None
        if stage == "priced_presubmit":
            exit_reason = None
    else:
        quote_id_on_row, http_on_row, err_on_row = quote_id, http_status, error_kind
    dec.update(exit_reason=exit_reason, quote_id_on_row=quote_id_on_row, http_status=http_on_row,
               error_kind=err_on_row, submit_wire_at=submit_at if stage == "submitted" and dec.get("send_kind") != "dropped" else None,
               exit_at=exit_at, latency_ms=latency_ms)
    return dec


def attempts_for(g: Gen, dec, kind, quote_id, http_status, error_kind, exit_reason, submit_at):
    """Submission-log records (pre / post / skipped) for one quoting decision."""
    out = []
    schema = "quote_submission_v2" if dec["post_era"] else "quote_submission_v1"
    base = {"schema": schema, "rfq_id": dec["rfq"].rfq_id,
            "yes_bid_quantized": f"{dec['yes_bid']:.4f}", "no_bid_quantized": f"{dec['no_bid']:.4f}",
            "max_payout_notional": float(dec["max_payout"])}
    if kind == "dropped":
        aid = g.uid()
        out.append({**base, "attempt_id": aid, "phase": "skipped", "ts": submit_at, "skip_reason": exit_reason,
                    "age_ms": round(g.u(300, 1500), 1), "max_age_ms": 1000.0, "submitted_ts": submit_at})
        return out
    tries = 2 if (kind == "rate_limited" and not dec["post_era"] and g.chance(0.5)) else 1
    t = submit_at
    for i in range(tries):
        aid = g.uid()
        final = i == tries - 1
        resp = t + dt.timedelta(milliseconds=g.u(25, 180))
        body = {"yes_bid": f"{dec['yes_bid']:.4f}", "no_bid": f"{dec['no_bid']:.4f}", "rest_remainder": False}
        out.append({**base, "attempt_id": aid, "phase": "pre", "ts": t, "submitted_ts": t, "success": False,
                    "request_body": body, "in_flight": g.r.randint(0, 6)})
        if kind == "network" and final:
            out.append({**base, "attempt_id": aid, "phase": "post", "ts": resp, "submitted_ts": t, "response_ts": resp,
                        "success": False, "error_kind": "network", "error_detail": "connection reset"})
        elif kind == "live" and final:
            sampled = g.chance(0.1)
            post = {**base, "attempt_id": aid, "phase": "post", "ts": resp, "submitted_ts": t, "response_ts": resp,
                    "success": True, "quote_id": quote_id, "http_status": 200}
            if sampled:
                rt = (resp - t).total_seconds() * 1000
                post.update(latency_sampled=True, token_wait_ms=round(g.u(0, 5), 2), network_ms=round(rt * 0.8, 2),
                            roundtrip_ms=round(rt, 2))
            out.append(post)
        else:
            k2 = kind if final else "rate_limited"
            st = http_status if final else 429
            out.append({**base, "attempt_id": aid, "phase": "post", "ts": resp, "submitted_ts": t, "response_ts": resp,
                        "success": False, "error_kind": k2, "http_status": st, "error_detail": f"http {st}"})
        t = resp + dt.timedelta(milliseconds=g.u(50, 200))
    return out


def make_fill(g: Gen, dec, k):
    rfq = dec["rfq"]
    side = "no" if g.chance(0.7) else "yes"
    accepted = dec["submitted_at"] + dt.timedelta(seconds=g.u(0.4, 25) + 30 * k)
    cap = rfq.contracts if rfq.contracts is not None else dec["max_payout"]
    count = money(D(str(round(g.u(0.15, 1.0) * float(cap), 2))).max(D("1.00")))
    return {"dec": dec, "rfq": rfq, "combo": rfq.combo, "side": side, "count": count, "accepted_at": accepted,
            "quote_id": dec["live_quote_id"], "rfq_id": rfq.rfq_id, "creator": rfq.creator, "origin": "bot_maker"}


def plant_scalars(g: Gen, w, fills):
    """Turn one leg market of a few filled combos into a scalar settlement, the
    other legs into winners, and pin the fill side, so every scalar case exists."""
    specs = [  # (fill side, scalar YES value, python rule, extra)
        ("no", D("0.30"), "yes_value", "two_sided"),
        ("no", D("0.65"), "yes_value", None),
        ("no", D("0.40"), "yes_value", None),
        ("yes", D("0.70"), "half", None),
        ("yes", D("0.25"), "half", None),
        ("yes", D("0.35"), "exact", None),
        ("yes", D("0.45"), "exact", None),
        ("yes", D("0.60"), "exact", None),
        ("yes", D("0.20"), "exact", "pnl_rounding"),
    ]
    used_markets = defaultdict(int)
    for f in fills:
        for m, _ in f["combo"].legs:
            used_markets[m.ticker] += 1
    seen = set()
    candidates = [f for f in fills if f["quote_id"] and f["accepted_at"] >= FILLS_SETTLED_FROM + dt.timedelta(days=1)
                  and f["accepted_at"] < BRIDGE_END_BEFORE - dt.timedelta(days=2) and len(f["combo"].legs) in (2, 3)]
    g.r.shuffle(candidates)
    planted = []
    for side, value, rule, extra in specs:
        for f in candidates:
            c = f["combo"]
            if c.ticker in seen:
                continue
            # a leg whose market is used by this combo only
            leg_i = next((i for i, (m, _) in enumerate(c.legs) if used_markets[m.ticker] == 1 and m.result != "scalar"), None)
            if leg_i is None:
                continue
            seen.add(c.ticker)
            m = c.legs[leg_i][0]
            m.result, m.value = "scalar", value
            for i, leg in enumerate(c.legs):
                if i != leg_i and leg[0].result in ("yes", "no"):
                    leg[1] = leg[0].result          # a winning leg
            f["side"] = side
            f["scalar_spec"] = rule
            if extra == "pnl_rounding":
                f["count"] = money("37.15")
                f["force_estimate_price"] = D("0.1234")
            if extra == "two_sided":
                twin = dict(f)
                twin["side"] = "yes"
                twin["scalar_spec"] = "exact"
                twin["accepted_at"] = f["accepted_at"] + dt.timedelta(minutes=3)
                twin.pop("force_estimate_price", None)
                fills.append(twin)
            planted.append((c.ticker, side, str(value), rule))
            break
    w.planted_scalars = planted


def finish_fill(g: Gen, f):
    dec = f["dec"]
    f["fill_id"], f["order_id"] = g.uid(), g.uid()
    price = dec["yes_bid"] if f["side"] == "yes" else dec["no_bid"]
    f["submit_price"] = price
    f["yes_bid"], f["no_bid"] = dec["yes_bid"], dec["no_bid"]
    f["rfq_created_at"] = f["rfq"].created
    f["submitted_at"] = dec["submitted_at"]
    f["n_legs"] = len(f["combo"].legs)
    f["max_payout"] = f["count"]
    # channel / provenance
    f["poll_discovered"] = f.get("poll_discovered", g.chance(0.02) and not f.get("scalar_spec"))
    if f["poll_discovered"]:
        f["quote_id"], f["rfq_id"], f["creator"] = None, None, None
        f["source"], f["reverify_path"] = "rest_poll", "settle_enriched"
    else:
        f["source"] = "ws"
        f["reverify_path"] = "reverify_confirmed"
        if g.chance(0.01) and not f.get("scalar_spec"):
            f["rfq_id"] = None                                   # quote id but no rfq id (unexplained)
    f["no_accepted_ts"] = g.chance(0.01)
    f["received_at"] = f["accepted_at"] + dt.timedelta(seconds=g.u(0.05, 1.5))
    # cost source and fee
    count = f["count"]
    if f.get("force_estimate_price") is not None:
        mode = "estimate"
    else:
        x = g.r.random()
        mode = "estimate" if x < 0.09 else ("folded" if x < 0.24 else "plain")
    f["fee"] = money((D("0.004") * count + D("0.01")).quantize(CENT))
    if mode == "estimate":
        est_price = f.get("force_estimate_price") or price
        f["cost_source"] = "estimate"
        f["fill_cost"] = money(count * est_price)
        f["submit_cost_estimate"] = f["fill_cost"]
        f["fill_price"] = None if g.chance(0.5) else est_price
        f["exchange_cost"] = money((count * (est_price - D(str(round(g.u(0.002, 0.02), 4))))).quantize(D("0.0001")))
        f["fee"] = None
        f["fee_in_cost"] = False
        if f["source"] == "ws" and g.chance(0.5):
            f["reverify_path"] = "settle_enriched"
    else:
        f["cost_source"] = "refetch"
        f["fill_price"] = price
        f["submit_cost_estimate"] = money(count * price)
        base = money(count * price)
        if mode == "folded" or f["poll_discovered"]:
            f["reverify_path"] = "settle_enriched"
        f["fee_in_cost"] = f["reverify_path"] == "settle_enriched"
        f["fill_cost"] = money(base + f["fee"]) if f["fee_in_cost"] else base
        f["exchange_cost"] = base
    f["schema"] = "fill_v3" if f["accepted_at"] >= FILLS_SETTLED_FROM else "fill_v2"
    f["dup_channel"] = f["accepted_at"] >= FILLS_SETTLED_FROM and not f["poll_discovered"] and g.chance(0.015)
    f["is_manually_fixed"] = False


def make_taker(g: Gen, w, games_by_start, fills, other_makers, account_creator):
    """The account's separate taker script: two strategies, each with a plays ledger (CSV,
    different headers) and an event log (JSONL). Its executed takes are fills on this
    account with no quote id (the account was the requester)."""
    out = {"fills": [], "plays": {"taker_a": [], "taker_b": []}, "log": {"taker_a": [], "taker_b": []}}
    days = [START.date() + dt.timedelta(days=i) for i in range(4, 26)]
    plan = [("taker_a", 12, 2, 10), ("taker_b", 8, 0, 14)]   # executed takes, accepted-not-executed, passes
    for strat, n_exec, n_unexec, n_pass in plan:
        kinds = ["exec"] * n_exec + ["unexec"] * n_unexec + ["pass"] * n_pass
        g.r.shuffle(kinds)
        for kind in kinds:
            day = g.pick(days)
            t = dt.datetime.combine(day, dt.time(16, 0)) + dt.timedelta(minutes=g.u(0, 240))
            upcoming = [gm for gm in games_by_start if t + dt.timedelta(hours=1) < gm.start < t + dt.timedelta(hours=12)]
            gm = g.pick(upcoming)
            legs = [[m, "yes"] for m in g.r.sample(gm.markets, 2)]
            combo = new_combo(g, legs)
            rfq_id = g.uid()
            quotes = []
            for mk in g.r.sample(other_makers, g.r.randint(2, 3)):
                yb = D(str(round(g.u(0.1, 0.4), 2)))
                nb = D(str(round(1 - float(yb) - g.u(0.03, 0.08), 2)))
                quotes.append({"id": g.uid(), "creator_id": mk, "yes_bid": yb, "no_bid": nb})
            best = max(quotes, key=lambda q: q["no_bid"])
            contracts = money(round(g.u(20, 150), 2))
            cost = money(contracts * (1 - best["no_bid"]))
            log = out["log"][strat]
            log.append({"ts": epoch(t), "kind": "rfq", "payload": {"combo": combo.ticker, "rfq": {"id": rfq_id}}})
            for i, q in enumerate(quotes):
                log.append({"ts": epoch(t + dt.timedelta(seconds=2 + i)), "kind": "quote", "payload": {"quote": {
                    "id": q["id"], "rfq_id": rfq_id, "market_ticker": combo.ticker, "creator_id": q["creator_id"],
                    "rfq_creator_id": account_creator, "yes_bid_dollars": f"{q['yes_bid']:.4f}",
                    "no_bid_dollars": f"{q['no_bid']:.4f}", "yes_contracts_fp": f"{contracts:.2f}",
                    "no_contracts_fp": f"{contracts:.2f}", "status": "open", "created_ts": iso(t + dt.timedelta(seconds=2 + i))}}})
            action = "PASS" if kind == "pass" else "TAKE"
            play = {"ts_utc": iso(t + dt.timedelta(seconds=10)), "date": local(gm.start).date().isoformat(),
                    "game": f"{gm.away}@{gm.home}", "combo_ticker": combo.ticker, "rfq_id": rfq_id,
                    "quote_id": best["id"] if action == "TAKE" else "", "action": action,
                    "contracts": f"{contracts:.2f}" if action == "TAKE" else "", "cost": f"{cost:.2f}" if action == "TAKE" else "",
                    "n_quotes": str(len(quotes)), "slot": str(g.r.randint(1, 9)), "best_price": f"{1 - best['no_bid']:.4f}"}
            out["plays"][strat].append(play)
            if action == "TAKE":
                acc_t = t + dt.timedelta(seconds=11)
                log.append({"ts": epoch(acc_t), "kind": "accepted", "payload": {"cost": float(cost), "quote": {
                    "id": best["id"], "rfq_id": rfq_id, "market_ticker": combo.ticker, "creator_id": best["creator_id"],
                    "rfq_creator_id": account_creator, "yes_bid_dollars": f"{best['yes_bid']:.4f}",
                    "no_bid_dollars": f"{best['no_bid']:.4f}", "yes_contracts_fp": f"{contracts:.2f}",
                    "no_contracts_fp": f"{contracts:.2f}", "status": "accepted", "created_ts": iso(t)}}})
            if kind == "exec":
                price = 1 - best["no_bid"]
                fee = money((D("0.004") * contracts + D("0.01")).quantize(CENT))
                f = {"combo": combo, "side": "yes", "count": contracts, "accepted_at": acc_t + dt.timedelta(seconds=1),
                     "quote_id": None, "rfq_id": None, "creator": None, "origin": "taker_script",
                     "fill_id": g.uid(), "order_id": g.uid(), "submit_price": price, "yes_bid": None, "no_bid": None,
                     "rfq_created_at": t, "submitted_at": None, "n_legs": 2, "max_payout": contracts,
                     "poll_discovered": True, "source": "rest_poll", "reverify_path": "settle_enriched",
                     "no_accepted_ts": False, "received_at": acc_t + dt.timedelta(seconds=40),
                     "fee": fee, "cost_source": "refetch", "fill_price": price, "submit_cost_estimate": cost,
                     "fee_in_cost": True, "fill_cost": money(cost + fee), "exchange_cost": cost,
                     "schema": "fill_v3", "dup_channel": False, "is_manually_fixed": False, "dec": None, "rfq": None}
                out["fills"].append(f)
    # the self-trade-looking fill: a bot maker fill the taker_b ledger also lists as a take
    cand = next(f for f in fills if f["quote_id"] and f["rfq_id"] and not f.get("scalar_spec") and f["origin"] == "bot_maker"
                and FILLS_SETTLED_FROM + dt.timedelta(days=5) < f["accepted_at"] < BRIDGE_END_BEFORE - dt.timedelta(days=3))
    cand["self_trade_candidate"] = True
    t = cand["accepted_at"]
    out["plays"]["taker_b"].append({
        "ts_utc": iso(t), "date": local(t).date().isoformat(), "game": "rebuilt-from-fill",
        "combo_ticker": cand["combo"].ticker, "rfq_id": cand["rfq_id"], "quote_id": cand["quote_id"], "action": "TAKE",
        "contracts": f"{cand['count']:.2f}", "cost": f"{cand['fill_cost']:.2f}", "n_quotes": "", "slot": "", "best_price": ""})
    out["plays"]["taker_a"].sort(key=lambda p: p["ts_utc"])
    out["plays"]["taker_b"].sort(key=lambda p: p["ts_utc"])
    for s in out["log"]:
        out["log"][s].sort(key=lambda x: x["ts"])
    out["n_exec"] = sum(p[1] for p in plan)
    return out


def python_grade(f):
    """The legacy Python settlement script's answer, with its known quirks on scalars."""
    outcome, value = combo_outcome(f["combo"])
    f["parlay_result"], f["parlay_yes_value"] = outcome, value
    side = f["side"]
    payout = (D(1) if outcome == side else D(0)) if outcome in ("yes", "no") else (value if side == "yes" else 1 - value)
    f["payout"] = payout
    f["revenue_exact"] = money(f["max_payout"] * payout)
    f["revenue_paid"] = floor_cent(f["revenue_exact"])
    if outcome != "scalar":
        rev = f["revenue_exact"]
        pnl = money(rev - f["fill_cost"])
    else:
        rule = f.get("scalar_spec") or ("yes_value" if side == "no" else "exact")
        if side == "no":
            rule = "yes_value"
        rev = {"yes_value": money(f["max_payout"] * value), "half": money(f["max_payout"] * D("0.5")),
               "exact": f["revenue_exact"]}[rule]
        pnl = money((rev - f["fill_cost"]).quantize(D("0.0001")))
    f["py_revenue"], f["py_pnl"] = rev, pnl


# =========================================================================== settlement feed + loss budget
def settle(g: Gen, w):
    """Positions settle in exchange batches; the bot's reconciler polls every
    POLL_MINUTES and reads ONE page of PAGE_SIZE settlements (advancing its cursor
    past the rest). Each booked position is written to the loss-budget ledger,
    then to the settlement log."""
    pos = {}
    for f in w.fills:
        key = (f["combo"].ticker, f["side"])
        p = pos.setdefault(key, {"combo": f["combo"], "side": f["side"], "fills": [], "kind": "fills",
                                 "extra_count": D(0), "extra_cost": D(0)})
        p["fills"].append(f)
    # combo settlement time: next exchange batch after the last leg settles
    for p in pos.values():
        c = p["combo"]
        if c.settled_at is None:
            last_leg = max(m.settled_at for m, _ in c.legs)
            c.settled_at = next_batch(last_leg + dt.timedelta(minutes=5)) + dt.timedelta(seconds=g.u(1, 50))

    def first_leg(p):
        return min(m.settled_at for m, _ in p["combo"].legs)

    bot_pos = [p for p in pos.values() if any(f["origin"] == "bot_maker" for f in p["fills"])]

    def force_into_batch(batch, n, pred=lambda p: True):
        cands = [p for p in bot_pos if max(m.settled_at for m, _ in p["combo"].legs) < batch - dt.timedelta(minutes=5)
                 and p["combo"].settled_at > batch and pred(p)
                 and not any(q["combo"] is p["combo"] for q in bot_pos if q is not p)
                 and combo_outcome(p["combo"])[0] != "scalar"]
        cands.sort(key=lambda p: p["combo"].ticker)
        for i, p in enumerate(cands[:n]):
            p["combo"].settled_at = batch + dt.timedelta(seconds=1 + i)
        return cands[:n]

    force_into_batch(dt.datetime(2025, 3, 7, 18, 0), 3)            # dual instance re-books this batch
    force_into_batch(dt.datetime(2025, 3, 12, 12, 0), 3)           # ledger write lost at the cutover
    # ---- the account's non-bot activity (manual trades, hedges, extra contracts)
    in_window = [p for p in bot_pos if dt.datetime(2025, 3, 14) < p["combo"].settled_at < dt.datetime(2025, 3, 27)
                 and combo_outcome(p["combo"])[0] != "scalar" and len(p["fills"]) == 1]
    in_window.sort(key=lambda p: p["combo"].ticker)
    g.r.shuffle(in_window)
    two_sided = {p["combo"].ticker for p in bot_pos if (p["combo"].ticker, "yes") in pos and (p["combo"].ticker, "no") in pos}
    hedge_src = [p for p in in_window if p["combo"].ticker not in two_sided][:3]
    for p in hedge_src:
        opp = "yes" if p["side"] == "no" else "no"
        cnt = money(round(g.u(10, 80), 2))
        pos[(p["combo"].ticker, opp)] = {"combo": p["combo"], "side": opp, "fills": [], "kind": "hedge",
                                         "extra_count": cnt, "extra_cost": money(cnt * D(str(round(g.u(0.2, 0.6), 2))))}
    for p in in_window[3:5]:
        cnt = money(round(g.u(5, 40), 2))
        p["extra_count"], p["extra_cost"] = cnt, money(cnt * D(str(round(g.u(0.2, 0.7), 2))))
        p["kind"] = "fills_plus_manual"
    # manual single-market trades: leg markets, not combos
    singles = [m for m in w.markets.values() if dt.datetime(2025, 3, 14) < m.settled_at < dt.datetime(2025, 3, 27)
               and m.result in ("yes", "no")]
    singles.sort(key=lambda m: m.ticker)
    for m in g.r.sample(singles, 3):
        c = new_combo(g, [[m, "yes"], [m, "yes"]])
        c.ticker, c.legs, c.collection = m.ticker, [[m, "yes"]], None
        c.settled_at = next_batch(m.settled_at + dt.timedelta(minutes=5)) + dt.timedelta(seconds=g.u(1, 50))
        side = g.pick(["yes", "no"])
        cnt = money(round(g.u(10, 100), 2))
        pos[(m.ticker, side)] = {"combo": c, "side": side, "fills": [], "kind": "manual_single",
                                 "extra_count": cnt, "extra_cost": money(cnt * D(str(round(g.u(0.3, 0.7), 2))))}
    # manual combos: no fills on either side; one before the restart (outside the window)
    for when in [dt.datetime(2025, 3, 10, 21), dt.datetime(2025, 3, 16, 3), dt.datetime(2025, 3, 22, 3), dt.datetime(2025, 3, 25, 23)]:
        ms = [m for m in w.markets.values() if when - dt.timedelta(hours=6) < m.settled_at < when and m.result in ("yes", "no")]
        ms.sort(key=lambda m: m.ticker)
        legs = [[m, m.result if g.chance(0.4) else ("no" if m.result == "yes" else "yes")] for m in g.r.sample(ms, 2)]
        c = new_combo(g, legs)
        c.settled_at = next_batch(when) + dt.timedelta(seconds=g.u(1, 50))
        cnt = money(round(g.u(10, 60), 2))
        pos[(c.ticker, "yes")] = {"combo": c, "side": "yes", "fills": [], "kind": "manual_combo",
                                  "extra_count": cnt, "extra_cost": money(cnt * D(str(round(g.u(0.05, 0.3), 2))))}
    w.positions = pos

    # the bridge remainder: positions booked while the settlement log was missing cannot be
    # checked for count or cost. Exactly one of them carries an estimate-cost overstatement:
    # the others in those batches are made plain refetch fills (fee outside the cost).
    gap = [p for p in pos.values() if p["fills"] and SETTLEMENT_LOG_GAP[0] < p["combo"].settled_at < SETTLEMENT_LOG_GAP[1]
           and all(f["origin"] == "bot_maker" and not f.get("scalar_spec") and not f.get("self_trade_candidate")
                   for f in p["fills"])]
    gap.sort(key=lambda p: (p["combo"].settled_at, p["combo"].ticker, p["side"]))
    for p in gap:
        for f in p["fills"]:
            if f["cost_source"] == "estimate":
                f["cost_source"], f["fill_price"] = "refetch", f["submit_price"]
                f["fill_cost"] = f["exchange_cost"] = money(f["count"] * f["submit_price"])
                f["fee"], f["fee_in_cost"] = money((D("0.004") * f["count"] + D("0.01")).quantize(CENT)), False
                f["reverify_path"] = "reverify_confirmed"
                python_grade(f)
    est = [p for p in gap if len(p["fills"]) == 1 and not p["fills"][0]["fee_in_cost"]
           and sum(1 for q in pos.values() if q["combo"] is p["combo"]) == 1][:1]
    for p in est:
        f = p["fills"][0]
        f["cost_source"], f["fee"], f["fee_in_cost"], f["fill_price"] = "estimate", None, False, None
        f["fill_cost"] = money(f["exchange_cost"] + D("0.37"))
        f["submit_cost_estimate"] = f["fill_cost"]
        python_grade(f)
        c = p["combo"]                       # first in its batch, so the page always includes it
        c.settled_at = c.settled_at.replace(minute=0, second=0, microsecond=0) + dt.timedelta(milliseconds=500)
    w.remainder_positions = [p["combo"].ticker + ":" + p["side"] for p in est]

    # ---- polls
    queue = sorted(pos.values(), key=lambda p: (p["combo"].settled_at, p["combo"].ticker, p["side"]))
    ledger, slog = [], []
    cursor = START
    t = START + dt.timedelta(minutes=POLL_MINUTES)
    cum = D(0)
    daily = D(0)
    day = None
    booked, dropped = {}, []
    qi = 0
    batches = []
    while t <= LEDGER_LAST:
        batch = []
        while qi < len(queue) and queue[qi]["combo"].settled_at <= t:
            batch.append(queue[qi])
            qi += 1
        page, rest = batch[:PAGE_SIZE], batch[PAGE_SIZE:]
        dropped.extend(rest)
        if t >= LEDGER_RESTART and cursor < LEDGER_RESTART:
            cum, daily = D(0), D(0)
        entries = []
        for i, p in enumerate(page):
            ts = t + dt.timedelta(seconds=0.05 * (i + 1))
            row = settlement_row(p)
            p["settlement"] = row
            booked[(p["combo"].ticker, p["side"])] = ts
            if local(ts).date() != day:
                day, daily = local(ts).date(), D(0)
            if t != LEDGER_WRITE_LOST_POLL:
                cum += row["pnl"]
                daily += row["pnl"]
                entries.append({"ts": ts, "key": f"settle:{p['combo'].ticker}:{p['side']}", "ticker": p["combo"].ticker,
                                "pnl": row["pnl"], "daily": daily, "cum": cum})
            if not (SETTLEMENT_LOG_GAP[0] <= t < SETTLEMENT_LOG_GAP[1]):
                slog.append({**row, "ts": ts + dt.timedelta(milliseconds=10), "p": p})
            p["booked_at"] = ts
        ledger.extend(entries)
        if entries:
            batches.append((t, entries))
        cursor = t
        t += dt.timedelta(minutes=POLL_MINUTES)
    # dual instance: B re-books A's latest batch from the same base
    prior = [b for b in batches if b[0] < DUAL_START]
    bt, bentries = prior[-1]
    base = bentries[0]["cum"] - bentries[0]["pnl"]
    base_daily = bentries[0]["daily"] - bentries[0]["pnl"]
    b_rows = []
    c2, d2 = base, base_daily
    for i, e in enumerate(bentries):
        c2 += e["pnl"]
        d2 += e["pnl"]
        b_rows.append({**e, "ts": DUAL_START + dt.timedelta(minutes=2, seconds=0.07 * (i + 1)), "cum": c2, "daily": d2})
    ledger.extend(b_rows)
    ledger.sort(key=lambda e: e["ts"])
    w.ledger, w.slog = ledger, slog
    w.dual_rebooked = len(b_rows)
    w.dropped = dropped
    end_entries = [e for e in ledger if e["ts"] < BRIDGE_END_BEFORE]
    w.bridge_target = end_entries[-1]["cum"]
    w.never_booked_bot = [p for p in dropped if p["fills"] and any(f["origin"] == "bot_maker" for f in p["fills"])]


def next_batch(t):
    base = t.replace(minute=0, second=0, microsecond=0)
    h = (base.hour // BATCH_HOURS + 1) * BATCH_HOURS
    return base.replace(hour=0) + dt.timedelta(hours=h)


def settlement_row(p):
    """What the bot's reconciler logs for one (ticker, side): the exchange's count and cost
    (cost excludes fees); revenue computed by the bot: count x $1 if the result is the
    held side, the cost back if void, else 0 (so a scalar result logs 0)."""
    outcome, _ = combo_outcome(p["combo"]) if p["kind"] != "manual_single" else (p["combo"].legs[0][0].result, None)
    count = sum((f["count"] for f in p["fills"]), D(0)) + p["extra_count"]
    cost = sum((f["exchange_cost"] for f in p["fills"]), D(0)) + p["extra_cost"]
    count, cost = money(count), money(cost)
    revenue = count if outcome == p["side"] else D(0)
    revenue = money(revenue)
    return {"count": count, "cost": cost, "revenue": revenue, "pnl": money(revenue - cost), "result": outcome,
            "ticker": p["combo"].ticker, "side": p["side"], "settled_at": p["combo"].settled_at,
            "event": p["combo"].collection or p["combo"].legs[0][0].event_ticker}


# =========================================================================== writing
def write_all(w, out):
    if os.path.exists(out):
        shutil.rmtree(out)
    os.makedirs(out)
    tmp = tempfile.mkdtemp(prefix="pm_synth_")
    con = duckdb.connect()
    manifest = []
    files_by_table = defaultdict(lambda: defaultdict(list))   # table -> source_key -> rows

    def src_key(stem, t):
        origin = ARCHIVE if t < SUBMISSION_LOG_COMPLETE_FROM else HOST
        return f"{origin}/{stem}.jsonl", origin

    # ---------------- decisions (rfq_outcomes + slim) and events
    def hour_stem(prefix, t):
        return f"{prefix}-{t:%Y-%m-%d-%H}"

    tail_cut = {}
    rng = random.Random(7)
    h = ERA_CUTOVER.replace(minute=0)
    while h < TAIL_LOSS_UNTIL:
        # the bug fires when the hour's log file grows past the rotation ceiling: busy hours only
        busy = local(h).hour >= 12 or local(h).hour < 1
        if busy and rng.random() < 0.3:
            tail_cut[h] = h + dt.timedelta(minutes=rng.uniform(36, 54))
        h += dt.timedelta(hours=1)

    def lost(t):
        hh = t.replace(minute=0, second=0, microsecond=0)
        return hh in tail_cut and t >= tail_cut[hh]

    w.tail_cut_hours = len(tail_cut)
    lost_decisions = 0
    parse_err_todo = {"rfq_outcome_v1", "exposure_rejected_v2"}   # one truncated line each
    for dec in sorted(w.decisions, key=lambda d: (d["exit_at"], d["instance"])):
        rec, typed = decision_record(dec)
        if dec["post_era"] and lost(dec["exit_at"]):
            lost_decisions += 1
            dec["lost"] = True
            continue
        if dec["stage"] == "priced_rejected" and rec["schema"] in parse_err_todo and dec["instance"] == "A":
            parse_err_todo.discard(rec["schema"])
            dec["parse_error"] = True
        sk, origin = src_key(hour_stem("pricing-decisions", dec["exit_at"]), dec["exit_at"])
        files_by_table["rfq_outcomes"][sk].append((dec["exit_at"], typed, rec, origin, dec.get("parse_error", False)))
    w.lost_decisions = lost_decisions
    w.n_doubled_rfqs = len({d["rfq"].rfq_id for d in w.decisions if d["instance"] == "B" and not d.get("parse_error")})

    # slim: every RFQ in the window the bot recorded, filtered included
    slim_rows = defaultdict(list)
    dec_by_rfq = {d["rfq"].rfq_id: d for d in w.decisions if d["instance"] == "A"}
    for rfq in w.rfqs:
        if not (WINDOW_START - dt.timedelta(hours=1) <= rfq.created < WINDOW_END + dt.timedelta(hours=1)):
            continue
        if rfq.bot_state != "on":
            continue
        if rfq.filtered:
            t = rfq.created + dt.timedelta(milliseconds=rng.uniform(20, 300))
            if lost(t):
                continue
            row = {"rfq_id": rfq.rfq_id, "received_ts": epoch(t), "schema": "rfq_outcome_v1", "terminal_stage": "filtered",
                   "exit_reason": rng.choice(["leg_spread_too_wide", "stale_book", "unsupported_market", "requester_cooldown",
                                              "min_quote_size"]),
                   "creator_id": rfq.creator, "combo_ticker": rfq.combo.ticker, "n_legs": len(rfq.combo.legs),
                   "n_requested_legs": len(rfq.combo.legs), "collection_ticker": rfq.combo.collection}
            sk, origin = src_key(hour_stem("pricing-decisions", t), t)
            slim_rows[sk].append((t, row, origin))
        else:
            d = dec_by_rfq.get(rfq.rfq_id)
            if d is None or d.get("lost") or d.get("parse_error"):
                continue
            row = {"rfq_id": rfq.rfq_id, "received_ts": epoch(d["received_at"]), "schema": "rfq_outcome_v1",
                   "terminal_stage": d["stage"], "exit_reason": d["exit_reason"], "creator_id": rfq.creator,
                   "combo_ticker": rfq.combo.ticker, "n_legs": len(rfq.combo.legs),
                   "n_requested_legs": len(rfq.combo.legs), "collection_ticker": rfq.combo.collection}
            sk, origin = src_key(hour_stem("pricing-decisions", d["exit_at"]), d["exit_at"])
            slim_rows[sk].append((d["exit_at"], row, origin))

    # events: rfq_created for every RFQ while the bot (which writes this log too) is up; ~1% frames dropped
    ev_rows = defaultdict(list)
    ev_other = defaultdict(list)
    seq = 0
    for rfq in w.rfqs:
        if not (WINDOW_START - dt.timedelta(hours=1) <= rfq.created < WINDOW_END + dt.timedelta(hours=1)):
            continue
        if rfq.bot_state == "off" or rng.random() < 0.01:
            continue
        seq += 1
        t = rfq.created + dt.timedelta(milliseconds=rng.uniform(5, 60))
        row = {"rfq_id": rfq.rfq_id, "created_ts": iso(rfq.created), "received_ts": epoch(t), "creator_id": rfq.creator,
               "combo_ticker": rfq.combo.ticker, "n_legs": len(rfq.combo.legs), "seq": seq, "sid": 1,
               "collection_ticker": rfq.combo.collection}
        sk, origin = src_key(hour_stem("events", t), t)
        ev_rows[sk].append((t, row, origin))
    fills_by_quote = defaultdict(list)
    for f in w.fills:
        if f.get("quote_id"):
            fills_by_quote[f["quote_id"]].append(f)
    seq2 = 0
    for d in w.decisions:
        if not d["live_quote_id"] or not (WINDOW_START <= d["submitted_at"] < WINDOW_END):
            continue
        rfq = d["rfq"]
        frames = [("quote_created", d["submitted_at"] + dt.timedelta(milliseconds=rng.uniform(30, 200)), None)]
        for f in fills_by_quote.get(d["live_quote_id"], []):
            frames.append(("quote_accepted", f["accepted_at"], f))
            frames.append(("quote_executed", f["accepted_at"] + dt.timedelta(milliseconds=rng.uniform(5, 50)), f))
        for ftype, t, f in frames:
            seq2 += 1
            msg = {"quote_id": d["live_quote_id"], "rfq_id": rfq.rfq_id, "quote_creator_id": w.maker_id,
                   "rfq_creator_id": rfq.creator, "market_ticker": rfq.combo.ticker, "event_ticker": rfq.combo.collection,
                   "yes_bid_dollars": f"{d['yes_bid']:.4f}", "no_bid_dollars": f"{d['no_bid']:.4f}",
                   "yes_contracts_offered_fp": f"{d['max_payout']:.2f}", "no_contracts_offered_fp": f"{d['max_payout']:.2f}",
                   "created_ts": iso(d["submitted_at"])}
            if f is not None:
                # the requester takes the opposite side of what this account bought
                msg.update(accepted_side="no" if f["side"] == "yes" else "yes", contracts_accepted_fp=f"{f['count']:.2f}",
                           order_id=f["order_id"], client_order_id=None)
                if ftype == "quote_executed":
                    msg["executed_ts"] = iso(f["accepted_at"])
            raw = json.dumps({"type": ftype, "sid": 2, "seq": seq2, "msg": msg})
            row = {"frame_type": ftype, "received_ts": epoch(t), "seq": seq2, "sid": 2,
                   "msg_id": None, "msg_rfq_id": rfq.rfq_id, "msg_quote_id": d["live_quote_id"],
                   "raw_json": json.dumps({"received_ts": epoch(t), "raw": raw})}
            sk, origin = src_key(hour_stem("events", t), t)
            ev_other[sk].append((t, row, origin))

    # ---------------- quote submissions (archive copy before SUBMISSION_LOG_COMPLETE_FROM is missing rows)
    qrows = defaultdict(list)
    parse_err_q = False
    att_lost = set()
    for a in w.attempts:
        if a["ts"] < SUBMISSION_LOG_COMPLETE_FROM and a["attempt_id"] not in att_lost and rng.random() < 0.35:
            att_lost.add(a["attempt_id"])
    for a in sorted(w.attempts, key=lambda a: a["ts"]):
        if a["attempt_id"] in att_lost:
            continue
        rec = {k: v for k, v in a.items() if k not in ("ts", "submitted_ts", "response_ts")}
        rec["ts"] = epoch(a["ts"])
        for k in ("submitted_ts", "response_ts"):
            if a.get(k) is not None:
                rec[k] = epoch(a[k])
        typed = {"schema": a["schema"], "phase": a["phase"], "attempt_id": a["attempt_id"], "ts": rec["ts"],
                 "rfq_id": a["rfq_id"], "quote_id": a.get("quote_id"), "success": a.get("success")}
        pe = False
        if not parse_err_q and a["phase"] == "skipped" and a["ts"] > SUBMISSION_LOG_COMPLETE_FROM:
            pe, parse_err_q = True, True
        sk, origin = src_key(hour_stem("quote-submissions", a["ts"]), a["ts"])
        qrows[sk].append((a["ts"], typed, rec, origin, pe))
    w.n_attempts_lost = len(att_lost)

    # ---------------- fills: settled file (daily, local date) + raw hourly log
    settled, raw = defaultdict(list), defaultdict(list)
    for f in w.fills:
        rec = fill_record(f, settled=False)
        recs = [(f["received_at"], rec, f["source"])]
        if f["dup_channel"]:
            r1 = dict(rec, source="rest_poll", reverify_path="poll_discovered")
            r2 = dict(rec)
            recs = [(f["received_at"], r1, "rest_poll"), (f["received_at"] + dt.timedelta(seconds=rng.uniform(5, 60)), r2, "ws")]
        for t, r, src in recs:
            typed = {"fill_id": r["fill_id"], "accepted_ts": r.get("accepted_ts"), "received_ts": r["received_ts"],
                     "quote_id": r["quote_id"], "rfq_id": r["rfq_id"], "market_ticker": r["market_ticker"],
                     "side": r["side"], "source": src, "schema": r["schema"]}
            sk, origin = src_key(hour_stem("fills", t), t)
            raw[sk].append((t, typed, r, origin, False))
        if f["accepted_at"] >= FILLS_SETTLED_FROM:
            srec = fill_record(f, settled=True)
            typed = {"fill_id": srec["fill_id"], "accepted_ts": srec.get("accepted_ts"), "received_ts": srec["received_ts"],
                     "quote_id": srec["quote_id"], "rfq_id": srec["rfq_id"], "market_ticker": srec["market_ticker"],
                     "side": srec["side"], "parlay_result": srec["parlay_result"], "schema": srec["schema"]}
            day = local(f["accepted_at"]).date()
            sk = f"{HOST}/fills-settled-{day:%Y-%m-%d}.jsonl"
            settled[sk].append((f["accepted_at"], typed, srec, HOST, False))

    # ---------------- settlement log + ledger (daily UTC files)
    srows, lrows = defaultdict(list), defaultdict(list)
    for s in w.slog:
        rec = {"schema": "settlement_v1", "ts": epoch(s["ts"]), "market_ticker": s["ticker"], "event_ticker": s["event"],
               "side": s["side"], "count": float(s["count"]), "cost": float(s["cost"]), "revenue": float(s["revenue"]),
               "pnl": float(s["pnl"]), "settled_ts": epoch(s["settled_at"]), "settlement_result": s["result"]}
        typed = {"schema": "settlement_v1", "ts": rec["ts"], "market_ticker": s["ticker"], "event_ticker": s["event"],
                 "side": s["side"], "settled_ts": str(rec["settled_ts"])}
        sk, origin = src_key(f"settlements-{s['ts']:%Y-%m-%d}", s["ts"])
        srows[sk].append((s["ts"], typed, rec, origin, False))
    for e in w.ledger:
        rec = {"schema": "loss_budget_v1", "ts": epoch(e["ts"]), "fill_id": e["key"], "market_ticker": e["ticker"],
               "pnl": float(e["pnl"]), "daily_realized_pnl": float(e["daily"]), "cumulative_realized_pnl": float(e["cum"])}
        typed = {"schema": "loss_budget_v1", "ts": rec["ts"], "fill_id": e["key"], "market_ticker": e["ticker"]}
        sk, origin = src_key(f"loss-budget-{e['ts']:%Y-%m-%d}", e["ts"])
        lrows[sk].append((e["ts"], typed, rec, origin, False))

    # ---------------- write the Parquet tables
    def write_table(table, groups, typed_cols, with_raw=True, ordered=True):
        path = os.path.join(tmp, f"{table}.jsonl")
        n_files = 0
        with open(path, "w", encoding="utf-8") as fh:
            for sk, rows in groups.items():
                rows.sort(key=lambda r: r[0])
                n_files += 1
                pe_count = 0
                for i, row in enumerate(rows, start=1):
                    if with_raw:
                        t, typed, rec, origin, pe = row
                        raw_json = json.dumps(rec, default=jdefault)
                        if pe:
                            raw_json = raw_json[: int(len(raw_json) * 0.6)]
                            typed = {k: None for k in typed}
                            pe_count += 1
                        out_row = {**typed, "raw_json": raw_json}
                    else:
                        t, typed, origin = row
                        pe = False
                        out_row = dict(typed)
                    out_row.update(_source_key=sk, _source_origin=origin, _line_no=i, _parse_error=pe,
                                   _file=f"{table}/month={t:%Y-%m}/{os.path.basename(sk)[:-6]}.parquet")
                    fh.write(json.dumps(out_row, default=jdefault) + "\n")
                manifest.append({"table": table, "source_key": sk, "source_origin": rows[0][-2] if with_raw else rows[0][2],
                                 "status": "ok", "rows_read": str(len(rows)), "rows_written": str(len(rows)),
                                 "rows_parse_error": str(pe_count)})
        cols = {"_source_key": "VARCHAR", "_source_origin": "VARCHAR", "_line_no": "BIGINT", "_parse_error": "BOOLEAN",
                **typed_cols, **({"raw_json": "VARCHAR"} if with_raw else {}), "_file": "VARCHAR"}
        colspec = "{" + ", ".join(f"'{k}': '{v}'" for k, v in cols.items()) + "}"
        con.execute(f"create or replace table t as select * from read_json('{path.replace(os.sep, '/')}', "
                    f"format='newline_delimited', columns={colspec})")
        for (f,) in con.execute("select distinct _file from t").fetchall():
            dst = os.path.join(out, f)
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            con.execute(f"copy (select * exclude (_file) from t where _file = ? order by _line_no) to "
                        f"'{dst.replace(os.sep, '/')}' (format parquet)", [f])
        return n_files

    V, DB, BI, B = "VARCHAR", "DOUBLE", "BIGINT", "BOOLEAN"
    write_table("rfq_outcomes", files_by_table["rfq_outcomes"],
                {"rfq_id": V, "schema": V, "terminal_stage": V, "exit_reason": V, "ts": DB, "received_ts": DB,
                 "exit_ts": DB, "creator_id": V, "combo_ticker": V, "quote_id": V, "n_legs": BI})
    write_table("rfq_outcomes_slim", slim_rows,
                {"rfq_id": V, "received_ts": DB, "schema": V, "terminal_stage": V, "exit_reason": V, "creator_id": V,
                 "combo_ticker": V, "n_legs": BI, "n_requested_legs": BI, "collection_ticker": V}, with_raw=False)
    write_table("events_rfq_created", ev_rows,
                {"rfq_id": V, "created_ts": V, "received_ts": DB, "creator_id": V, "combo_ticker": V, "n_legs": BI,
                 "seq": BI, "sid": BI, "collection_ticker": V}, with_raw=False)
    write_table("events_other", ev_other,
                {"frame_type": V, "received_ts": DB, "seq": BI, "sid": BI, "msg_id": V, "msg_rfq_id": V,
                 "msg_quote_id": V, "raw_json": V}, with_raw=False)
    write_table("quote_submissions", qrows,
                {"schema": V, "phase": V, "attempt_id": V, "ts": DB, "rfq_id": V, "quote_id": V, "success": B})
    write_table("fills_settled", settled,
                {"fill_id": V, "accepted_ts": DB, "received_ts": DB, "quote_id": V, "rfq_id": V, "market_ticker": V,
                 "side": V, "parlay_result": V, "schema": V})
    write_table("fills_raw", raw,
                {"fill_id": V, "accepted_ts": DB, "received_ts": DB, "quote_id": V, "rfq_id": V, "market_ticker": V,
                 "side": V, "source": V, "schema": V})
    write_table("exchange_settlements", srows,
                {"schema": V, "ts": DB, "market_ticker": V, "event_ticker": V, "side": V, "settled_ts": V})
    write_table("loss_budget", lrows,
                {"schema": V, "ts": DB, "fill_id": V, "market_ticker": V})

    # ---------------- market results (every leg market any fill or position touched)
    tickers = set()
    for f in w.fills:
        for m, _ in f["combo"].legs:
            tickers.add(m.ticker)
    for p in w.positions.values():
        if p["kind"] == "manual_single":
            tickers.add(p["combo"].ticker)
    mpath = os.path.join(tmp, "markets.jsonl")
    with open(mpath, "w", encoding="utf-8") as fh:
        for tk in sorted(tickers):
            m = w.markets[tk]
            sv = "1.0000" if m.result == "yes" else ("0.0000" if m.result == "no" else f"{m.value:.4f}")
            raw_json = {"ticker": m.ticker, "event_ticker": m.event_ticker, "market_type": "binary", "status": "finalized",
                        "result": m.result, "title": m.title, "yes_sub_title": m.yes_sub, "no_sub_title": m.no_sub,
                        "expiration_value": "", "settlement_value_dollars": sv, "settlement_ts": iso(m.settled_at),
                        "open_time": iso(m.game.start - dt.timedelta(days=3)), "close_time": iso(m.game.end),
                        "occurrence_datetime": iso(m.game.start), "last_price_dollars": sv,
                        "volume_fp": f"{len(tk) * 37.0:.2f}", "open_interest_fp": "0.00"}
            fh.write(json.dumps({"market_ticker": m.ticker, "status": "finalized",
                                 "result": m.result if m.result in ("yes", "no") else None, "settled_ts": None,
                                 "bucket": "scored" if m.result in ("yes", "no") else "void",
                                 "raw_json": json.dumps(raw_json), "fetched_ts": epoch(FETCHED_AT),
                                 "in_original_cache": tk < "M"}) + "\n")
    dst = os.path.join(out, "market_results", "market_settlements.parquet")
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    con.execute(f"copy (select * from read_json('{mpath.replace(os.sep, '/')}', format='newline_delimited', columns="
                "{'market_ticker': 'VARCHAR', 'status': 'VARCHAR', 'result': 'VARCHAR', 'settled_ts': 'DOUBLE', "
                "'bucket': 'VARCHAR', 'raw_json': 'VARCHAR', 'fetched_ts': 'DOUBLE', 'in_original_cache': 'BOOLEAN'})) "
                f"to '{dst.replace(os.sep, '/')}' (format parquet)")
    manifest.append({"table": "market_results", "source_key": "exchange-api/market_settlements", "source_origin": "api",
                     "status": "ok", "rows_read": str(len(tickers)), "rows_written": str(len(tickers)), "rows_parse_error": "0"})
    w.n_markets = len(tickers)

    # ---------------- taker ledgers (raw text)
    headers = {"taker_a": ["ts_utc", "date", "game", "combo_ticker", "rfq_id", "quote_id", "action", "contracts", "cost", "n_quotes"],
               "taker_b": ["ts_utc", "date", "game", "slot", "combo_ticker", "rfq_id", "quote_id", "action", "contracts", "cost", "best_price"]}
    w.taker_counts = {}
    for strat, hdr in headers.items():
        d = os.path.join(out, "taker", strat)
        os.makedirs(d, exist_ok=True)
        with open(os.path.join(d, "plays.csv"), "w", encoding="utf-8", newline="\n") as fh:
            fh.write(",".join(hdr) + "\n")
            for p in w.taker["plays"][strat]:
                fh.write(",".join(p[h] for h in hdr) + "\n")
        with open(os.path.join(d, "log.jsonl"), "w", encoding="utf-8", newline="\n") as fh:
            for line in w.taker["log"][strat]:
                fh.write(json.dumps(line) + "\n")
        w.taker_counts[strat] = (len(w.taker["plays"][strat]), len(w.taker["log"][strat]))

    # ---------------- superseded copies (the manifest keeps them; staging must not count them)
    for sk in list(files_by_table["rfq_outcomes"])[:2]:
        manifest.append({"table": "rfq_outcomes", "source_key": sk.replace(".jsonl", ".partial.jsonl"),
                         "source_origin": ARCHIVE, "status": "superseded", "rows_read": "10", "rows_written": "10",
                         "rows_parse_error": "0"})
    write_small(con, os.path.join(out, "_manifest", "manifest.parquet"), manifest)
    con.close()
    shutil.rmtree(tmp, ignore_errors=True)


def write_small(con, path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".jsonl"
    with open(tmp, "w", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r, default=jdefault) + "\n")
    con.execute(f"copy (select * from read_json('{tmp.replace(os.sep, '/')}', format='newline_delimited')) "
                f"to '{path.replace(os.sep, '/')}' (format parquet)")
    os.remove(tmp)


def jdefault(x):
    if isinstance(x, Decimal):
        return float(x)
    if isinstance(x, dt.datetime):
        return epoch(x)
    raise TypeError(type(x))


def decision_record(dec):
    rfq = dec["rfq"]
    post = dec["post_era"]
    legs = []
    for i, (m, side) in enumerate(rfq.combo.legs):
        bid = 20 + stable_int(rfq.rfq_id, i) % 50
        book = {"best_bid": float(bid), "best_ask": float(bid + 1 + (i % 4)), "bid_depth": float(100 + 37 * i),
                "ask_depth": float(80 + 23 * i), "book_last_update_ts": epoch(dec["received_at"] - dt.timedelta(seconds=2 + i))}
        if post:
            legs.append({"market_ticker": m.ticker, "side": side, "sport": m.sport, "market_type": m.market_type,
                         "game_slug": f"{m.sport}:{m.game.matchup}", "game_start_ts": epoch(m.game.start),
                         "live_at_receipt": m.game.start <= dec["received_at"], **book})
        else:
            legs.append({"market_ticker": m.ticker, "side": side, "event_ticker": m.event_ticker,
                         "metadata_sport": m.sport, "metadata_market_type": m.market_type,
                         "metadata_game_start_ts": epoch(m.game.start), "metadata_status": "active", **book})
    stage = dec["stage"]
    common = {"rfq_id": rfq.rfq_id, "creator_id": rfq.creator, "received_ts": epoch(dec["received_at"]), "legs": legs}
    if rfq.contracts is not None:
        common["contracts_fp"] = float(rfq.contracts)
    else:
        common["target_cost_dollars"] = float(rfq.target_cost)
    priced = {"yes_bid": float(dec["yes_bid"]), "no_bid": float(dec["no_bid"]), "max_payout": float(dec["max_payout"])}
    if post:
        rec = {"schema": "rfq_outcome_v1", **common, "combo_ticker": rfq.combo.ticker,
               "collection_ticker": rfq.combo.collection, "ts": epoch(dec["exit_at"]), "exit_ts": epoch(dec["exit_at"]),
               "terminal_stage": stage, "exit_reason": dec["exit_reason"], "n_legs": len(legs)}
        if stage in ("submitted", "priced_rejected", "priced_logged", "gated_skipped"):
            rec.update(priced)
        if stage == "submitted":
            if dec["submit_wire_at"] is not None:
                rec["submit_wire_ts"] = epoch(dec["submit_wire_at"])
                rec["request_bids"] = {"yes": float(dec["yes_bid"]), "no": float(dec["no_bid"])}
            rec.update(quote_id=dec["quote_id_on_row"] or "", http_status=dec["http_status"], error_kind=dec["error_kind"])
        if stable_int(rfq.rfq_id) % 100 == 0:
            rec["is_replacement"] = True
        typed_stage, typed_reason = stage, dec["exit_reason"]
    else:
        schema = {"priced_presubmit": "would_quote_v2", "priced_rejected": "exposure_rejected_v2",
                  "breaker_skipped": "breaker_skip_v2", "balance_skipped": "balance_guard_skip_v1"}[stage]
        rec = {"schema": schema, **common, "ts": epoch(dec["exit_at"])}
        if schema in ("would_quote_v2", "exposure_rejected_v2"):
            rec.update(priced)
        if schema == "exposure_rejected_v2":
            rec["exposure_layer"] = dec["exit_reason"]
        typed_stage, typed_reason = None, None
    typed = {"rfq_id": rfq.rfq_id, "schema": rec["schema"], "terminal_stage": typed_stage, "exit_reason": typed_reason,
             "ts": rec["ts"], "received_ts": rec["received_ts"], "exit_ts": rec.get("exit_ts"),
             "creator_id": rfq.creator, "combo_ticker": rec.get("combo_ticker"), "quote_id": rec.get("quote_id") or None,
             "n_legs": len(legs)}
    return rec, typed


def fill_record(f, settled):
    c = f["combo"]
    legs = []
    for i, (m, side) in enumerate(c.legs):
        if f["poll_discovered"]:
            legs.append({"market_ticker": m.ticker, "side": side})
            continue
        bid = 20 + (len(m.ticker) * 7 + i * 11) % 50
        legs.append({"market_ticker": m.ticker, "side": side, "sport": m.sport, "market_type": m.market_type,
                     "game_slug": None if c.no_slug_leg == i else f"{m.sport}:{m.game.matchup}",
                     "best_bid_at_submit": float(bid), "best_ask_at_submit": float(bid + 2),
                     "spread_cents_at_submit": 2.0, "depth_touched_at_submit": float(50 + i * 10),
                     "live_at_submit": m.game.start <= (f["submitted_at"] or f["accepted_at"])})
    slugs = [x.get("game_slug") for x in legs if x.get("game_slug")]
    is_sgp = None if f["poll_discovered"] else (len(set(slugs)) < len(slugs))
    rec = {"schema": f["schema"], "fill_id": f["fill_id"], "order_id": f["order_id"], "quote_id": f["quote_id"] or "",
           "rfq_id": f["rfq_id"] or "", "requester_creator_id": f["creator"], "market_ticker": c.ticker,
           "side": f["side"], "count": float(f["count"]), "max_payout": float(f["max_payout"]), "n_legs": f["n_legs"],
           "received_ts": epoch(f["received_at"]), "source": f["source"]}
    if f["schema"] == "fill_v3":
        rec.update({"is_same_game_parlay": is_sgp, "is_straight": False,
                    "yes_bid": float(f["yes_bid"]) if f["yes_bid"] is not None else None,
                    "no_bid": float(f["no_bid"]) if f["no_bid"] is not None else None,
                    "submit_price": float(f["submit_price"]),
                    "submit_cost_estimate": float(f["submit_cost_estimate"]),
                    "fill_price": float(f["fill_price"]) if f["fill_price"] is not None else None,
                    "fill_cost": float(f["fill_cost"]), "fee": float(f["fee"]) if f["fee"] is not None else None,
                    "cost_source": f["cost_source"],
                    "rfq_created_ts": epoch(f["rfq_created_at"]) if f["rfq_created_at"] else None,
                    "submitted_ts": epoch(f["submitted_at"]) if f["submitted_at"] else None,
                    "accepted_ts": None if f["no_accepted_ts"] else epoch(f["accepted_at"]),
                    "executed_ts": epoch(f["accepted_at"] + dt.timedelta(milliseconds=8)),
                    "submit_to_accept_sec": round((f["accepted_at"] - f["submitted_at"]).total_seconds(), 3) if f["submitted_at"] else None,
                    "reverify_path": f["reverify_path"], "legs": legs})
    else:
        rec.update({"accepted_ts": epoch(f["accepted_at"]), "fill_price": float(f["submit_price"])})
    if settled:
        rec.update({"parlay_result": f["parlay_result"], "revenue": float(f["py_revenue"]),
                    "realized_pnl": float(f["py_pnl"]), "settled": None,
                    "is_manually_fixed": f["is_manually_fixed"] or None,
                    "manual_fix_note": "revenue re-entered by hand after a settlement re-check" if f["is_manually_fixed"] else None,
                    "legs_settled": [{"market_ticker": m.ticker, "side": s, "result": m.result,
                                      "won": (m.result == s) if m.result in ("yes", "no") else None} for m, s in c.legs]})
    else:
        rec["parlay_result"] = None
    return rec


# =========================================================================== expectations
def expectations(w, out):
    settled = [f for f in w.fills if f["accepted_at"] >= FILLS_SETTLED_FROM]
    graded_gross = sum((f["revenue_paid"] - f["fill_cost"] for f in settled), D(0))
    fee_known = [f for f in settled if f["fee"] is not None]
    net = sum((f["revenue_paid"] - f["fill_cost"] - (D(0) if f["fee_in_cost"] else f["fee"]) for f in fee_known), D(0))
    rows = [
        ("fills_settled_rows", len(settled), "fills in the settled fill file"),
        ("python_total_cost", sum((f["fill_cost"] for f in settled), D(0)), "sum of fill_cost, settled file"),
        ("python_total_revenue", sum((f["py_revenue"] for f in settled), D(0)), "the Python script's revenue"),
        ("python_total_realized_pnl", sum((f["py_pnl"] for f in settled), D(0)), "the Python script's realized P&L"),
        ("total_fees", sum((f["fee"] for f in fee_known), D(0)), "sum of known fees"),
        ("graded_gross_pnl", graded_gross, "gross P&L, payouts floored to the cent (computed by the generator)"),
        ("graded_net_pnl_fee_known", net, "net P&L over fills with a known fee"),
        ("taker_fills_on_ledger", w.taker["n_exec"] + 1, "fills matching a taker-ledger take (incl. the self-trade-looking one)"),
        ("taker_script_fills", w.taker["n_exec"], "executed taker-script fills"),
        ("self_trade_candidates", 1, "bot maker fills a taker ledger also claims"),
        ("doubled_rfqs", w.n_doubled_rfqs, "RFQs decided twice in the dual-instance window"),
        ("dual_window_rebooked_ledger_entries", w.dual_rebooked, "ledger entries the second instance re-booked"),
        ("bridge_target", w.bridge_target, "loss-budget cumulative at the bridged entry"),
        ("taker_a_plays", w.taker_counts["taker_a"][0], "rows in taker_a plays.csv (header excluded)"),
        ("taker_b_plays", w.taker_counts["taker_b"][0], "rows in taker_b plays.csv"),
        ("taker_a_log", w.taker_counts["taker_a"][1], "lines in taker_a log.jsonl"),
        ("taker_b_log", w.taker_counts["taker_b"][1], "lines in taker_b log.jsonl"),
        # descriptive (README / review), not asserted by tests
        ("planted_channel_duplicates", sum(1 for f in w.fills if f["dup_channel"]), "fills logged twice in the raw log"),
        ("planted_scalar_combos", len(w.planted_scalars), "combos forced to a scalar settlement"),
        ("scalar_fills", sum(1 for f in settled if f["parlay_result"] == "scalar"), "fills on scalar parlays"),
        ("fee_in_cost_fills", sum(1 for f in settled if f["fee_in_cost"]), "fills whose fee is inside fill_cost"),
        ("fee_unknown_fills", sum(1 for f in settled if f["fee"] is None), "estimate-cost fills with no fee"),
        ("never_reconciled_bot_positions", len(w.never_booked_bot), "bot positions the paginated reconciler skipped"),
        ("decisions_lost_to_tail_bug", w.lost_decisions, "decision rows lost at the end of an hour"),
        ("hours_with_tail_loss", w.tail_cut_hours, "hours the logging bug cut"),
        ("same_game_flag_missed", len(w.same_game_missed), "same-game combos the bot flagged false"),
        ("submission_attempts_missing_from_archive", w.n_attempts_lost, "attempts missing before the host copy starts"),
        ("bridge_remainder_positions", len(w.remainder_positions), "positions behind the bridge remainder"),
    ]
    con = duckdb.connect()
    write_small(con, os.path.join(out, "_manifest", "expectations.parquet"),
                [{"key": k, "value": float(v), "description": d} for k, v, d in rows])
    con.close()
    return rows


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--out", default=os.environ.get("DBT_DATA_ROOT", "data/synthetic"))
    ap.add_argument("--seed", type=int, default=20250303)
    a = ap.parse_args()
    w = generate(a.seed)
    write_all(w, a.out)
    rows = expectations(w, a.out)
    print(f"wrote {a.out}: {len(w.rfqs)} RFQs, {len(w.decisions)} decisions, {len(w.fills)} fills, "
          f"{len(w.ledger)} ledger entries, {len(w.slog)} settlement rows, {w.n_markets} markets")
    for k, v, _ in rows:
        print(f"  {k:<44} {v}")


if __name__ == "__main__":
    main()
