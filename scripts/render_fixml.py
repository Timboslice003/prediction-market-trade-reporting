"""Render sample FIXML trade capture reports from mart_daily_trade_report.

dbt cannot write XML, so this script does it. It reads the trade report mart
(plus fct_fills for the order id and for choosing samples) from DuckDB,
read-only, and writes ONE FIXML file per chosen trade date: a Batch wrapper
(header with a placeholder sender id, TID="CFTC", creation time, and TotMsg)
holding one TrdCaptRpt per sampled trade.

    python scripts/render_fixml.py [--db PATH] [--out-dir DIR]
        [--dates 2025-03-11,2025-03-18] [--per-date 20] [--created-at ISO]

Output goes to work/fixml/ by default (git-ignored). Field mapping and every
convention: docs/FIXML_MAPPING.md. Validation and the round-trip check:
scripts/check_fixml.py.

The format is MODELED ON the FIX TradeCaptureReport (FIXML TrdCaptRpt). It is
not certified against any regulator's specification, and it is one market
participant's view of its own trades, not an exchange's full daily file.

Sample selection (deterministic; no randomness, no hard-coded trade ids):
  Dates (unless --dates is given), in this order, duplicates dropped:
    1. the earliest trade date with a taker-script fill
    2. the trade date of the self-trade-candidate fill
    3. the trade date with the most scalar-settled fills (ties: earliest)
    4. the trade date with the most derived-price fills (ties: earliest)
  Trades within a date: for each CASE below, in order, the first trade of
  that case by md5(trade_id) unless an already-picked trade covers it; then
  more trades by md5(trade_id) until --per-date is reached.
"""
import argparse
import datetime as dt
import json
import os
import sys
from decimal import Decimal

import duckdb
from lxml import etree

NS = "http://www.fixprotocol.org/FIXML-Latest"
FIXML_V = "FIX.Latest"       # fixed by the schema
FIXML_XV = "309"             # extension pack of the schema we validate against
SENDER_PLACEHOLDER = "PLACEHOLDER_SENDER_ID"
TARGET = "CFTC"

# Case name -> SQL predicate over the joined row (m = mart, f = fct_fills).
CASES = [
    ("side_yes", "m.side = 'yes'"),
    ("side_no", "m.side = 'no'"),
    ("scalar_settlement", "f.parlay_result = 'scalar'"),
    ("taker_script_fill", "f.fill_origin = 'taker_script'"),
    ("same_game_parlay", "f.is_same_game"),
    ("cost_estimated", "m.is_cost_estimated"),
    ("fee_unknown", "m.fee is null"),
    ("price_derived", "m.price_source <> 'recorded'"),
    ("exec_time_fallback", "m.is_execution_time_fallback"),
    ("no_quote_id", "m.quote_id is null"),
    ("quote_without_rfq_id", "m.quote_id is not null and m.rfq_id is null"),
    ("self_trade_candidate", "f.is_self_trade_candidate"),
    ("maker", "m.role = 'maker'"),
]

BASE_SQL = """
select m.*, f.order_id, f.fill_origin, f.parlay_result, f.is_same_game,
       f.is_self_trade_candidate, md5(m.trade_id) as _h
from marts.mart_daily_trade_report m
join marts.fct_fills f on f.fill_id = m.trade_id
"""


def choose_dates(con):
    q = f"""
    with b as ({BASE_SQL}),
    d as (
        select trade_date,
               count(*) filter (where fill_origin = 'taker_script') as n_taker,
               count(*) filter (where is_self_trade_candidate)      as n_self,
               count(*) filter (where parlay_result = 'scalar')     as n_scalar,
               count(*) filter (where price_source <> 'recorded')   as n_derived
        from b group by 1
    )
    select
      (select min(trade_date) from d where n_taker > 0),
      (select min(trade_date) from d where n_self > 0),
      (select trade_date from d order by n_scalar desc, trade_date limit 1),
      (select trade_date from d order by n_derived desc, trade_date limit 1)
    """
    out = []
    for d in con.execute(q).fetchone():
        if d is not None and d not in out:
            out.append(d)
    return out


def choose_trades(con, trade_date, per_date):
    case_cols = ", ".join(f"coalesce({p}, false) as c_{n}" for n, p in CASES)
    rows = con.execute(
        f"select m.trade_id, md5(m.trade_id) as _h, {case_cols} "
        f"from marts.mart_daily_trade_report m "
        f"join marts.fct_fills f on f.fill_id = m.trade_id "
        f"where m.trade_date = ? order by _h, m.trade_id",
        [trade_date],
    ).fetchall()
    names = [n for n, _ in CASES]
    flags = {r[0]: dict(zip(names, r[2:])) for r in rows}
    order = [r[0] for r in rows]
    picked, reason = [], {}
    for n in names:
        if any(flags[t][n] for t in picked):
            continue
        hit = next((t for t in order if flags[t][n]), None)
        if hit is not None and hit not in picked:
            picked.append(hit)
            reason[hit] = f"first {n} by md5(trade_id)"
    for t in order:
        if len(picked) >= per_date:
            break
        if t not in picked:
            picked.append(t)
            reason[t] = "fill-up by md5(trade_id)"
    cases = {t: [n for n in names if flags[t][n]] for t in picked}
    return picked, reason, cases, len(order)


def dec(x, places):
    return str(Decimal(x).quantize(Decimal(1).scaleb(-places)))


def ts(x):
    return x.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


def notes(r):
    """RptSide/@Txt: labelled key=value notes for what no standard field holds."""
    parts = []
    if r["rfq_id"] is not None:
        parts.append(f"rfq_id={r['rfq_id']}")
    if r["fee"] is None:
        parts.append("fee=unknown")
    if r["price_source"] != "recorded":
        parts.append(f"price_source={r['price_source']}")
    if r["is_cost_estimated"]:
        parts.append("cost_estimated=Y")
    if r["is_execution_time_fallback"]:
        parts.append("execution_time=received_time_fallback")
    return ";".join(parts) or None


def E(parent, tag, **attrs):
    el = etree.SubElement(parent, f"{{{NS}}}{tag}") if parent is not None else etree.Element(f"{{{NS}}}{tag}", nsmap={None: NS})
    for k, v in attrs.items():
        if v is not None:
            el.set(k, v)
    return el


def trade_capture_report(batch, r):
    yes_px = r["price"] if r["side"] == "yes" else Decimal(1) - r["price"]
    tcr = E(
        batch, "TrdCaptRpt",
        RptID=r["trade_id"], TrdID=r["trade_id"], MtchID=r["trade_id"],
        TransTyp="0", RptTyp="0",
        LastQty=dec(r["quantity"], 2), LastPx=dec(yes_px, 6), PxTyp="2",
        Ccy="USD", TrdDt=r["trade_date"].isoformat(),
        MLegRptTyp="1", TxnTm=ts(r["execution_time_utc"]),
        GrossTrdAmt=dec(r["notional"], 6),
    )
    E(tcr, "Instrmt", Sym=r["contract_ticker"])
    side = E(
        tcr, "RptSide",
        Side="1" if r["side"] == "yes" else "2",
        AgrsrInd="Y" if r["role"] == "taker" else "N",
        Txt=notes(r),
    )
    if r["fee"] is not None:
        E(side, "MiscFees", Amt=dec(r["fee"], 6), Curr="USD", Typ="4", Basis="0")
    E(side, "TrdRptOrdDetl", OrdID=r["order_id"],
      RefOrdID=r["quote_id"], RefOrdIDSrc="5" if r["quote_id"] else None)


def render(con, trade_date, trade_ids, created_at, out_dir):
    ph = ",".join("?" * len(trade_ids))
    cur = con.execute(f"select * from ({BASE_SQL}) b where trade_id in ({ph}) "
                      f"order by execution_time_utc, trade_id", trade_ids)
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, x)) for x in cur.fetchall()]
    assert len(rows) == len(trade_ids), "trade ids not found"
    root = E(None, "FIXML", v=FIXML_V, xv=FIXML_XV)
    batch = E(root, "Batch", ID=f"TCR-SAMPLE-{trade_date:%Y%m%d}", TotMsg=str(len(rows)))
    E(batch, "Hdr", SID=SENDER_PLACEHOLDER, TID=TARGET, Snt=ts(created_at))
    for r in rows:
        trade_capture_report(batch, r)
    path = os.path.join(out_dir, f"trade_capture_{trade_date:%Y%m%d}.fixml.xml")
    etree.ElementTree(root).write(path, xml_declaration=True, encoding="UTF-8", pretty_print=True)
    return path, len(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=os.environ.get("PM_DUCKDB_PATH", "work/pm_reporting.duckdb"))
    ap.add_argument("--out-dir", default=os.environ.get("PM_FIXML_DIR", "work/fixml"))
    ap.add_argument("--dates", help="comma-separated YYYY-MM-DD; default: the selection rule")
    ap.add_argument("--per-date", type=int, default=20)
    ap.add_argument("--created-at", help="ISO UTC time for Hdr/@Snt; default: now")
    a = ap.parse_args()

    out_dir = os.path.abspath(a.out_dir)
    if not 10 <= a.per_date <= 25:
        sys.exit("--per-date must be 10-25")
    os.makedirs(out_dir, exist_ok=True)
    created_at = (dt.datetime.fromisoformat(a.created_at.rstrip("Z")) if a.created_at
                  else dt.datetime.now(dt.timezone.utc).replace(tzinfo=None))

    con = duckdb.connect(a.db, read_only=True)
    dates = ([dt.date.fromisoformat(d) for d in a.dates.split(",")] if a.dates
             else choose_dates(con))
    manifest = {"created_at_utc": ts(created_at), "per_date": a.per_date,
                "date_rule": None if a.dates else
                ["earliest taker_script date", "self-trade-candidate date",
                 "most scalar fills", "most derived-price fills"],
                "files": []}
    for d in dates:
        ids, reason, cases, n_day = choose_trades(con, d, a.per_date)
        path, n = render(con, d, ids, created_at, out_dir)
        manifest["files"].append({
            "trade_date": d.isoformat(), "file": os.path.basename(path),
            "trades_in_file": n, "trades_on_date": n_day,
            "trades": [{"trade_id": t, "why": reason[t], "cases": cases[t]} for t in ids],
        })
        print(f"{d}  {n:>3} of {n_day:>5} trades  -> {path}")
    covered = sorted({c for f in manifest["files"] for t in f["trades"] for c in t["cases"]})
    missing = [n for n, _ in CASES if n not in covered]
    manifest["cases_covered"], manifest["cases_missing"] = covered, missing
    with open(os.path.join(out_dir, "selection_manifest.json"), "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2)
    print("cases missing:", missing or "none")


if __name__ == "__main__":
    main()
