"""Validate the sample FIXML files and round-trip them against the mart.

    python scripts/check_fixml.py [--fixml-dir DIR]
        [--schema PATH/fixml-main-Latest.xsd] [--db PATH]

1. Schema: every file must validate against the FIXML schema given by
   --schema (default: the patched copy of the FIX Trading Community's
   "FIXML Schema - Latest EP" package, EP309, that scripts/fetch_fixml_schema.py
   builds; see docs/FIXML_MAPPING.md for the one-type patch). The schema is
   never committed.
   If the schema cannot be loaded the script says so and falls back to
   well-formedness checks only (exit code 2, not 0).
2. Round trip: parse each file back and check, for every TrdCaptRpt, that the
   ids (trade, order, quote, RFQ), quantity, price, side, execution time,
   trade date, contract, notional, fee, role and data-quality notes equal
   mart_daily_trade_report (and fct_fills for order_id) EXACTLY, that the
   file holds exactly the trades in selection_manifest.json, and that
   Batch/@TotMsg equals the number of messages.
Exit 0 only when everything passes.
"""
import argparse
import datetime as dt
import glob
import json
import os
import sys
from decimal import Decimal

import duckdb
from lxml import etree

NS = "{http://www.fixprotocol.org/FIXML-Latest}"


def parse_ts(s):
    assert s.endswith("Z"), s
    return dt.datetime.fromisoformat(s[:-1])


def parse_notes(s):
    out = {}
    for part in (s or "").split(";"):
        if part:
            k, v = part.split("=", 1)
            out[k] = v
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--fixml-dir", default=os.environ.get("PM_FIXML_DIR", "work/fixml"))
    ap.add_argument("--schema", default=os.environ.get(
        "PM_FIXML_SCHEMA", "work/fixml_schema/latest_ep309_patched/fixml-main-Latest.xsd"))
    ap.add_argument("--db", default=os.environ.get("PM_DUCKDB_PATH", "work/pm_reporting.duckdb"))
    a = ap.parse_args()

    files = sorted(glob.glob(os.path.join(a.fixml_dir, "*.fixml.xml")))
    if not files:
        sys.exit("no FIXML files found")
    with open(os.path.join(a.fixml_dir, "selection_manifest.json"), encoding="utf-8") as fh:
        manifest = {f["file"]: f for f in json.load(fh)["files"]}

    schema, schema_ok = None, True
    try:
        schema = etree.XMLSchema(etree.parse(a.schema))
        print(f"schema loaded: {a.schema}")
    except Exception as e:  # noqa: BLE001 - report and fall back
        schema_ok = False
        print(f"SCHEMA NOT LOADED ({e}); well-formedness checks only")

    con = duckdb.connect(a.db, read_only=True)
    failures = []

    def fail(msg):
        failures.append(msg)
        print("  FAIL", msg)

    total = 0
    for path in files:
        name = os.path.basename(path)
        doc = etree.parse(path)  # raises on malformed XML
        print(f"{name}: well-formed")
        if schema is not None:
            if schema.validate(doc):
                print(f"{name}: schema-valid")
            else:
                for err in schema.error_log:
                    fail(f"{name}: schema: line {err.line}: {err.message}")

        batch = doc.getroot().find(f"{NS}Batch")
        hdr = batch.find(f"{NS}Hdr")
        if hdr.get("TID") != "CFTC" or not hdr.get("SID") or not hdr.get("Snt"):
            fail(f"{name}: header SID/TID/Snt")
        tcrs = batch.findall(f"{NS}TrdCaptRpt")
        if int(batch.get("TotMsg")) != len(tcrs) or len(batch) - 1 != len(tcrs):
            fail(f"{name}: TotMsg {batch.get('TotMsg')} != {len(tcrs)} messages")
        expected = {t["trade_id"] for t in manifest[name]["trades"]}
        got = [t.get("TrdID") for t in tcrs]
        if set(got) != expected or len(got) != len(set(got)):
            fail(f"{name}: trade set differs from selection_manifest.json")

        ph = ",".join("?" * len(got))
        cur = con.execute(
            f"select m.*, f.order_id from marts.mart_daily_trade_report m "
            f"join marts.fct_fills f on f.fill_id = m.trade_id where m.trade_id in ({ph})", got)
        cols = [d[0] for d in cur.description]
        mart = {r[cols.index("trade_id")]: dict(zip(cols, r)) for r in cur.fetchall()}

        for t in tcrs:
            tid = t.get("TrdID")
            m = mart.get(tid)
            if m is None:
                fail(f"{name}: {tid} not in mart")
                continue
            try:
                side = t.find(f"{NS}RptSide")
                instr = t.find(f"{NS}Instrmt")
                od = side.find(f"{NS}TrdRptOrdDetl")
                fee_el = side.find(f"{NS}MiscFees")
                nt = parse_notes(side.get("Txt"))
                side_s = {"1": "yes", "2": "no"}[side.get("Side")]
                yes_px = Decimal(t.get("LastPx"))
                price = yes_px if side_s == "yes" else Decimal(1) - yes_px
                checks = {
                    "RptID/MtchID = TrdID": t.get("RptID") == tid == t.get("MtchID"),
                    "side": side_s == m["side"],
                    "quantity": Decimal(t.get("LastQty")) == m["quantity"],
                    "price": price == m["price"],
                    "execution_time": parse_ts(t.get("TxnTm")) == m["execution_time_utc"],
                    "trade_date": dt.date.fromisoformat(t.get("TrdDt")) == m["trade_date"],
                    "contract": instr.get("Sym") == m["contract_ticker"],
                    "notional": Decimal(t.get("GrossTrdAmt")) == m["notional"],
                    "fee": (fee_el is None and m["fee"] is None and nt.get("fee") == "unknown")
                           or (fee_el is not None and Decimal(fee_el.get("Amt")) == m["fee"]
                               and "fee" not in nt),
                    "role": {"Y": "taker", "N": "maker"}[side.get("AgrsrInd")] == m["role"],
                    "order_id": od.get("OrdID") == m["order_id"],
                    "quote_id": od.get("RefOrdID") == m["quote_id"]
                                and (od.get("RefOrdIDSrc") == "5") == (m["quote_id"] is not None),
                    "rfq_id": nt.get("rfq_id") == m["rfq_id"],
                    "price_source": nt.get("price_source", "recorded") == m["price_source"],
                    "cost_estimated": (nt.get("cost_estimated") == "Y") == m["is_cost_estimated"],
                    "exec_time_fallback": ("execution_time" in nt) == m["is_execution_time_fallback"],
                }
                for k, ok in checks.items():
                    if not ok:
                        fail(f"{name}: {tid}: {k}")
            except Exception as e:  # noqa: BLE001 - a malformed report is a failure, not a crash
                fail(f"{name}: {tid}: unreadable report ({type(e).__name__}: {e})")
        total += len(tcrs)
        print(f"{name}: {len(tcrs)} trades round-tripped")

    print(f"\n{len(files)} files, {total} trades, {len(failures)} failures")
    if failures:
        sys.exit(1)
    if not schema_ok:
        sys.exit(2)
    print("ALL CHECKS PASSED")


if __name__ == "__main__":
    main()
