# FIXML trade capture report: field mapping and conventions

`scripts/render_fixml.py` turns rows of `mart_daily_trade_report` (plus
`fct_fills.order_id`) into FIXML `TrdCaptRpt` messages, one file per trade
date. `scripts/check_fixml.py` validates the files against the FIXML schema
and round-trips them against the mart.

## What this is, and what it is not

- **The rule.** CFTC Rule 16.02 requires designated contract markets to file
  daily trade and supporting data reports: transaction-level trade data and
  related order information. CFTC staff set the exact content and format; the
  Part 16.02 field specification is not publicly published.
- **The format.** These reports use the FIX standard TradeCaptureReport
  message (FIXML element `TrdCaptRpt`) inside a `Batch` wrapper whose header
  carries a sender id and `TID="CFTC"`, following the CFTC's public FIXML
  batch conventions.
- **So this is modeled on the standard, not certified against the CFTC's
  specification.** It has not been submitted anywhere.
- **It is one market participant's view of its own trades,** not an
  exchange's full daily file. An exchange's report would carry both sides of
  every trade on the venue. Here each report has one `RptSide`: this
  account's side.
- The files are samples (20 trades per date, chosen by a rule), not complete
  days. On this public copy every trade is synthetic.

## Schema and validation

- **Source:** the FIX Trading Community's "FIXML Schema – Latest EP"
  package, `fixmlschema_FIX.Latest_EP309.zip`. It is not redistributed here:
  `scripts/fetch_fixml_schema.py` downloads it from fixtrading.org, checks
  its sha256 (`2c1f0800…79931a3`), and unzips it under `work/fixml_schema/`.
- **Why not "5.0 SP2":** the FIX Trading Community no longer offers the
  SP2-specific package. It offers "FIX Latest", which is FIX 5.0 SP2 plus
  every extension pack since (EP309 here). So the namespace is
  `http://www.fixprotocol.org/FIXML-Latest` and `FIXML/@v` is `FIX.Latest`
  (the schema fixes it). Every element and attribute used here is a
  long-standing TradeCapture field, but they were **not checked against an
  SP2 schema**, since none could be obtained.
- **One-type patch (validation copy only):** EP309 does not compile as
  published. `NoTradingServices_t` in `fixml-fields-base-Latest.xsd` derives
  from a type `NumInGroup` that the package never defines. The validation
  copy adds `NumInGroup` as `xs:nonNegativeInteger` (FIX's NumInGroup
  datatype is a non-negative count) to `fixml-datatypes-Latest.xsd`. Nothing
  else changes, and no field used here touches that type. The fetch script
  refuses to patch if the type is already defined.
- **Validator:** lxml (libxml2) `XMLSchema` over `fixml-main-Latest.xsd`.
- **Schema validation is a local step, not part of CI.** The schema is a
  third-party package that this repo does not redistribute, and CI should
  not depend on, or automate downloads from, the FIX Trading Community's
  site on every push. CI renders the files and runs the round trip only
  (`scripts/run_all.py --no-fixml-schema`, which sets
  `PM_FIXML_SKIP_SCHEMA=1` for `check_fixml.py` and the Dagster check). To
  validate locally, after a build:

  ```bash
  python scripts/fetch_fixml_schema.py      # once: download, sha256 check, patch
  python scripts/render_fixml.py
  python scripts/check_fixml.py             # schema + round trip; ends "ALL CHECKS PASSED"
  ```

  `python scripts/run_all.py` without the flag does the same.
- **Schema validity is not enough on its own.** Many FIX enum types accept
  reserved or extra values; for example `Side="9"` is schema-valid (it means
  "Cross short"). The round-trip check is what proves the content.

## Exchange FIX conventions followed

Where FIX leaves a choice open, the files follow **a prediction market
exchange's published FIX conventions** (FIXT.1.1 / FIX50SP2) for
participant-to-exchange messages (QuoteRequest 35=R, Quote 35=S, AcceptQuote
35=UA, QuoteStatusReport 35=AI, ExecutionReport 35=8). Those are not a
regulatory report; they set field conventions only. The report format stays
TrdCaptRpt.

| Question | The exchange's convention (tag) | Used here |
| - | - | - |
| YES / NO side | ExecutionReport `Side(54)`: `1` = buy YES, `2` = buy NO | `RptSide/@Side` `1` = this account bought YES, `2` = bought NO |
| Price | ExecutionReport `LastPx(31)` is the YES price regardless of side; the NO price is 1 − LastPx | `@LastPx` = YES price in dollars. On a NO trade, `LastPx = 1 − price`. The amount actually paid is in `@GrossTrdAmt` |
| Trade id | ExecutionReport `TrdMatchID(880)`: the unique trade identifier | `@MtchID` (and `@TrdID`, `@RptID`) = trade_id |
| Order id | ExecutionReport `OrderID(37)` (exchange-assigned) | `TrdRptOrdDetl/@OrdID` = fct_fills.order_id |
| Quote id | `QuoteID(117)` on Quote, QuoteStatusReport, AcceptQuote | `TrdRptOrdDetl/@RefOrdID` with `@RefOrdIDSrc="5"` (Quote ID). TrdCaptRpt has no QuoteID field; this is FIX's standard way to reference a quote from an order detail |
| RFQ id | `QuoteReqID(131)` on the quote request | No TrdCaptRpt field holds an RFQ id. It is carried in `RptSide/@Txt` as `rfq_id=<id>` (see below) |
| Combo (parlay) | A combo is its own market. The legs are named only when the RFQ is created (with venue-specific custom tags); every later message, including ExecutionReport, identifies the combo by `Symbol(55)` alone | `Instrmt/@Sym` = the combo ticker. `@MLegRptTyp="1"` (single security). No legs in the report (see below) |
| Fee | ExecutionReport `MiscFeeAmt(137)` (dollars), `MiscFeeCurr(138)` USD, `MiscFeeType(139)` `4` = exchange fees, `MiscFeeBasis(891)` `0` = absolute | `RptSide/MiscFees` `@Amt @Curr="USD" @Typ="4" @Basis="0"` |
| Maker / taker | ExecutionReport `AggressorIndicator(1057)` | `RptSide/@AgrsrInd` `Y` = taker, `N` = maker |
| Quantity | `LastQty(32)`: fixed-point decimal, 0.01-contract increments | `@LastQty` with 2 decimals |

## Message layout

Values below are made up.

```xml
<FIXML xmlns="http://www.fixprotocol.org/FIXML-Latest" v="FIX.Latest" xv="309">
  <Batch ID="TCR-SAMPLE-20250310" TotMsg="20">
    <Hdr SID="PLACEHOLDER_SENDER_ID" TID="CFTC" Snt="2025-04-07T12:00:00.000000Z"/>
    <TrdCaptRpt RptID="t-1" TrdID="t-1" MtchID="t-1" TransTyp="0" RptTyp="0"
                LastQty="48.09" LastPx="0.200000" PxTyp="2" Ccy="USD"
                TrdDt="2025-03-10" MLegRptTyp="1"
                TxnTm="2025-03-10T00:24:41.616785Z" GrossTrdAmt="9.618000">
      <Instrmt Sym="CMB.0123456789AB"/>
      <RptSide Side="1" AgrsrInd="N" Txt="rfq_id=r-1">
        <MiscFees Amt="0.200000" Curr="USD" Typ="4" Basis="0"/>
        <TrdRptOrdDetl OrdID="o-1" RefOrdID="q-1" RefOrdIDSrc="5"/>
      </RptSide>
    </TrdCaptRpt>
    <!-- ... one TrdCaptRpt per trade ... -->
  </Batch>
</FIXML>
```

## Every populated field

| FIXML | FIX tag | Value / source | Notes |
| - | - | - | - |
| `FIXML/@v` | – | `FIX.Latest` | Fixed by the schema |
| `FIXML/@xv` | – | `309` | Extension pack of the schema used |
| `Batch/@ID` | BatchID (FIXML-only) | `TCR-SAMPLE-<yyyymmdd>` | Says it is a sample |
| `Batch/@TotMsg` | BatchTotalMessages (FIXML-only) | number of `TrdCaptRpt` in the file | Checked by check_fixml.py |
| `Batch/Hdr/@SID` | SenderCompID 49 | `PLACEHOLDER_SENDER_ID` | No regulator-assigned sender id exists for this account |
| `Batch/Hdr/@TID` | TargetCompID 56 | `CFTC` | Batch convention |
| `Batch/Hdr/@Snt` | SendingTime 52 | file creation time, UTC, microseconds | `--created-at` makes it reproducible |
| `@RptID` | TradeReportID 571 | `trade_id` | One report per trade, no amendments, so the trade id also identifies the report |
| `@TrdID` | TradeID 1003 | `trade_id` | |
| `@MtchID` | TrdMatchID 880 | `trade_id` | The exchange's ExecutionReport convention |
| `@TransTyp` | TradeReportTransType 487 | `0` New | |
| `@RptTyp` | TradeReportType 856 | `0` Submit | |
| `@LastQty` | LastQty 32 | `quantity` (2 dp) | Fractional contracts |
| `@LastPx` | LastPx 31 | YES price: `price` if side = yes, `1 − price` if side = no (6 dp) | See "Side and price" |
| `@PxTyp` | PriceType 423 | `2` Per unit | Dollars per contract; a contract pays at most $1 |
| `@Ccy` | Currency 15 | `USD` | |
| `@TrdDt` | TradeDate 75 | `trade_date` (UTC date) | The other marts use the reporting day (UTC-5); this report uses UTC, like its mart |
| `@MLegRptTyp` | MultiLegReportingType 442 | `1` Single security | The combo trades as one contract |
| `@TxnTm` | TransactTime 60 | `execution_time_utc`, `Z`, microseconds | Full precision of the source |
| `@GrossTrdAmt` | GrossTradeAmt 381 | `notional` (6 dp) | What was paid for the side bought, fee excluded |
| `Instrmt/@Sym` | Symbol 55 | `contract_ticker` | The combo ticker |
| `RptSide/@Side` | Side 54 | `1` if side = yes, `2` if side = no | |
| `RptSide/@AgrsrInd` | AggressorIndicator 1057 | `Y` if role = taker, `N` if maker | |
| `RptSide/@Txt` | Text 58 | labelled notes, see below | Omitted when there is nothing to note |
| `RptSide/MiscFees/@Amt` | MiscFeeAmt 137 | `fee` (6 dp) | Whole element omitted when `fee` is NULL |
| `RptSide/MiscFees/@Curr` | MiscFeeCurr 138 | `USD` | |
| `RptSide/MiscFees/@Typ` | MiscFeeType 139 | `4` Exchange fees | |
| `RptSide/MiscFees/@Basis` | MiscFeeBasis 891 | `0` Absolute | |
| `RptSide/TrdRptOrdDetl/@OrdID` | OrderID 37 | `fct_fills.order_id` | Not a mart_daily_trade_report column |
| `RptSide/TrdRptOrdDetl/@RefOrdID` | RefOrderID 1080 | `quote_id` | Omitted when NULL |
| `RptSide/TrdRptOrdDetl/@RefOrdIDSrc` | RefOrderIDSource 1081 | `5` Quote ID | Only with RefOrdID |

### `RptSide/@Txt` notes

`;`-separated `key=value` pairs, in this order, each only when it applies:

| Key | When | Why it is here |
| - | - | - |
| `rfq_id=<id>` | `rfq_id` is not NULL | No TrdCaptRpt field holds an RFQ id. Leaving it out would drop an id the mart has |
| `fee=unknown` | `fee` is NULL (estimate-cost trades) | `MiscFees` is then absent. The note makes the absence say "unknown", not "zero" |
| `price_source=derived_from_estimated_cost` | price was never recorded | `LastPx` is then notional / quantity |
| `cost_estimated=Y` | `is_cost_estimated` | Notional and price come from the bot's estimate, not the exchange's fill record |
| `execution_time=received_time_fallback` | `is_execution_time_fallback` | `TxnTm` is then the time the bot received the fill |

This is a convention of this project, not a standard FIX use: Text is a
free-text remark field. A reader that ignores it loses only the RFQ id and the
data-quality flags; every standard field means what FIX says it means.

## Conventions, and the choices behind them

### Side and price: a binary YES / NO purchase

- Every trade in the mart is a purchase: `side` is the side this account
  bought. FIX `Side` has no YES / NO values.
- **Chosen (exchange convention):** `Side=1` = bought YES, `Side=2` = bought
  NO, and `LastPx` is always the YES price. So a NO purchase at $0.30 is
  reported as `Side=2 LastPx=0.70`: the exchange's single-book view, where
  buying NO at p is selling YES at 1 − p. `GrossTrdAmt` keeps the dollars
  actually paid (quantity × 0.30 in the example).
- **Caveat:** `Side=2` does not mean the account reduced a YES position. The
  exchange does not net YES against NO: each side settles on its own at full
  count. Read `Side=2` as "bought NO".
- **Considered and dropped:** `Side=1` on every trade with the outcome side
  in `Instrmt/@SubTyp`, and `LastPx` = the price of the side bought. That
  matches the mart's columns more directly, but it invents an instrument
  qualifier and breaks the exchange's published convention.
- Round trip: `side = yes if Side=1 else no`; `price = LastPx if yes else
  1 − LastPx`. Both are exact in decimal arithmetic; check_fixml.py confirms
  it for every trade.

### Multi-leg combos

- **No clean fit exists for carrying the legs in a TrdCaptRpt, so the legs
  are left out.** The report identifies the combo by its ticker
  (`Instrmt/@Sym`) and marks it `MLegRptTyp=1` (single security). The legs
  stay in `fct_fill_legs`.
- Why the FIX multileg blocks don't fit:
  - `TrdLeg` and `SecurityType=MLEG` describe a strategy whose legs are
    positions, each with its own side, quantity and price. A parlay leg is a
    condition, not a position: there is no leg fill and no leg price, and the
    payoff is the product of the leg outcomes. Filling `LegSide` with the
    leg's YES / NO would misuse a buy / sell field.
  - `Undly` (underlying instruments) has no field for the leg's YES / NO
    selection, which is half of what defines a leg.
- The exchange agrees: it names legs only when the RFQ is created, then
  refers to the combo by `Symbol` alone, including on executions.
- `Instrmt/@SecTyp` is left out. No FIX SecurityType value names an event
  contract, and scalar settlements don't pay 0 / 1.

### Identifiers

- `trade_id` → `RptID`, `TrdID`, `MtchID`. The same id three times is
  deliberate: TrdCaptRpt expects a report id and a trade id, and the
  exchange's convention puts its trade id in TrdMatchID.
- `order_id` → `OrdID`; `quote_id` → `RefOrdID` + `RefOrdIDSrc=5`;
  `rfq_id` → `Txt` note.
- NULL ids stay absent, never empty strings or placeholders.

### Timestamps and dates

- `TxnTm` = `execution_time_utc`, ISO 8601 with `Z`, microsecond precision.
- `TrdDt` = the UTC date, matching the mart's `trade_date`.
- `Hdr/@Snt` = when the file was written.

## Fields a real TradeCaptureReport would carry that this data can't fill

| FIXML | Field | Why it is missing |
| - | - | - |
| `Batch/Hdr/@SID` (real value) | SenderCompID | Assigned to reporting entities; this account has none |
| `RptSide/Pty` R=4 / R=21 | Clearing firm / clearing organization | The account holds a direct position; the data has no clearing member or clearing house id |
| `RptSide/Pty` R=1 / R=12 / R=24 | Executing firm / trader / customer account | No account or trader identifiers in the extract |
| `RptSide/@AcctTyp`, `@CustCpcty` | Account type, customer type indicator | Not in the data |
| `TrdRptOrdDetl/@Cpcty`, `@OrdTyp`, `@Px`, `@TmInForce`, `@OrdStat` | Order capacity, type, limit price, time in force, status | The extract keeps the fill, not the order record. The quote's bids are quote prices, not an order's limit |
| Second `RptSide` + `Pty` R=17 / R=37 | Contra side, contra firm / trader | One participant's view. On maker fills the exchange exposes only a pseudonymous requester id (a shared SHA-256('') placeholder when obscured): not a trader id |
| `@BizDt` | Clearing business date | Not kept by the extract. UTC `TrdDt` is not a substitute |
| `@SettlDt`, `Instrmt/@MatDt` | Settlement / expiry date | Per leg, not per combo; in fct_fill_legs |
| `@TrdTyp`, `@VenuTyp`, `@ExecMeth` | Trade type, venue type, execution method | Not in the data |
| `@LastMkt`, `Pty` R=22 | Market / exchange id | No MIC on record |
| `RptSide/@PosEfct` | Open / close | Not recorded |
| `TrdRegTS` | Regulatory timestamps | Only the execution time is reliable (it is `TxnTm`) |
| `@ExecID`, `@OrigTrdID`, `TransTyp` 1 / 2 | Execution report id, corrections, cancels | No execution-report ids, busts or corrections in the extract |

## Mart columns not carried

- `leg_count`: a property of the combo, not of the trade. It is in
  `dim_combos` / `fct_fill_legs`.
- Every other column of `mart_daily_trade_report` is carried, as a field or
  as a `Txt` note, and round-tripped.
