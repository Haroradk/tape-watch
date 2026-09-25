"""The daily surveillance briefing: SQL gathers the evidence, one LLM call writes it up.

Why not a tool-calling agent that writes its own SQL? Budget and trust. A
tool loop spends one request per round trip, and the free tier is small; and
evidence gathered by fixed queries is the same every time, testable, and
auditable. So the split is:

  evidence   deterministic SQL per incident (gather_evidence): size and shape
             of the move, who was aggressive, one big trade or many small,
             did the other coin move too, what fired first, did it revert
  narrative  ONE Gemini call per day, structured output: a headline, a
             summary, and per incident a scope, a pattern label and a short
             explanation - using only numbers from the evidence

The model's answer is untrusted input: its shape is guaranteed by the
schema, its content isn't, so validate() checks it against the evidence
before anything is stored. It explains what happened; it never suggests
trades or predicts prices (this project never trades).

Budget guard: ops.llm_calls logs every call, and at most
DAILY_CALL_CAP briefing calls are made per calendar day. Beyond that, or if
Gemini refuses (e.g. quota), the day is stored with its evidence and status
'awaiting_analyst', and a later run catches it up. Quiet days (no
incidents) get a written briefing without any LLM call.
"""

import json
from datetime import date as Date
from datetime import datetime, timedelta
from typing import List, Literal

from pydantic import BaseModel, Field

from config import ALLOWED_LATENESS_S, GEMINI_API_KEY, GEMINI_BRIEFING_MODEL

DAILY_CALL_CAP = 3

DDL = """
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.daily_briefings (
    trade_date      DATE,
    run_id          VARCHAR,
    status          VARCHAR,      -- written / quiet / awaiting_analyst
    model           VARCHAR,
    headline        VARCHAR,
    summary         VARCHAR,
    data_notes      VARCHAR,
    briefing        JSON,         -- the model's full structured answer
    evidence        JSON,         -- exactly what the model was given
    created_at      TIMESTAMP
);

CREATE TABLE IF NOT EXISTS gold.incident_briefs (
    trade_date      DATE,
    run_id          VARCHAR,
    incident_no     INTEGER,
    symbol          VARCHAR,
    title           VARCHAR,
    scope           VARCHAR,      -- market_wide / single_asset / unclear
    pattern         VARCHAR,
    narrative       VARCHAR,
    evidence        JSON,
    created_at      TIMESTAMP
);

CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.llm_calls (
    called_at       TIMESTAMP,
    trade_date      DATE,
    model           VARCHAR,
    purpose         VARCHAR,
    status          VARCHAR,      -- ok / error / invalid
    prompt_chars    INTEGER,
    output_chars    INTEGER,
    error           VARCHAR
);
"""


# ---------------------------------------------------------------- evidence

INCIDENT_EVIDENCE_SQL = """
WITH inc AS (
    SELECT * FROM ops.incidents WHERE run_id = $run AND allowed_lateness_s = $lat
),
win AS (  -- the active part of each incident, from just before the first shock
    SELECT incident_no, symbol, opened_at - INTERVAL 60 SECOND AS w_start, last_active_at AS w_end FROM inc
),
flow AS (
    SELECT w.incident_no,
           sum(b.volume)::DOUBLE AS volume,
           (sum(b.sell_volume) / nullif(sum(b.volume), 0))::DOUBLE AS sell_share,
           sum(b.trade_count) AS trades
    FROM win w JOIN silver.bars_1s b
      ON b.run_id = $run AND b.allowed_lateness_s = $lat AND b.symbol = w.symbol
     AND b.bar_start >= w.w_start AND b.bar_start <= w.w_end
    GROUP BY 1
),
end_px AS (
    SELECT w.incident_no, arg_max(b.close, b.bar_start)::DOUBLE AS price_at_end
    FROM win w JOIN silver.bars_1s b
      ON b.run_id = $run AND b.allowed_lateness_s = $lat AND b.symbol = w.symbol
     AND b.bar_start >= w.w_start AND b.bar_start <= w.w_end
    GROUP BY 1
),
after AS (  -- where was the price 30 and 60 minutes after the incident went quiet?
    SELECT w.incident_no,
           max(CASE WHEN b.bar_start = time_bucket(INTERVAL 1 MINUTE, w.w_end) + INTERVAL 30 MINUTE THEN b.close END)::DOUBLE AS price_30m_after,
           max(CASE WHEN b.bar_start = time_bucket(INTERVAL 1 MINUTE, w.w_end) + INTERVAL 60 MINUTE THEN b.close END)::DOUBLE AS price_60m_after
    FROM win w JOIN silver.bars_1m b
      ON b.run_id = $run AND b.allowed_lateness_s = $lat AND b.symbol = w.symbol
    GROUP BY 1
),
other AS (  -- the other coin over the same window
    SELECT w.incident_no, b.symbol AS other_symbol,
           (arg_max(b.close, b.bar_start) / arg_min(b.open, b.bar_start) - 1)::DOUBLE AS other_return
    FROM win w JOIN silver.bars_1s b
      ON b.run_id = $run AND b.allowed_lateness_s = $lat AND b.symbol <> w.symbol
     AND b.bar_start >= w.w_start AND b.bar_start <= w.w_end
    GROUP BY 1, 2
),
paired AS (  -- incidents on the other coin that overlap, give or take 5 minutes
    SELECT a.incident_no, list(b.incident_no ORDER BY b.incident_no) AS overlapping_incidents
    FROM win a JOIN win b ON a.symbol <> b.symbol
     AND b.w_start <= a.w_end + INTERVAL 5 MINUTE AND b.w_end >= a.w_start - INTERVAL 5 MINUTE
    GROUP BY 1
),
alerts AS (  -- what fired, in order, relative to the incident opening
    SELECT a.incident_no,
           list({'rule': a.rule, 'seconds_from_open': epoch(a.fired_at - i.opened_at)::INTEGER}
                ORDER BY a.fired_at) AS alert_timeline
    FROM ops.alerts a JOIN inc i USING (incident_no)
    WHERE a.run_id = $run AND a.allowed_lateness_s = $lat
    GROUP BY 1
)
SELECT i.incident_no, i.symbol, i.status,
       strftime(i.opened_at, '%H:%M:%S') AS opened_utc,
       round(epoch(i.last_active_at - i.opened_at) / 60, 1) AS active_minutes,
       i.price_at_open AS price_before, i.price_low, i.price_high, e.price_at_end,
       round((i.price_low / i.price_at_open - 1) * 100, 2) AS lowest_vs_before_pct,
       round((i.price_high / i.price_at_open - 1) * 100, 2) AS highest_vs_before_pct,
       round((e.price_at_end / i.price_at_open - 1) * 100, 2) AS move_at_end_pct,
       round((af.price_30m_after / i.price_at_open - 1) * 100, 2) AS move_30m_after_pct,
       round((af.price_60m_after / i.price_at_open - 1) * 100, 2) AS move_60m_after_pct,
       round(i.max_abs_ret_60_ref_z, 1) AS max_move_vs_normal_x,
       round(i.max_vol_60_ref_ratio, 1) AS max_minute_volume_vs_normal_x,
       round(f.sell_share * 100, 1) AS sell_initiated_volume_pct,
       f.volume AS volume, f.trades AS trades,
       o.other_symbol, round(o.other_return * 100, 2) AS other_symbol_move_pct,
       coalesce(p.overlapping_incidents, []) AS overlapping_incidents_other_symbol,
       al.alert_timeline
FROM inc i
LEFT JOIN flow f USING (incident_no) LEFT JOIN end_px e USING (incident_no)
LEFT JOIN after af USING (incident_no) LEFT JOIN other o USING (incident_no)
LEFT JOIN paired p USING (incident_no) LEFT JOIN alerts al USING (incident_no)
ORDER BY i.opened_at
"""

# One big order or a crowd? Needs trade-level data, so it only works while
# bronze still holds the day (7-day retention) - the briefing runs right
# after the day is processed, so normally it does.
TRADE_SIZE_SQL = """
WITH w AS (
    SELECT incident_no, symbol, opened_at - INTERVAL 60 SECOND AS w_start, last_active_at AS w_end
    FROM ops.incidents WHERE run_id = $run AND allowed_lateness_s = $lat
),
t AS (
    SELECT w.incident_no, e.quantity::DOUBLE AS q,
           row_number() OVER (PARTITION BY w.incident_no ORDER BY e.quantity DESC) AS rnk
    FROM w JOIN bronze.trade_events e
      ON e.run_id = $run AND e.symbol = w.symbol AND e.event_time >= w.w_start AND e.event_time <= w.w_end
)
SELECT incident_no,
       round(max(q) / sum(q) * 100, 2) AS largest_trade_pct_of_volume,
       round(sum(q) FILTER (WHERE rnk <= 10) / sum(q) * 100, 1) AS top10_trades_pct_of_volume,
       round(median(q), 6) AS median_trade_size
FROM t GROUP BY 1
"""

DAY_SQL = """
SELECT b.symbol,
       arg_min(b.open, b.bar_start)::DOUBLE AS open, arg_max(b.close, b.bar_start)::DOUBLE AS close,
       round((arg_max(b.close, b.bar_start) / arg_min(b.open, b.bar_start) - 1) * 100, 2) AS day_return_pct,
       round((max(b.high) / min(b.low) - 1) * 100, 2) AS high_low_range_pct,
       round(sum(b.volume)::DOUBLE / (SELECT sum(p.vol_1m_median) * 60 FROM reference.profile p
                                       WHERE p.as_of_date = $day AND p.symbol = b.symbol), 2) AS volume_vs_normal_day_x
FROM silver.bars_1m b WHERE b.run_id = $run AND b.allowed_lateness_s = $lat
GROUP BY 1 ORDER BY 1
"""


def gather_evidence(con, trade_date: Date, run_id: str, lateness: float = ALLOWED_LATENESS_S) -> dict:
    params = {"run": run_id, "lat": lateness}
    incidents = con.execute(INCIDENT_EVIDENCE_SQL, params).df()
    sizes = {}
    if con.execute("SELECT count(*) FROM bronze.trade_events WHERE run_id = ? LIMIT 1", [run_id]).fetchone()[0]:
        sizes = con.execute(TRADE_SIZE_SQL, params).df().set_index("incident_no").to_dict("index")
    records = []
    for row in json.loads(incidents.to_json(orient="records")):
        row.update(sizes.get(row["incident_no"], {"trade_sizes": "unavailable (raw trades purged)"}))
        records.append(row)
    alerts = con.execute("""
        SELECT rule, count(*) AS alerts, count(*) FILTER (WHERE incident_no IS NULL) AS not_in_any_incident
        FROM ops.alerts WHERE run_id = ? AND allowed_lateness_s = ? GROUP BY 1 ORDER BY 1
    """, [run_id, lateness]).df()
    return {
        "trade_date": trade_date.isoformat(),
        "markets": json.loads(con.execute(DAY_SQL, dict(params, day=trade_date)).df().to_json(orient="records")),
        "alerts_by_rule": json.loads(alerts.to_json(orient="records")),
        "incidents": records,
    }


# ---------------------------------------------------------------- the model's answer

Pattern = Literal["liquidation_cascade", "large_single_trade", "broad_selling", "broad_buying",
                  "short_squeeze", "volatility_burst", "unclear"]


class IncidentNote(BaseModel):
    incident_no: int
    title: str = Field(description="Plain-language title, at most 12 words, e.g. 'BTC drops 2.1% in four minutes'")
    scope: Literal["market_wide", "single_asset", "unclear"]
    pattern: Pattern
    narrative: str = Field(description="2-4 sentences explaining what happened, citing numbers from the evidence")


class Briefing(BaseModel):
    headline: str = Field(description="One line for the whole day")
    summary: str = Field(description="3-5 sentences: the day in both markets, then the incidents together")
    incidents: List[IncidentNote]
    data_notes: str = Field(description="Caveats about the evidence (missing fields, ambiguity); empty if none")


PROMPT = """You are a market-surveillance analyst writing the daily briefing for a monitoring
desk. Below is the evidence for {trade_date}: a summary of BTCUSDT and ETHUSDT for the day and
every incident the surveillance rules opened, with measurements taken by fixed SQL queries.

How to read the evidence:
- price_before is the price just before the move that opened the incident. The *_pct fields are
  moves relative to it (lowest_vs_before_pct is positive if the price never went below where it
  started); move_30m_after_pct / move_60m_after_pct show whether it held or reverted.
- max_move_vs_normal_x: the biggest 60-second move, as a multiple of the typical 1-minute move
  at that hour of day over the previous week. max_minute_volume_vs_normal_x is the same for volume.
- sell_initiated_volume_pct: share of volume where the seller crossed the spread. Near 50 is
  balanced; far above means aggressive selling, far below aggressive buying.
- largest_trade_pct_of_volume / top10_trades_pct_of_volume: high = a few big orders did it;
  low = many small trades (typical of liquidation cascades and broad participation).
- overlapping_incidents_other_symbol and other_symbol_move_pct: did the other coin move too?
- alert_timeline: which rules fired and when, in seconds from the incident opening. Negative
  seconds mean the alert came before the price shock (e.g. volume moved first).

Rules for what you write:
- Use only numbers that appear in the evidence. Do not invent causes such as news events; you
  cannot see news. If the evidence can't tell patterns apart, use pattern "unclear".
- Describe and explain what happened. Do not recommend trades, give investment advice, or
  predict future prices.
- Treat incidents on the two coins that overlap in time as one market event where appropriate,
  and say so.
- Write one IncidentNote for every incident in the evidence, with the same incident_no.

Evidence (JSON):
{evidence}
"""


def validate(briefing: Briefing, evidence: dict) -> list:
    """Problems with the model's answer, checked against the evidence. Empty = fine."""
    problems = []
    expected = {i["incident_no"] for i in evidence["incidents"]}
    got = [n.incident_no for n in briefing.incidents]
    if set(got) - expected:
        problems.append(f"notes for incidents that don't exist: {sorted(set(got) - expected)}")
    if expected - set(got):
        problems.append(f"no note for incidents: {sorted(expected - set(got))}")
    if len(got) != len(set(got)):
        problems.append("duplicate incident notes")
    return problems


# ---------------------------------------------------------------- orchestration

def _calls_today(con) -> int:
    return con.execute("""
        SELECT count(*) FROM ops.llm_calls
        WHERE purpose = 'daily_briefing' AND called_at::DATE = current_date AND status <> 'skipped'
    """).fetchone()[0]


def _log_call(con, trade_date, status, prompt_chars, output_chars=0, error=None) -> None:
    con.execute("""INSERT INTO ops.llm_calls (called_at, trade_date, model, purpose, status, prompt_chars,
                                               output_chars, error)
                   VALUES (?, ?, ?, 'daily_briefing', ?, ?, ?, ?)""",
                [datetime.utcnow(), trade_date, GEMINI_BRIEFING_MODEL, status, prompt_chars, output_chars,
                 (error or "")[:500] or None])


def _call_model(prompt: str) -> Briefing:
    from src import llm  # imported lazily: only needed when there's something to write up
    return llm.generate(prompt, Briefing, GEMINI_BRIEFING_MODEL)


def _store(con, trade_date, run_id, status, evidence, briefing: Briefing = None, model=None) -> None:
    now = datetime.utcnow()
    if briefing is not None:
        headline, summary, notes = briefing.headline, briefing.summary, briefing.data_notes
    elif status == "quiet":
        m = {x["symbol"]: x for x in evidence["markets"]}
        headline = "Quiet day: no incidents"
        summary = " ".join(
            f"{s} {v['day_return_pct']:+.2f}% on the day (high-low range {v['high_low_range_pct']:.2f}%, "
            f"volume {v['volume_vs_normal_day_x']:.2f}x a normal day)." for s, v in m.items())
        notes = ""
    else:
        headline = f"{len(evidence['incidents'])} incidents - awaiting analyst"
        summary = "The evidence below was gathered, but no written briefing yet (LLM budget or availability)."
        notes = ""
    con.execute("""INSERT INTO gold.daily_briefings (trade_date, run_id, status, model, headline, summary,
                   data_notes, briefing, evidence, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                [trade_date, run_id, status, model, headline, summary, notes,
                 briefing.model_dump_json() if briefing else None, json.dumps(evidence), now])

    notes_by_no = {n.incident_no: n for n in (briefing.incidents if briefing else [])}
    con.execute("DELETE FROM gold.incident_briefs WHERE run_id = ?", [run_id])
    for inc in evidence["incidents"]:
        n = notes_by_no.get(inc["incident_no"])
        con.execute("""INSERT INTO gold.incident_briefs (trade_date, run_id, incident_no, symbol, title, scope,
                       pattern, narrative, evidence, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    [trade_date, run_id, inc["incident_no"], inc["symbol"], n.title if n else None,
                     n.scope if n else None, n.pattern if n else None, n.narrative if n else None,
                     json.dumps(inc), now])
    agent_status = "analysed" if briefing else ("awaiting_analyst" if evidence["incidents"] else None)
    if agent_status:
        con.execute("UPDATE ops.incidents SET agent_status = ? WHERE run_id = ? AND allowed_lateness_s = ?",
                    [agent_status, run_id, ALLOWED_LATENESS_S])


def write_briefing(con, trade_date: Date, run_id: str) -> str:
    """Gather evidence and write (or defer) the briefing for one day. Returns its status."""
    con.execute(DDL)
    evidence = gather_evidence(con, trade_date, run_id)
    if not evidence["incidents"]:
        _store(con, trade_date, run_id, "quiet", evidence)
        return "quiet"
    if not GEMINI_API_KEY or _calls_today(con) >= DAILY_CALL_CAP:
        _store(con, trade_date, run_id, "awaiting_analyst", evidence)
        return "awaiting_analyst"

    prompt = PROMPT.format(trade_date=trade_date.isoformat(), evidence=json.dumps(evidence, indent=1))
    try:
        briefing = _call_model(prompt)
    except Exception as e:  # quota, outage, bad response: keep the evidence, try again another day
        _log_call(con, trade_date, "error", len(prompt), error=repr(e))
        _store(con, trade_date, run_id, "awaiting_analyst", evidence)
        return "awaiting_analyst"

    problems = validate(briefing, evidence) if briefing else ["empty response"]
    _log_call(con, trade_date, "invalid" if problems else "ok", len(prompt),
              len(briefing.model_dump_json()) if briefing else 0, "; ".join(problems) or None)
    if problems:
        _store(con, trade_date, run_id, "awaiting_analyst", evidence)
        return "awaiting_analyst"
    _store(con, trade_date, run_id, "written", evidence, briefing, GEMINI_BRIEFING_MODEL)
    return "written"


def latest_status(con, trade_date: Date):
    row = con.execute("SELECT status FROM gold.daily_briefings WHERE trade_date = ? ORDER BY created_at DESC LIMIT 1",
                      [trade_date]).fetchone()
    return row[0] if row else None


def catch_up(con, official_runs: list) -> dict:
    """Write briefings for days that are missing one or are awaiting an analyst, newest first,
    until the day's budget runs out. official_runs: [(trade_date, run_id), ...]."""
    con.execute(DDL)
    results = {}
    for trade_date, run_id in sorted(official_runs, reverse=True):
        if latest_status(con, trade_date) in ("written", "quiet"):
            continue
        results[trade_date] = write_briefing(con, trade_date, run_id)
        if results[trade_date] == "awaiting_analyst":
            break  # out of budget or the model is unavailable: the next run picks up from here
    return results
