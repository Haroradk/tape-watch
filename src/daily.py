"""The daily job: process one finished trading day end to end.

Binance publishes a day's files around 01:30-03:00 UTC the next night, so
production is a batch job, not a stream: replay yesterday as a fast backfill
through the same code the live demo uses (bronze -> silver -> rules), check
the result, then apply retention.

ops.daily_runs is the job's audit log and the "official run" pointer: the
latest completed row per trade_date is what the dashboard and the briefing
read. Re-running a day (--force) creates a new run and moves the pointer;
nothing is overwritten in place.
"""

import os
import subprocess
import traceback
from datetime import date as Date
from datetime import datetime, timedelta
from pathlib import Path

import requests

from config import ALLOWED_LATENESS_S, SYMBOLS, get_connection
from src import briefing, reference, rules
from src.bronze import ensure_tables as ensure_bronze
from src.download import csv_path
from src.replayer import new_run_id, replay
from src.silver import build_bars

# Raw trades are the bulk of the warehouse (5-10M rows a day) and can always
# be downloaded again from Binance. Silver bars, alerts and incidents are
# small and kept forever.
BRONZE_RETENTION_DAYS = 7

DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.daily_runs (
    trade_date          DATE,
    run_id              VARCHAR,
    status              VARCHAR,     -- running / completed / failed / source_missing
    started_at          TIMESTAMP,
    finished_at         TIMESTAMP,
    trades              BIGINT,
    alerts              INTEGER,
    incidents           INTEGER,
    minutes_checked     INTEGER,     -- 1m bars compared with Binance's own klines
    minutes_matching    INTEGER,     -- ... and how many matched exactly
    bronze_rows_purged  BIGINT,
    code_version        VARCHAR,     -- git commit the job ran
    error               VARCHAR
);
"""


def _code_version() -> str:
    if os.environ.get("GITHUB_SHA"):
        return os.environ["GITHUB_SHA"][:12]
    try:
        return subprocess.check_output(["git", "rev-parse", "--short=12", "HEAD"],
                                       cwd=Path(__file__).parent.parent, text=True).strip()
    except Exception:
        return "unknown"


def official_run(con, trade_date: Date):
    row = con.execute("""
        SELECT run_id FROM ops.daily_runs WHERE trade_date = ? AND status = 'completed'
        ORDER BY finished_at DESC LIMIT 1
    """, [trade_date]).fetchone()
    return row[0] if row else None


def _update(con, run_id: str, **fields) -> None:
    sets = ", ".join(f"{k} = ?" for k in fields)
    con.execute(f"UPDATE ops.daily_runs SET {sets} WHERE run_id = ?", list(fields.values()) + [run_id])


def purge_bronze(con, trade_date: Date) -> int:
    """Drop raw trades for runs of days older than the retention window."""
    cutoff = trade_date - timedelta(days=BRONZE_RETENTION_DAYS)
    runs = [r[0] for r in con.execute(
        "SELECT run_id FROM bronze.replay_runs WHERE trade_date < ? AND bronze_purged_at IS NULL", [cutoff]
    ).fetchall()]
    if not runs:
        return 0
    purged = con.execute("SELECT count(*) FROM bronze.trade_events WHERE list_contains(?, run_id)", [runs]).fetchone()[0]
    con.execute("DELETE FROM bronze.trade_events WHERE list_contains(?, run_id)", [runs])
    con.execute("UPDATE bronze.replay_runs SET bronze_purged_at = ? WHERE list_contains(?, run_id)",
                [datetime.utcnow(), runs])
    return purged


def run_day(trade_date: Date, force: bool = False, delete_raw: bool = False) -> str:
    """Process one day. Returns the resulting status."""
    con = get_connection()
    con.execute(DDL)
    if not force and official_run(con, trade_date):
        print(f"{trade_date}: already processed (run {official_run(con, trade_date)}) - skipping")
        return "skipped"

    run_id = new_run_id()
    con.execute("INSERT INTO ops.daily_runs (trade_date, run_id, status, started_at, code_version) VALUES (?, ?, 'running', ?, ?)",
                [trade_date, run_id, datetime.utcnow(), _code_version()])
    print(f"{trade_date}: daily run {run_id}")
    start = datetime.combine(trade_date, datetime.min.time())

    try:
        # Fast backfill: no sleeping, 10 simulated seconds per micro-batch.
        # Batch size is a latency knob, and in a batch job latency doesn't matter.
        replay(trade_date.isoformat(), SYMBOLS, start, start + timedelta(days=1), speed=0, tick=10, run_id=run_id)
        con = get_connection()  # replay() closes its own; reconnect after the long write
        build_bars(con, run_id, ALLOWED_LATENESS_S)
        engine = rules.evaluate(con, run_id, ALLOWED_LATENESS_S)
        check = reference.reconcile(con, run_id, ALLOWED_LATENESS_S)
        checked, matching = int(check["minutes"].sum()), int(check["all_exact"].sum())
        trades = con.execute("SELECT events_emitted FROM bronze.replay_runs WHERE run_id = ?", [run_id]).fetchone()[0]
        _update(con, run_id, status="completed", finished_at=datetime.utcnow(), trades=trades,
                alerts=len(engine.alerts), incidents=len(engine.incidents),
                minutes_checked=checked, minutes_matching=matching)
        print(f"{trade_date}: {trades:,} trades, {len(engine.alerts)} alerts, {len(engine.incidents)} incidents, "
              f"{matching}/{checked} minutes match Binance klines")
        if matching < checked:
            print(f"{trade_date}: WARNING - {checked - matching} minutes differ from Binance's own candles")
    except requests.HTTPError as e:
        status = "source_missing" if e.response is not None and e.response.status_code == 404 else "failed"
        _update(get_connection(), run_id, status=status, finished_at=datetime.utcnow(), error=str(e)[:500])
        print(f"{trade_date}: {status} - {e}")
        return status
    except Exception as e:
        _update(get_connection(), run_id, status="failed", finished_at=datetime.utcnow(),
                error=traceback.format_exc()[-2000:])
        print(f"{trade_date}: failed - {e!r}")
        return "failed"

    ensure_bronze(con)
    purged = purge_bronze(con, trade_date)
    _update(con, run_id, bronze_rows_purged=purged)
    if purged:
        print(f"{trade_date}: retention - purged {purged:,} bronze rows older than {BRONZE_RETENTION_DAYS} days")
    if delete_raw:
        for symbol in SYMBOLS:
            csv_path(symbol, trade_date.isoformat()).unlink(missing_ok=True)
    return "completed"


def write_briefings(lookback_days: int = 14) -> dict:
    """Briefings for recent official runs that don't have one yet, newest first, within the LLM budget."""
    con = get_connection()
    runs = con.execute("""
        SELECT trade_date, arg_max(run_id, finished_at) FROM ops.daily_runs
        WHERE status = 'completed' AND trade_date >= current_date - ?::INTEGER
        GROUP BY 1
    """, [lookback_days]).fetchall()
    return briefing.catch_up(con, runs)
