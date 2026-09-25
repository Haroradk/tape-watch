"""Replay a day of historical trades as if they were arriving live.

The replayer runs a simulated clock that advances `speed` times faster than
the wall clock. Every `tick` wall-seconds it emits, as one micro-batch, every
trade whose arrival time has passed on the simulated clock.

Arrival time normally equals event time. With late_fraction > 0, a random
share of trades is held back by a random delay (exponential, mean
late_delay_s) - they still carry their true event_time, they just show up
later, the way a real feed hiccups. Random rather than fixed delays mean no
single watermark setting catches every late trade, which is realistic.

A micro-batch (batch_id) is a logical unit - what arrived together. How rows
are physically written is separate: live mode writes each batch as it's
emitted, backfill mode groups many batches per insert, because every insert
is a network round trip to MotherDuck.
"""

import time
import uuid
from datetime import datetime, timedelta

import duckdb
import numpy as np
import pyarrow as pa

from config import get_connection
from src.bronze import ensure_tables
from src.download import csv_path, download_day

# Binance switched spot timestamps from milliseconds to microseconds on
# 2025-01-01. Epoch-ms values are ~1.7e12 and epoch-us values ~1.7e15, so the
# magnitude tells us which unit a row is in - safe across mixed-year files.
SOURCE_SQL = """
SELECT
    '{symbol}' AS symbol,
    agg_trade_id, price, quantity, first_trade_id, last_trade_id, is_buyer_maker,
    CASE WHEN ts > 100000000000000 THEN ts ELSE ts * 1000 END AS event_us
FROM read_csv('{path}', header = false, columns = {{
    'agg_trade_id': 'BIGINT', 'price': 'DECIMAL(18,8)', 'quantity': 'DECIMAL(18,8)',
    'first_trade_id': 'BIGINT', 'last_trade_id': 'BIGINT', 'ts': 'BIGINT',
    'is_buyer_maker': 'BOOLEAN', 'is_best_match': 'BOOLEAN'
}})
"""

BACKFILL_FLUSH_ROWS = 250_000


def new_run_id() -> str:
    return datetime.utcnow().strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]


def _to_us(ts: datetime) -> int:
    return int((ts - datetime(1970, 1, 1)).total_seconds() * 1_000_000)


def _from_us(us: int) -> datetime:
    return datetime(1970, 1, 1) + timedelta(microseconds=int(us))


def load_source(symbols, date, window_start, window_end) -> pa.Table:
    # Source files are read by a local, in-memory DuckDB: they're on this
    # laptop, and the warehouse connection may be MotherDuck.
    local = duckdb.connect()
    parts = []
    for symbol in symbols:
        download_day(symbol, date)
        sql = SOURCE_SQL.format(symbol=symbol, path=csv_path(symbol, date))
        parts.append(local.execute(
            f"SELECT * FROM ({sql}) WHERE event_us >= ? AND event_us < ?",
            [_to_us(window_start), _to_us(window_end)],
        ).fetch_arrow_table())
    return pa.concat_tables(parts)


def assign_arrivals(tape: pa.Table, late_fraction: float, late_delay_s: float, seed: int = 42):
    """Give every trade an arrival time and sort the tape into arrival order."""
    rng = np.random.default_rng(seed)
    event_us = tape["event_us"].to_numpy()
    is_late = rng.random(len(event_us)) < late_fraction
    delay_us = rng.exponential(late_delay_s * 1_000_000, len(event_us)).astype("int64")
    arrival_us = event_us + np.where(is_late, delay_us, 0)

    _, symbol_codes = np.unique(tape["symbol"].to_numpy(zero_copy_only=False), return_inverse=True)
    order = np.lexsort((tape["agg_trade_id"].to_numpy(), symbol_codes, arrival_us))
    tape = tape.take(pa.array(order)).append_column("arrival_us", pa.array(arrival_us[order]))
    # The stream carries no "I'm late" flag - that is the point. We only
    # report the count so it can be checked against what silver detects.
    return tape, int(is_late.sum())


INSERT_SQL = """
INSERT INTO bronze.trade_events (run_id, batch_id, symbol, agg_trade_id, price, quantity, first_trade_id,
                                 last_trade_id, is_buyer_maker, event_time, arrival_time, ingested_at)
SELECT ?, batch_id, symbol, agg_trade_id, price, quantity, first_trade_id, last_trade_id,
       is_buyer_maker, make_timestamp(event_us), make_timestamp(arrival_clock_us), ?
FROM batch
"""


def _flush(con, run_id: str, segments: list) -> None:
    """Write buffered micro-batches: [(batch_id, sim_now_us, arrow slice), ...], then the heartbeat."""
    if not segments:
        return
    sim_clock = _from_us(segments[-1][1])
    tables = []
    for batch_id, sim_now, part in segments:
        n = part.num_rows
        tables.append(part.append_column("batch_id", pa.array(np.full(n, batch_id, dtype="int32")))
                          .append_column("arrival_clock_us", pa.array(np.full(n, sim_now, dtype="int64"))))
    con.register("batch", pa.concat_tables(tables))
    con.execute(INSERT_SQL, [run_id, datetime.utcnow()])
    con.unregister("batch")
    con.execute("UPDATE bronze.replay_runs SET sim_clock = ? WHERE run_id = ?", [sim_clock, run_id])
    segments.clear()


def replay(
    date: str,
    symbols: list,
    window_start: datetime,
    window_end: datetime,
    speed: float = 60.0,
    tick: float = 0.5,
    late_fraction: float = 0.0,
    late_delay_s: float = 30.0,
    run_id: str = None,
) -> str:
    con = get_connection()
    ensure_tables(con)

    tape, late_injected = assign_arrivals(load_source(symbols, date, window_start, window_end),
                                          late_fraction, late_delay_s)
    arrivals = tape["arrival_us"].to_numpy()
    print(f"Loaded {len(tape):,} trades for {', '.join(symbols)} "
          f"{window_start:%Y-%m-%d %H:%M} to {window_end:%Y-%m-%d %H:%M} UTC ({late_injected:,} will arrive late)")

    run_id = run_id or new_run_id()
    con.execute(
        # Named columns: the table has gained columns over time (sim_clock,
        # bronze_purged_at), and a positional VALUES list breaks every time.
        """INSERT INTO bronze.replay_runs (run_id, trade_date, symbols, window_start, window_end, speed,
               late_fraction, late_delay_s, started_at, status, events_emitted, sim_clock)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'running', 0, ?)""",
        [run_id, date, symbols, window_start, window_end, speed, late_fraction, late_delay_s, datetime.utcnow(),
         window_start],
    )
    print(f"Run {run_id} started")

    start_us = _to_us(window_start)
    # Late trades can arrive after the window closes; keep the clock running until they're in.
    end_us = max(_to_us(window_end), int(arrivals[-1]) + 1) if len(tape) else _to_us(window_end)
    wall_start = time.monotonic()
    pos, batch_id, status = 0, 0, "completed"
    segments, buffered = [], 0

    try:
        while pos < len(tape):
            loop_start = time.monotonic()
            if speed > 0:
                sim_now = min(start_us + int((time.monotonic() - wall_start) * speed * 1_000_000), end_us)
            else:
                sim_now = min(int(arrivals[pos]) + int(tick * 1_000_000), end_us)  # backfill: fixed sim steps
            upto = int(np.searchsorted(arrivals, sim_now, side="right"))

            if upto > pos:
                segments.append((batch_id, sim_now, tape.slice(pos, upto - pos)))
                buffered += upto - pos
                pos, batch_id = upto, batch_id + 1
                if speed > 0 or buffered >= BACKFILL_FLUSH_ROWS or pos == len(tape):
                    _flush(con, run_id, segments)
                    buffered = 0
                if speed > 0 and batch_id % 20 == 0:
                    lag = (time.monotonic() - wall_start) * speed - (sim_now - start_us) / 1e6
                    print(f"  sim {_from_us(sim_now):%H:%M:%S}  batches {batch_id:>5}  events {pos:>10,}"
                          + (f"  (behind by {lag:.0f} sim s)" if lag > 5 else ""))

            if speed > 0:
                # Sleep out the rest of the tick. Sleeping a full tick *after* a
                # slow insert made batches drift larger and larger.
                time.sleep(max(0.0, tick - (time.monotonic() - loop_start)))
    except KeyboardInterrupt:
        status = "interrupted"
        print("\nInterrupted - marking run as interrupted.")

    _flush(con, run_id, segments)
    con.execute(
        "UPDATE bronze.replay_runs SET finished_at = ?, status = ?, events_emitted = ? WHERE run_id = ?",
        [datetime.utcnow(), status, pos, run_id],
    )
    reconcile(con, run_id, expected=pos)
    con.close()
    return run_id


def reconcile(con, run_id: str, expected: int) -> None:
    """Did bronze receive exactly what the replayer says it sent, once each?"""
    landed, distinct = con.execute(
        "SELECT count(*), count(DISTINCT (symbol, agg_trade_id)) FROM bronze.trade_events WHERE run_id = ?",
        [run_id],
    ).fetchone()
    ok = landed == expected == distinct
    print(f"Run {run_id}: emitted {expected:,}, landed {landed:,}, distinct {distinct:,} -> {'OK' if ok else 'MISMATCH'}")
