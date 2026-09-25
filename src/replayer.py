"""Replay a day of historical trades as if they were arriving live.

The replayer runs a simulated clock that advances `speed` times faster than
the wall clock. Every `tick` wall-seconds it emits, as one micro-batch, every
trade whose arrival time has passed on the simulated clock.

Arrival time normally equals event time. With late_fraction > 0, a random
share of trades is held back by a random delay (exponential, mean
late_delay_s) - they still carry their true event_time, they just show up
later, the way a real feed hiccups. Random rather than fixed delays mean no
single watermark setting catches every late trade, which is realistic.
"""

import time
import uuid
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

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


def _to_us(ts: datetime) -> int:
    return int((ts - datetime(1970, 1, 1)).total_seconds() * 1_000_000)


def _from_us(us: int) -> datetime:
    return datetime(1970, 1, 1) + timedelta(microseconds=int(us))


def load_source(con, symbols, date, window_start, window_end) -> pd.DataFrame:
    parts = []
    for symbol in symbols:
        download_day(symbol, date)
        sql = SOURCE_SQL.format(symbol=symbol, path=csv_path(symbol, date))
        parts.append(
            con.execute(
                f"SELECT * FROM ({sql}) WHERE event_us >= ? AND event_us < ?",
                [_to_us(window_start), _to_us(window_end)],
            ).df()
        )
    return pd.concat(parts, ignore_index=True)


def assign_arrivals(df: pd.DataFrame, late_fraction: float, late_delay_s: float, seed: int = 42) -> pd.DataFrame:
    """Give every trade an arrival time and sort the tape into arrival order."""
    rng = np.random.default_rng(seed)
    is_late = rng.random(len(df)) < late_fraction
    delay_us = rng.exponential(late_delay_s * 1_000_000, len(df)).astype("int64")
    df["arrival_us"] = df["event_us"] + np.where(is_late, delay_us, 0)
    # The stream carries no "I'm late" flag - that is the point. We only print
    # the count so the run summary can be checked against what silver detects.
    df.attrs["late_injected"] = int(is_late.sum())
    return df.sort_values(["arrival_us", "symbol", "agg_trade_id"], kind="stable").reset_index(drop=True)


INSERT_SQL = """
INSERT INTO bronze.trade_events
SELECT ?, ?, symbol, agg_trade_id, price, quantity, first_trade_id, last_trade_id,
       is_buyer_maker, make_timestamp(event_us), ?, ?
FROM batch
"""


def replay(
    date: str,
    symbols: list,
    window_start: datetime,
    window_end: datetime,
    speed: float = 60.0,
    tick: float = 0.5,
    late_fraction: float = 0.0,
    late_delay_s: float = 30.0,
) -> str:
    con = get_connection()
    ensure_tables(con)

    tape = assign_arrivals(load_source(con, symbols, date, window_start, window_end), late_fraction, late_delay_s)
    arrivals = tape["arrival_us"].to_numpy()
    print(f"Loaded {len(tape):,} trades for {', '.join(symbols)} "
          f"{window_start:%H:%M}-{window_end:%H:%M} UTC ({tape.attrs['late_injected']:,} will arrive late)")

    run_id = datetime.utcnow().strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]
    con.execute(
        "INSERT INTO bronze.replay_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 'running', 0)",
        [run_id, date, symbols, window_start, window_end, speed, late_fraction, late_delay_s, datetime.utcnow()],
    )

    start_us = _to_us(window_start)
    # Late trades can arrive after the window closes; keep the clock running until they're in.
    end_us = max(_to_us(window_end), int(arrivals[-1]) + 1) if len(tape) else _to_us(window_end)
    wall_start = time.monotonic()
    pos, batch_id, status = 0, 0, "completed"

    try:
        while pos < len(tape):
            if speed > 0:
                sim_now = min(start_us + int((time.monotonic() - wall_start) * speed * 1_000_000), end_us)
            else:
                sim_now = min(int(arrivals[pos]) + int(tick * 1_000_000), end_us)  # backfill: fixed sim steps
            upto = int(np.searchsorted(arrivals, sim_now, side="right"))

            if upto > pos:
                con.register("batch", tape.iloc[pos:upto])
                con.execute(INSERT_SQL, [run_id, batch_id, _from_us(sim_now), datetime.utcnow()])
                con.unregister("batch")
                pos, batch_id = upto, batch_id + 1
                if batch_id % 20 == 0:
                    print(f"  sim {_from_us(sim_now):%H:%M:%S}  batches {batch_id:>5}  events {pos:>10,}")

            if speed > 0:
                time.sleep(tick)
    except KeyboardInterrupt:
        status = "interrupted"
        print("\nInterrupted - marking run as interrupted.")

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
