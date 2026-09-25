"""Silver: 1-second and 1-minute bars, built the way a stream processor would.

The builder consumes a bronze run batch by batch, in arrival order - exactly
what a live consumer sees - and never looks ahead. The question it has to
answer for every bar is "is this second finished, or could more trades for it
still show up?". It can't know for sure, so it uses a watermark:

    watermark = latest event_time seen so far - allowed_lateness

A bar [t, t+1s) is closed and written to silver once t+1s <= watermark. Any
trade that turns up for an already-closed bar is too late: it goes to
silver.late_trades instead of silently changing a bar that downstream
(rules, the agent) may already have acted on.

allowed_lateness is the knob. Bigger = fewer dropped trades but every bar is
published later; smaller = fast bars that are sometimes wrong. There is no
free setting, which is what scripts/lateness_experiment.py shows.

Because consumption is replayed from bronze, the same run can be rebuilt with
different lateness settings and the results compared side by side - each
build is keyed by (run_id, allowed_lateness_s).

The builder is a stateful operator: its working state (trades for bars that
are still open, and 1s bars for minutes that are still open) lives in a
local in-memory DuckDB, and only finished output goes to the warehouse. Every
warehouse call is a network round trip to MotherDuck, so state that changes
thousands of times a minute has to stay local - the same split as a Flink or
Spark job's state store vs its sink.

Two ways to drive it, same operator underneath:
  build_bars()  offline: read a finished bronze run, write all output at once
  follow()      live: poll bronze for new batches while the replayer runs
"""

import time
from datetime import datetime, timedelta

import duckdb
import numpy as np
import pyarrow as pa

DDL = """
CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.bars_1s (
    run_id              VARCHAR,
    allowed_lateness_s  DOUBLE,
    symbol              VARCHAR,
    bar_start           TIMESTAMP,
    open                DECIMAL(18, 8),
    high                DECIMAL(18, 8),
    low                 DECIMAL(18, 8),
    close               DECIMAL(18, 8),
    volume              DECIMAL(24, 8),   -- base asset, e.g. BTC
    quote_volume        DECIMAL(28, 8),   -- USDT
    sell_volume         DECIMAL(24, 8),   -- sell-initiated (is_buyer_maker)
    trade_count         INTEGER,
    emitted_at          TIMESTAMP         -- sim clock when the watermark closed it
);

CREATE TABLE IF NOT EXISTS silver.bars_1m (
    run_id              VARCHAR,
    allowed_lateness_s  DOUBLE,
    symbol              VARCHAR,
    bar_start           TIMESTAMP,
    open                DECIMAL(18, 8),
    high                DECIMAL(18, 8),
    low                 DECIMAL(18, 8),
    close               DECIMAL(18, 8),
    vwap                DECIMAL(18, 8),
    volume              DECIMAL(24, 8),
    quote_volume        DECIMAL(28, 8),
    sell_volume         DECIMAL(24, 8),
    trade_count         INTEGER,
    active_seconds      INTEGER,          -- seconds with at least one trade
    emitted_at          TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.late_trades (
    run_id              VARCHAR,
    allowed_lateness_s  DOUBLE,
    symbol              VARCHAR,
    agg_trade_id        BIGINT,
    quantity            DECIMAL(18, 8),
    event_time          TIMESTAMP,
    arrival_time        TIMESTAMP,
    watermark_at_arrival TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.builds (
    run_id              VARCHAR,
    allowed_lateness_s  DOUBLE,
    built_at            TIMESTAMP,
    batches             INTEGER,
    trades_accepted     BIGINT,
    trades_dropped      BIGINT,
    PRIMARY KEY (run_id, allowed_lateness_s)
);
"""

LOCAL_STATE_DDL = """
CREATE TABLE pending (
    symbol VARCHAR, agg_trade_id BIGINT, price DECIMAL(18, 8), quantity DECIMAL(18, 8),
    is_buyer_maker BOOLEAN, event_time TIMESTAMP, arrival_time TIMESTAMP
);
CREATE TABLE recent_1s AS SELECT * FROM (SELECT
    ''::VARCHAR AS run_id, 0::DOUBLE AS allowed_lateness_s, ''::VARCHAR AS symbol, NULL::TIMESTAMP AS bar_start,
    0::DECIMAL(18, 8) AS open, 0::DECIMAL(18, 8) AS high, 0::DECIMAL(18, 8) AS low, 0::DECIMAL(18, 8) AS close,
    0::DECIMAL(24, 8) AS volume, 0::DECIMAL(28, 8) AS quote_volume, 0::DECIMAL(24, 8) AS sell_volume,
    0::INTEGER AS trade_count, NULL::TIMESTAMP AS emitted_at) LIMIT 0;
"""

BATCH_COLUMNS = "symbol, agg_trade_id, price, quantity, is_buyer_maker, event_time, arrival_time"
BAR_END = "time_bucket(INTERVAL 1 SECOND, event_time) + INTERVAL 1 SECOND"

# Open/close use agg_trade_id rather than event_time: Binance assigns ids in
# exchange order, so it's an exact tiebreak where several trades share a
# microsecond - and it stays correct when a late trade arrives out of order.
CLOSE_1S_SQL = f"""
SELECT ?::VARCHAR AS run_id, ?::DOUBLE AS allowed_lateness_s, symbol,
       time_bucket(INTERVAL 1 SECOND, event_time) AS bar_start,
       arg_min(price, agg_trade_id)::DECIMAL(18, 8) AS open, max(price) AS high, min(price) AS low,
       arg_max(price, agg_trade_id)::DECIMAL(18, 8) AS close,
       sum(quantity)::DECIMAL(24, 8) AS volume,
       sum(CAST(price AS DECIMAL(38, 8)) * quantity)::DECIMAL(28, 8) AS quote_volume,
       coalesce(sum(quantity) FILTER (WHERE is_buyer_maker), 0)::DECIMAL(24, 8) AS sell_volume,
       count(*)::INTEGER AS trade_count, ?::TIMESTAMP AS emitted_at
FROM pending
WHERE {BAR_END} <= ?
GROUP BY symbol, bar_start
ORDER BY bar_start, symbol
"""

CLOSE_1M_SQL = """
SELECT run_id, allowed_lateness_s, symbol, time_bucket(INTERVAL 1 MINUTE, bar_start) AS minute,
       arg_min(open, bar_start) AS open, max(high) AS high, min(low) AS low, arg_max(close, bar_start) AS close,
       (sum(quote_volume) / sum(volume))::DECIMAL(18, 8) AS vwap,
       sum(volume) AS volume, sum(quote_volume) AS quote_volume, sum(sell_volume) AS sell_volume,
       sum(trade_count)::INTEGER AS trade_count, count(*)::INTEGER AS active_seconds,
       ?::TIMESTAMP AS emitted_at
FROM recent_1s
WHERE bar_start >= ? AND bar_start < ?
GROUP BY ALL
ORDER BY minute, symbol
"""


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(DDL)


def _floor_minute(ts: datetime) -> datetime:
    return ts.replace(second=0, microsecond=0)


class BarBuilder:
    """Watermark bar builder. Feed it bronze micro-batches in arrival order."""

    def __init__(self, run_id: str, allowed_lateness_s: float):
        self.key = [run_id, float(allowed_lateness_s)]
        self.lateness = timedelta(seconds=allowed_lateness_s)
        self.state = duckdb.connect()  # in-memory, private to this operator
        self.state.execute("SET TimeZone = 'UTC'")
        self.state.execute(LOCAL_STATE_DDL)
        self.max_event = None
        self.watermark = None
        self.next_minute = None  # first minute whose 1m bar hasn't been emitted yet
        self.last_arrival = None
        self.batches = self.accepted = self.dropped = 0

    def process(self, batch: pa.Table = None, final: bool = False) -> dict:
        """Process one micro-batch (or none, with final=True, to flush). Returns closed output."""
        db = self.state
        out = {}

        if batch is not None and batch.num_rows:
            db.register("batch", batch)
            self.last_arrival = db.execute("SELECT max(arrival_time) FROM batch").fetchone()[0]
            batch_max_event = db.execute("SELECT max(event_time) FROM batch").fetchone()[0]

            # 1. Anything for a bar the previous watermark already closed is too late.
            if self.watermark is not None:
                out["late_trades"] = db.execute(f"""
                    SELECT ?::VARCHAR, ?::DOUBLE, symbol, agg_trade_id, quantity, event_time, arrival_time,
                           ?::TIMESTAMP
                    FROM batch WHERE {BAR_END} <= ?
                """, self.key + [self.watermark, self.watermark]).fetch_arrow_table()
                self.dropped += out["late_trades"].num_rows
            db.execute(f"INSERT INTO pending SELECT {BATCH_COLUMNS} FROM batch WHERE ? IS NULL OR {BAR_END} > ?",
                       [self.watermark, self.watermark])
            db.unregister("batch")
            self.batches += 1

            # 2. Advance the watermark. It only moves forward: a late trade carries
            #    an old event_time, so it can never pull the watermark back.
            self.max_event = batch_max_event if self.max_event is None else max(self.max_event, batch_max_event)
            self.watermark = self.max_event - self.lateness
            if self.next_minute is None:
                self.next_minute = _floor_minute(db.execute("SELECT min(event_time) FROM pending").fetchone()[0])

        if self.max_event is None:
            return out
        if final:
            self.watermark = datetime.max  # end of stream: flush everything

        # 3. Close every 1s bar the watermark has passed and drop its trades from state.
        bars_1s = db.execute(CLOSE_1S_SQL, self.key + [self.last_arrival, self.watermark]).fetch_arrow_table()
        if bars_1s.num_rows:
            db.register("closed", bars_1s)
            db.execute("INSERT INTO recent_1s SELECT * FROM closed")
            db.unregister("closed")
            db.execute(f"DELETE FROM pending WHERE {BAR_END} <= ?", [self.watermark])
            self.accepted += int(np.sum(bars_1s["trade_count"].to_numpy()))
            out["bars_1s"] = bars_1s

        # 4. A minute is closed once all its seconds are - roll those 1s bars up.
        closed_upto = _floor_minute(min(self.watermark, self.max_event + timedelta(minutes=1)))
        if closed_upto > self.next_minute:
            out["bars_1m"] = db.execute(CLOSE_1M_SQL, [self.last_arrival, self.next_minute, closed_upto]).fetch_arrow_table()
            db.execute("DELETE FROM recent_1s WHERE bar_start < ?", [closed_upto])
            self.next_minute = closed_upto
        return out


def write_output(con, outputs: list) -> None:
    """One insert per table for a list of process() results."""
    for table in ("bars_1s", "bars_1m", "late_trades"):
        parts = [o[table] for o in outputs if table in o and o[table].num_rows]
        if parts:
            con.register("out", pa.concat_tables(parts))
            con.execute(f"INSERT INTO silver.{table} SELECT * FROM out")
            con.unregister("out")


def _reset(con, key: list) -> None:
    ensure_tables(con)
    for table in ("bars_1s", "bars_1m", "late_trades", "builds"):
        con.execute(f"DELETE FROM silver.{table} WHERE run_id = ? AND allowed_lateness_s = ?", key)


def _record_build(con, builder: BarBuilder) -> None:
    con.execute("INSERT INTO silver.builds VALUES (?, ?, ?, ?, ?, ?)",
                builder.key + [datetime.utcnow(), builder.batches, builder.accepted, builder.dropped])


def _batches(trades: pa.Table):
    """Split a table sorted by batch_id into per-batch slices (zero-copy)."""
    ids = trades["batch_id"].to_numpy()
    edges = np.flatnonzero(np.diff(ids)) + 1
    starts = np.concatenate([[0], edges])
    ends = np.concatenate([edges, [len(ids)]])
    for a, b in zip(starts, ends):
        yield trades.slice(int(a), int(b - a)).drop_columns(["batch_id"])


def _read_bronze(con, run_id: str, after_batch: int) -> pa.Table:
    return con.execute(f"""
        SELECT batch_id, {BATCH_COLUMNS} FROM bronze.trade_events
        WHERE run_id = ? AND batch_id > ? ORDER BY batch_id
    """, [run_id, after_batch]).fetch_arrow_table()


def _summary(builder: BarBuilder) -> str:
    total = builder.accepted + builder.dropped
    return (f"lateness {builder.key[1]:g}s: {builder.accepted:,} trades in bars, "
            f"{builder.dropped:,} dropped as too late ({builder.dropped / total:.3%})" if total else "no trades")


def build_bars(con, run_id: str, allowed_lateness_s: float, verbose: bool = True) -> dict:
    """Offline: rebuild silver for a finished bronze run."""
    builder = BarBuilder(run_id, allowed_lateness_s)
    _reset(con, builder.key)
    trades = _read_bronze(con, run_id, -1)
    outputs = [builder.process(b) for b in _batches(trades)]
    outputs.append(builder.process(final=True))
    write_output(con, outputs)
    _record_build(con, builder)
    if verbose:
        print(f"Built silver for run {run_id}, {_summary(builder)}")
    return {"accepted": builder.accepted, "dropped": builder.dropped, "batches": builder.batches}


def follow(con, run_id: str, allowed_lateness_s: float, poll_s: float = 0.25) -> None:
    """Live: consume bronze as the replayer writes it, until the run finishes."""
    builder = BarBuilder(run_id, allowed_lateness_s)
    _reset(con, builder.key)
    last_batch = -1
    last_log = time.monotonic()
    print(f"Following run {run_id} (lateness {allowed_lateness_s:g}s)")

    while True:
        # Read the run's status *before* its rows: if it already said "done",
        # every batch was committed before that, so an empty read means finished.
        status = con.execute("SELECT status FROM bronze.replay_runs WHERE run_id = ?", [run_id]).fetchone()
        trades = _read_bronze(con, run_id, last_batch)

        if trades.num_rows:
            last_batch = int(trades["batch_id"].to_numpy()[-1])
            write_output(con, [builder.process(b) for b in _batches(trades)])
            if time.monotonic() - last_log > 5 and builder.watermark is not None:
                print(f"  batch {last_batch:>5}  watermark {builder.watermark:%H:%M:%S}  "
                      f"{builder.accepted:,} trades in bars, {builder.dropped:,} late so far")
                last_log = time.monotonic()
        elif status and status[0] in ("completed", "interrupted"):
            write_output(con, [builder.process(final=True)])
            _record_build(con, builder)
            print(f"Run {run_id} finished - {_summary(builder)}")
            return
        else:
            time.sleep(poll_s)
