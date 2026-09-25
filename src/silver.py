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
"""

from datetime import datetime, timedelta

import duckdb

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

# Open/close use agg_trade_id rather than event_time: Binance assigns ids in
# exchange order, so it's an exact tiebreak where several trades share a
# microsecond - and it stays correct when a late trade arrives out of order.
CLOSE_1S_SQL = """
INSERT INTO silver.bars_1s
SELECT ?, ?, symbol, time_bucket(INTERVAL 1 SECOND, event_time) AS bar_start,
       arg_min(price, agg_trade_id), max(price), min(price), arg_max(price, agg_trade_id),
       sum(quantity), sum(CAST(price AS DECIMAL(38, 8)) * quantity),
       coalesce(sum(quantity) FILTER (WHERE is_buyer_maker), 0),
       count(*), ?
FROM pending
WHERE time_bucket(INTERVAL 1 SECOND, event_time) + INTERVAL 1 SECOND <= ?
GROUP BY symbol, bar_start
"""

CLOSE_1M_SQL = """
INSERT INTO silver.bars_1m
SELECT run_id, allowed_lateness_s, symbol, time_bucket(INTERVAL 1 MINUTE, bar_start) AS minute,
       arg_min(open, bar_start), max(high), min(low), arg_max(close, bar_start),
       sum(quote_volume) / sum(volume),
       sum(volume), sum(quote_volume), sum(sell_volume), sum(trade_count), count(*), ?
FROM silver.bars_1s
WHERE run_id = ? AND allowed_lateness_s = ?
  AND bar_start >= ? AND bar_start < ?
GROUP BY ALL
"""


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(DDL)


def _floor_minute(ts: datetime) -> datetime:
    return ts.replace(second=0, microsecond=0)


def build_bars(con: duckdb.DuckDBPyConnection, run_id: str, allowed_lateness_s: float, verbose: bool = True) -> dict:
    ensure_tables(con)
    key = [run_id, allowed_lateness_s]
    for table in ("bars_1s", "bars_1m", "late_trades", "builds"):
        con.execute(f"DELETE FROM silver.{table} WHERE run_id = ? AND allowed_lateness_s = ?", key)

    # The run's trades in arrival order, pulled once. Per-batch queries
    # against bronze would re-scan it thousands of times.
    con.execute("""
        CREATE OR REPLACE TEMP TABLE stream AS
        SELECT batch_id, symbol, agg_trade_id, price, quantity, is_buyer_maker, event_time, arrival_time
        FROM bronze.trade_events WHERE run_id = ? ORDER BY batch_id
    """, [run_id])
    batches = con.execute(
        "SELECT batch_id, max(arrival_time), max(event_time) FROM stream GROUP BY 1 ORDER BY 1"
    ).fetchall()
    con.execute("CREATE OR REPLACE TEMP TABLE pending AS SELECT * EXCLUDE (batch_id) FROM stream LIMIT 0")

    lateness = timedelta(seconds=allowed_lateness_s)
    max_event = None
    watermark = None
    next_minute = None  # first minute whose 1m bar hasn't been emitted yet

    for i, (batch_id, arrived, batch_max_event) in enumerate(batches):
        # 1. Anything for a bar the previous watermark already closed is too late.
        if watermark is not None:
            con.execute("""
                INSERT INTO silver.late_trades
                SELECT ?, ?, symbol, agg_trade_id, quantity, event_time, arrival_time, ?
                FROM stream WHERE batch_id = ?
                  AND time_bucket(INTERVAL 1 SECOND, event_time) + INTERVAL 1 SECOND <= ?
            """, key + [watermark, batch_id, watermark])
        con.execute("""
            INSERT INTO pending SELECT * EXCLUDE (batch_id) FROM stream
            WHERE batch_id = ? AND (? IS NULL OR time_bucket(INTERVAL 1 SECOND, event_time) + INTERVAL 1 SECOND > ?)
        """, [batch_id, watermark, watermark])

        # 2. Advance the watermark. It only moves forward: a late trade carries
        #    an old event_time, so it can never pull the watermark back.
        max_event = batch_max_event if max_event is None else max(max_event, batch_max_event)
        watermark = max_event - lateness
        if i == len(batches) - 1:
            watermark = datetime.max  # end of stream: flush everything
        if next_minute is None:
            next_minute = _floor_minute(con.execute("SELECT min(event_time) FROM pending").fetchone()[0])

        # 3. Close every 1s bar the watermark has passed and drop its trades from pending.
        con.execute(CLOSE_1S_SQL, key + [arrived, watermark])
        con.execute(
            "DELETE FROM pending WHERE time_bucket(INTERVAL 1 SECOND, event_time) + INTERVAL 1 SECOND <= ?",
            [watermark],
        )

        # 4. A minute is closed once all its seconds are - roll those 1s bars up.
        closed_upto = _floor_minute(min(watermark, max_event + timedelta(minutes=1)))
        if closed_upto > next_minute:
            con.execute(CLOSE_1M_SQL, [arrived] + key + [next_minute, closed_upto])
            next_minute = closed_upto

        if verbose and i % 500 == 0:
            print(f"  batch {batch_id:>5}  watermark {watermark:%H:%M:%S}")

    accepted, dropped = con.execute("""
        SELECT (SELECT coalesce(sum(trade_count), 0) FROM silver.bars_1s WHERE run_id = ? AND allowed_lateness_s = ?),
               (SELECT count(*) FROM silver.late_trades WHERE run_id = ? AND allowed_lateness_s = ?)
    """, key + key).fetchone()
    con.execute("INSERT INTO silver.builds VALUES (?, ?, ?, ?, ?, ?)",
                key + [datetime.utcnow(), len(batches), accepted, dropped])
    con.execute("DROP TABLE stream; DROP TABLE pending")

    total = accepted + dropped
    if verbose:
        print(f"Built silver for run {run_id}, lateness {allowed_lateness_s:g}s: "
              f"{accepted:,} trades in bars, {dropped:,} dropped as too late "
              f"({dropped / total:.3%})" if total else "no trades")
    return {"accepted": accepted, "dropped": dropped, "batches": len(batches)}
