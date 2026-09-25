"""Bronze: every trade exactly as the stream delivered it, append-only.

Two clocks live side by side in bronze.trade_events, and keeping them apart is
the whole point of this layer:

  event_time    when the trade happened on the exchange (from the source file)
  arrival_time  when the trade reached us, on the replay's simulated clock

Normally arrival_time is just after event_time. When the replayer injects late
events, some arrive long after trades that happened later than they did - the
situation silver's windowing has to cope with. ingested_at is the real
wall-clock time of the insert, for debugging the replayer itself.
"""

import duckdb

DDL = """
CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.replay_runs (
    run_id          VARCHAR PRIMARY KEY,
    trade_date      DATE,
    symbols         VARCHAR[],
    window_start    TIMESTAMP,
    window_end      TIMESTAMP,
    speed           DOUBLE,      -- 0 = as fast as possible (backfill)
    late_fraction   DOUBLE,
    late_delay_s    DOUBLE,
    started_at      TIMESTAMP,
    finished_at     TIMESTAMP,
    status          VARCHAR,     -- running / completed / interrupted
    events_emitted  BIGINT
);

CREATE TABLE IF NOT EXISTS bronze.trade_events (
    run_id          VARCHAR,
    batch_id        INTEGER,
    symbol          VARCHAR,
    agg_trade_id    BIGINT,
    price           DECIMAL(18, 8),
    quantity        DECIMAL(18, 8),
    first_trade_id  BIGINT,
    last_trade_id   BIGINT,
    is_buyer_maker  BOOLEAN,     -- true = the seller crossed the spread (sell-initiated)
    event_time      TIMESTAMP,
    arrival_time    TIMESTAMP,
    ingested_at     TIMESTAMP
);
"""


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(DDL)
    # The replayer's heartbeat: its simulated clock as of the last batch it
    # wrote. Consumers read "now" from here instead of deriving it from the
    # wall clock - a Mac that sleeps mid-run pauses the replayer's clock but
    # not the wall clock, and the two drift apart by hours.
    con.execute("ALTER TABLE bronze.replay_runs ADD COLUMN IF NOT EXISTS sim_clock TIMESTAMP")
