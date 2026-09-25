"""Reference data: what "normal" looks like, refreshed once a day.

Comparing the last minute to the last 30 minutes makes every quiet hour look
exciting: on a sleepy Saturday a 0.09% move is "4 sigma". Desks compare
against a slower reference instead - average daily volume and the intraday
volume curve ("3x what normally trades at this hour"). This module builds
that reference:

  reference.klines_1m   Binance's official 1-minute candles (tiny daily files)
  reference.profile     per symbol and hour of day, over the previous
                        LOOKBACK_DAYS: median 1-minute volume and the typical
                        size (std) of a 1-minute return

The profile for day D uses days D-7 .. D-1 only - never D itself. A reference
that has seen the day it's scoring leaks the answer, which would quietly
flatter any backtest built on these signals later.

The klines are also an independent check on our own pipeline: silver's
1-minute bars are computed from raw trades, Binance's candles by Binance.
They should agree (see reconcile()).
"""

from datetime import date as Date
from datetime import datetime, timedelta

import duckdb
import pandas as pd

from src.download import download_klines, klines_path

LOOKBACK_DAYS = 7

DDL = """
CREATE SCHEMA IF NOT EXISTS reference;

CREATE TABLE IF NOT EXISTS reference.klines_1m (
    symbol              VARCHAR,
    open_time           TIMESTAMP,
    open                DECIMAL(18, 8),
    high                DECIMAL(18, 8),
    low                 DECIMAL(18, 8),
    close               DECIMAL(18, 8),
    volume              DECIMAL(24, 8),
    quote_volume        DECIMAL(28, 8),
    trade_count         BIGINT,          -- raw trades, not aggTrades: not comparable to silver
    taker_buy_volume    DECIMAL(24, 8)   -- buy-initiated; sell-initiated = volume - this
);

CREATE TABLE IF NOT EXISTS reference.profile (
    as_of_date          DATE,            -- the day this profile is used to score
    symbol              VARCHAR,
    hour                INTEGER,         -- hour of day, UTC
    days_used           INTEGER,
    minutes_used        INTEGER,
    vol_1m_median       DOUBLE,          -- typical 1-minute volume at this hour
    ret_1m_std          DOUBLE,          -- typical size of a 1-minute return at this hour
    built_at            TIMESTAMP
);
"""

KLINES_SQL = """
SELECT '{symbol}' AS symbol,
       make_timestamp(CASE WHEN open_time > 100000000000000 THEN open_time ELSE open_time * 1000 END),
       open, high, low, close, volume, quote_volume, trade_count, taker_buy_volume
FROM read_csv('{path}', header = false, columns = {{
    'open_time': 'BIGINT', 'open': 'DECIMAL(18,8)', 'high': 'DECIMAL(18,8)', 'low': 'DECIMAL(18,8)',
    'close': 'DECIMAL(18,8)', 'volume': 'DECIMAL(24,8)', 'close_time': 'BIGINT',
    'quote_volume': 'DECIMAL(28,8)', 'trade_count': 'BIGINT', 'taker_buy_volume': 'DECIMAL(24,8)',
    'taker_buy_quote_volume': 'DECIMAL(28,8)', 'ignore': 'VARCHAR'
}})
"""

PROFILE_SQL = """
WITH k AS (
    SELECT symbol, open_time, volume::DOUBLE AS volume,
           close::DOUBLE / lag(close::DOUBLE) OVER (PARTITION BY symbol ORDER BY open_time) - 1 AS ret_1m
    FROM reference.klines_1m
    WHERE symbol = ANY(?) AND open_time >= ? AND open_time < ?
)
SELECT ?::DATE AS as_of_date, symbol, hour(open_time)::INTEGER AS hour,
       count(DISTINCT open_time::DATE)::INTEGER AS days_used, count(*)::INTEGER AS minutes_used,
       median(volume) AS vol_1m_median, stddev_samp(ret_1m) AS ret_1m_std, ?::TIMESTAMP AS built_at
FROM k
GROUP BY ALL
ORDER BY symbol, hour
"""


def ensure_tables(con) -> None:
    con.execute(DDL)


def load_klines(con, symbols: list, day: Date) -> None:
    """Download one day of klines per symbol and (re)load them. Idempotent."""
    local = duckdb.connect()
    for symbol in symbols:
        download_klines(symbol, day.isoformat())
        rows = local.execute(KLINES_SQL.format(symbol=symbol, path=klines_path(symbol, day.isoformat()))).fetch_arrow_table()
        start = datetime.combine(day, datetime.min.time())
        con.execute("DELETE FROM reference.klines_1m WHERE symbol = ? AND open_time >= ? AND open_time < ?",
                    [symbol, start, start + timedelta(days=1)])
        con.register("k", rows)
        con.execute("INSERT INTO reference.klines_1m SELECT * FROM k")
        con.unregister("k")


def build_profile(con, symbols: list, as_of: Date) -> pd.DataFrame:
    """Build (or rebuild) the profile used to score day `as_of`, from the LOOKBACK_DAYS before it."""
    ensure_tables(con)
    first = as_of - timedelta(days=LOOKBACK_DAYS)
    have = {(s, d) for s, d in con.execute(
        "SELECT DISTINCT symbol, open_time::DATE FROM reference.klines_1m WHERE open_time >= ? AND open_time < ?",
        [first, as_of]).fetchall()}
    for i in range(LOOKBACK_DAYS):
        day = first + timedelta(days=i)
        missing = [s for s in symbols if (s, day) not in have]
        if missing:
            load_klines(con, missing, day)

    profile = con.execute(PROFILE_SQL, [symbols, first, as_of, as_of, datetime.utcnow()]).df()
    con.execute("DELETE FROM reference.profile WHERE as_of_date = ? AND symbol = ANY(?)", [as_of, symbols])
    con.register("p", profile)
    con.execute("INSERT INTO reference.profile SELECT * FROM p")
    con.unregister("p")
    return profile


def get_profile(con, symbols: list, as_of: Date) -> pd.DataFrame:
    """The profile for scoring `as_of`, building it the first time it's asked for."""
    ensure_tables(con)
    profile = con.execute("SELECT * FROM reference.profile WHERE as_of_date = ? AND symbol = ANY(?)",
                          [as_of, symbols]).df()
    if profile["symbol"].nunique() < len(symbols) or (profile["days_used"] < LOOKBACK_DAYS).any():
        profile = build_profile(con, symbols, as_of)
    return profile


def reconcile(con, run_id: str, allowed_lateness_s: float) -> pd.DataFrame:
    """Our 1-minute bars (from raw trades) vs Binance's own candles, per symbol."""
    trade_date = con.execute("SELECT trade_date FROM bronze.replay_runs WHERE run_id = ?", [run_id]).fetchone()[0]
    load_klines(con, sorted(con.execute("SELECT DISTINCT symbol FROM silver.bars_1m WHERE run_id = ?",
                                        [run_id]).df()["symbol"]), trade_date)
    return con.execute("""
        SELECT b.symbol, count(*) AS minutes,
               count(*) FILTER (WHERE b.close = k.close AND b.open = k.open
                                  AND b.high = k.high AND b.low = k.low)        AS ohlc_exact,
               count(*) FILTER (WHERE b.volume = k.volume)                      AS volume_exact,
               count(*) FILTER (WHERE b.volume - b.sell_volume = k.taker_buy_volume) AS buy_volume_exact
        FROM silver.bars_1m b JOIN reference.klines_1m k ON k.symbol = b.symbol AND k.open_time = b.bar_start
        WHERE b.run_id = ? AND b.allowed_lateness_s = ?
        GROUP BY 1 ORDER BY 1
    """, [run_id, allowed_lateness_s]).df()
