"""Shared configuration for tape-watch."""

import os
from pathlib import Path

import duckdb
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

SYMBOLS = ["BTCUSDT", "ETHUSDT"]

# Binance public market data - free, no account, no API key:
# https://github.com/binance/binance-public-data
# Daily files appear the day after; each .zip has a sibling .CHECKSUM (sha256).
BINANCE_DATA_URL = "https://data.binance.vision/data/spot/daily/aggTrades"
BINANCE_KLINES_URL = "https://data.binance.vision/data/spot/daily/klines"

# 2025-10-10 is the big crypto liquidation day (evening UTC) - a day where the
# rules engine will definitely have something to find.
DEFAULT_DATE = "2025-10-10"

# How long silver waits for late trades before closing a bar - see the
# lateness experiment in the README for where 10s comes from.
ALLOWED_LATENESS_S = 10.0

PROJECT_DIR = Path(__file__).parent
RAW_DIR = PROJECT_DIR / "data" / "raw"
DB_PATH = str(PROJECT_DIR / "data" / "tape.duckdb")

# The warehouse lives on MotherDuck when a token is set, so the replayer,
# silver and rules can run as separate processes (a local DuckDB file only
# allows one writing process). Without a token everything uses the local file.
MOTHERDUCK_TOKEN = os.environ.get("MOTHERDUCK_TOKEN")
MOTHERDUCK_DATABASE = os.environ.get("MOTHERDUCK_DATABASE", "tape")


def get_connection(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """MotherDuck when MOTHERDUCK_TOKEN is set (cloud), else the local file."""
    if MOTHERDUCK_TOKEN:
        con = duckdb.connect("md:", config={"motherduck_token": MOTHERDUCK_TOKEN})
        con.execute(f"CREATE DATABASE IF NOT EXISTS {MOTHERDUCK_DATABASE}")
        con.execute(f"USE {MOTHERDUCK_DATABASE}")
    else:
        Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(DB_PATH, read_only=read_only)

    # Every timestamp in this project is naive-but-UTC; keep duckdb from
    # shifting them through the OS's local timezone.
    con.execute("SET TimeZone = 'UTC'")
    return con
