# tape-watch

A learning project: replay a real day of crypto trades as if it were a live feed, detect unusual
market moves with simple rules, and (later) wake an LLM agent only when a rule opens an incident,
to explain what happened. Think market surveillance desk, or an observability platform where the
metrics are trades.

The agent explains moves; it never suggests trades.

## Data

[Binance public market data](https://github.com/binance/binance-public-data): every spot trade,
published as daily CSV files. No account, no API key. We use `aggTrades` (trades at the same price
and the same moment merged into one row) for `BTCUSDT` and `ETHUSDT`.

The default day is **2025-10-10**, the big liquidation evening: BTC traded 115k → 102k inside the
21:00 UTC hour, on ~2.2M trades vs ~75k in a normal hour.

## Status

| Phase | What | State |
|---|---|---|
| 1 | Download + checksum, replayer, bronze | done |
| 2 | Silver 1s/1m bars (windowing, late events) | next |
| 3 | YAML rules → deduped incidents | |
| 4 | Gemini agent per incident, read-only SQL, daily budget guard | |
| 5 | Streamlit dashboard | |

## How the stream works

The replayer runs a simulated clock `--speed` times faster than real time. Every `--tick` wall-seconds
it emits, as one micro-batch, every trade whose arrival time has passed on that clock. So at the
default 60x with a 0.5 s tick, each batch carries ~30 simulated seconds of trades, and 90 minutes of
market plays out in 90 seconds.

Bronze keeps two clocks per trade in `bronze.trade_events`:

| Column | Meaning |
|---|---|
| `event_time` | when the trade happened on the exchange |
| `arrival_time` | when it reached us, on the simulated clock |

With `--late-fraction 0.01`, 1% of trades arrive `--late-delay` seconds late. They still carry their
true `event_time`, and nothing in the row says they're late. Working out lateness and deciding when a
1-second bar is "closed" is silver's problem (watermarks), which is the point of phase 2.

At the end of each run the replayer reconciles: emitted = landed = distinct `(symbol, agg_trade_id)`,
i.e. nothing lost, nothing duplicated.

## Running it

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

python replay.py                              # 20:30-22:00 UTC on the crash day, 60x
python replay.py --speed 0                    # backfill as fast as possible
python replay.py --late-fraction 0.01         # inject late events
python replay.py --start 21:00 --end 21:30 --speed 30
```

The first run downloads ~150 MB of zips (verified against Binance's sha256 checksums) and unpacks
them to ~900 MB of CSV in `data/raw/` (gitignored). Everything lands in the local DuckDB file
`data/tape.duckdb`.

Poke at it:

```bash
python -c "from config import get_connection; print(get_connection(read_only=True).sql('from bronze.replay_runs'))"
```

## Gotchas found so far

- **Timestamp units.** Binance spot timestamps switched from milliseconds to microseconds on
  2025-01-01. The replayer detects the unit per row by magnitude, so mixed-year replays work.
- **No CSV header.** The daily files are headerless; column order comes from Binance's docs.
- **Money is DECIMAL.** Price and quantity are `DECIMAL(18,8)`, not floats.

## Layout

```
config.py            symbols, data URL, default day, get_connection()
replay.py            CLI entry point
src/download.py      fetch daily zip + .CHECKSUM, verify sha256, unzip
src/replayer.py      simulated clock, late-event injection, micro-batch inserts, reconciliation
src/bronze.py        bronze.replay_runs and bronze.trade_events DDL
```
