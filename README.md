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
| 2 | Silver 1s/1m bars (windowing, watermarks, late events) | done |
| 3 | YAML rules → deduped incidents | next |
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

With `--late-fraction 0.01`, 1% of trades arrive late, by a random delay averaging `--late-delay` seconds. They still carry their
true `event_time`, and nothing in the row says they're late. Working out lateness and deciding when a
1-second bar is "closed" is silver's problem (watermarks), which is the point of phase 2.

At the end of each run the replayer reconciles: emitted = landed = distinct `(symbol, agg_trade_id)`,
i.e. nothing lost, nothing duplicated.

## Silver: bars and the watermark

`build_silver.py` consumes a bronze run batch by batch in arrival order, like a live consumer, and
builds `silver.bars_1s` and `silver.bars_1m` (OHLC, VWAP, volume, sell-initiated volume, trade count).
The question for every bar is whether that second is finished or more trades could still arrive. The
builder can't know, so it uses a watermark:

```
watermark = latest event_time seen - allowed_lateness
```

A bar closes, and is written to silver, once its end is behind the watermark. A trade for a bar that's
already closed goes to `silver.late_trades` instead of quietly changing a bar the rules may already
have acted on. `emitted_at` on each bar records when it closed, on the simulated clock.

Each build is keyed by `(run_id, allowed_lateness_s)`, so one bronze run can be rebuilt with several
settings and compared.

### What allowed lateness costs

`scripts/lateness_experiment.py` rebuilds silver at several settings and compares each to "truth":
1-minute bars computed with hindsight from every trade in bronze. A real stream processor never sees
truth; a replay can. Run: 20:50–21:30 UTC, 2.7M trades, 2% arriving late (random delay, mean 10 s):

| Allowed lateness | Trades dropped | 1m bars off by >1% volume | Worst 1m volume error | 1s bar delay (p50) |
|---:|---:|---:|---:|---:|
| 0 s | 1.84% | 74 / 80 | 8.9% | 0.3 s |
| 2 s | 1.50% | 67 / 80 | 8.7% | 2.3 s |
| 5 s | 1.11% | 34 / 80 | 8.3% | 5.3 s |
| **10 s** | **0.67%** | **10 / 80** | **7.9%** | **10.3 s** |
| 20 s | 0.24% | 2 / 80 | 7.6% | 20.3 s |
| 30 s | 0.09% | 2 / 80 | 7.5% | 30.3 s |
| 60 s | 0.00% | 0 / 80 | 0.2% | 60.3 s |

What it shows:

- **Every second of lateness is a second of delay on every bar.** There's no free setting.
- **Counting dropped trades hides the real risk.** From 10 s to 30 s, dropped trades fall 7x but the
  worst bar barely improves. It's one trade: 885 ETH (~$3M) at 21:18:31 that arrived 39 s late,
  7.4% of that minute's volume. Tail impact is decided by the biggest late trade, not the count.
- **Close prices were never wrong.** Late trades are random, and a minute's close is its single last
  trade, so dropping one rarely hits it. Volume and sell share (up to 4 percentage points off) are
  what move, which matters for the `one_sided_flow` rule in phase 3.

Default is 10 s: a rule looking at 60-second moves can live with bars 10 s behind, and 10 of 80
bars being >1% off in volume is acceptable for a demo that has 2% of its feed arriving late. Real feeds
are usually much cleaner.

Correctness check: with lateness set far beyond the longest delay, every 1-minute bar matches truth
exactly (0 / 80 different).

## Running it

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

python replay.py                              # 20:30-22:00 UTC on the crash day, 60x
python replay.py --speed 0                    # backfill as fast as possible
python replay.py --late-fraction 0.01         # inject late events
python replay.py --start 21:00 --end 21:30 --speed 30

python build_silver.py                        # latest run, 10s allowed lateness
python scripts/lateness_experiment.py         # the sweep above (~2 min)
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
- **Money is DECIMAL.** Price and quantity are `DECIMAL(18,8)`, not floats. Multiplying two of them
  overflows in DuckDB (price x quantity needs more digits than 18), so notional is computed after
  widening price to `DECIMAL(38,8)`.
- **Open/close by trade id, not timestamp.** Many trades share a microsecond. Binance assigns
  `agg_trade_id` in exchange order, so it's an exact tiebreak, and it stays right when a late trade
  arrives out of order.
- **The micro-batch interval is a latency floor.** Silver can't close a bar faster than batches
  arrive. At 60x with a 0.5 s tick, each batch is 30 simulated seconds, the same trade-off as a
  Spark Structured Streaming trigger interval. The experiment uses backfill mode (0.5 simulated
  seconds per batch) so lateness is what gets measured.
- **One writer per DuckDB file.** A local DuckDB file can't be written by two processes at once,
  so the replayer and silver builder can't run as separate live processes against it. For now silver
  replays bronze after the fact; running live alongside the replayer comes with phase 3.

## Layout

```
config.py            symbols, data URL, default day, get_connection()
replay.py            CLI entry point
src/download.py      fetch daily zip + .CHECKSUM, verify sha256, unzip
src/replayer.py      simulated clock, late-event injection, micro-batch inserts, reconciliation
src/bronze.py        bronze.replay_runs and bronze.trade_events DDL
build_silver.py      CLI: build silver bars for a run at a given allowed lateness
src/silver.py        watermark bar builder: bars_1s, bars_1m, late_trades, builds
scripts/lateness_experiment.py   lateness sweep vs hindsight truth
```
