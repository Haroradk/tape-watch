# tape-watch

A learning project: replay a real day of crypto trades as if it were a live feed, detect unusual
market moves with simple rules, group them into incidents, and (next) wake an LLM agent only when
an incident opens, to explain what happened. Think market surveillance desk, or an observability platform where the
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
| 3 | YAML rules → alerts → incidents, live on MotherDuck as three processes | done |
| 4 | Gemini agent per incident, read-only SQL, daily budget guard | next |
| 5 | Streamlit dashboard | |

## How the stream works

The replayer runs a simulated clock `--speed` times faster than real time. Every `--tick` wall-seconds
it emits, as one micro-batch, every trade whose arrival time has passed on that clock. So at the
default 60x with a 0.5 s tick, each batch carries ~30 simulated seconds of trades, and the default
two hours of market (20:00–22:00 UTC) plays out in two minutes.

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

## Rules and incidents

`rules.yml` defines three surveillance rules over per-second features, computed on a dense 1-second
series per symbol against a rolling 30-minute baseline (10-minute warm-up before anything can fire):

| Rule | Fires when | Severity |
|---|---|---|
| `price_shock` | 60 s return is more than 4 standard deviations from normal | high |
| `volume_burst` | last minute's volume is more than 5x the normal minute | medium |
| `one_sided_flow` | over 120 s, >80% of volume is sell-initiated (or <20%), on above-normal volume | medium |

Conditions are pandas expressions over the features, so thresholds are tuned in YAML, not code. The
exact YAML each evaluation used is stored in `ops.rule_runs`.

Three layers, the same shape as an observability stack:

- **Alerts** (`ops.alerts`) are edge-triggered. A rule *fires* when its condition goes false → true,
  and *clears* only after 60 s of being false. One alert per episode, not one per second.
- **Incidents** (`ops.incidents`). Alerts on a symbol join its open incident, or open a new one. An
  incident resolves after 5 minutes with no rule active. `ops.incident_events` is the append-only
  timeline (opened / alert / resolved) that the agent will consume in phase 4.
- **Two times on everything.** `fired_at` / `opened_at` is the market second it happened;
  `detected_at` is when the pipeline knew, on the replay's clock. The gap is detection delay.

### What the crash evening produces

20:00–22:00 UTC, BTC and ETH: 14,400 symbol-seconds evaluated, **21 alerts, 5 incidents**. That's the
alert manager's job in numbers: 21 alerts would be 21 pages; grouped, it's 5 things to look at, which
fits the agent's free-tier budget (about 20 LLM calls a day).

| # | Symbol | Opened | Lasted | Alerts | Rules (in order) | Low vs open | Max z | Max volume |
|---:|---|---|---:|---:|---|---:|---:|---:|
| 1 | BTC | 20:44:12 | 24 min | 6 | volume_burst, one_sided_flow, volume_burst, then 3x price_shock | −3.5% | 13.8 | 68x |
| 2 | ETH | 20:50:08 | 21 min | 5 | price_shock, volume_burst, 3x price_shock | −3.7% | 13.0 | 47x |
| 3 | ETH | 21:12:35 | 16 min | 4 | volume_burst, price_shock, volume_burst, price_shock | −8.8% | 7.4 | 9x |
| 4 | BTC | 21:13:07 | 13 min | 5 | price_shock, volume_burst, price_shock, price_shock, volume_burst | −9.0% | 11.0 | 13x |
| 5 | ETH | 21:56:52 | open | 1 | price_shock | −2.8% | 5.2 | 4x |

Things worth noticing:

- **Volume led price.** Incident 1 opened on a volume burst 6 minutes before the first price shock.
- **Incidents come in pairs.** BTC and ETH open within seconds to minutes of each other (1/2, 3/4).
  Incidents are per symbol, so "is this market-wide?" is left as the first question for the agent.
- **`one_sided_flow` barely fired** (1 of 21 alerts), even in a crash. Even heavy sell-offs rarely
  keep 80% of volume on one side for two full minutes, so the threshold is likely too strict. Tuning
  it is a rules.yml change.
- **Deterministic.** Offline and three separate live runs all produced the same 21 alerts and
  5 incidents. Only detection times differ, because those depend on how the processes ran.

## Running live: three processes on MotherDuck

```
replay.py --run-id X            -> bronze.trade_events         (writes each micro-batch)
build_silver.py --follow ...    bronze -> silver.bars_1s/1m    (polls bronze for new batches)
run_rules.py --follow ...       silver -> ops.alerts/incidents (polls silver for new bars)
```

`live.py` starts all three for one run and interleaves their output. They talk only through the
warehouse, like separately deployed services, which is why this needs MotherDuck: a local DuckDB file
allows one writing process. Each follower keeps its working state in local memory (silver's open bars,
the rules engine's rolling buffers and rule states) and writes only finished output, because every
warehouse call is a network round trip.

Detection delay, live at 60x: **median 26 simulated seconds, max 87**, against a floor of 11 s
(10 s allowed lateness + the 1 s bar, which is what an offline evaluation shows). The rest is
processing lag: roughly 0.2–1.3 s of wall-clock time for poll → write → poll → write, multiplied by
the 60x speed-up. At 1x the same pipeline would detect in about 12 s. Speeding up a replay magnifies
every second of processing lag.

## Running it

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# .env: MOTHERDUCK_TOKEN=..., MOTHERDUCK_DATABASE=tape   (without a token: local file, no live mode)

caffeinate -i python live.py                  # everything live, 20:00-22:00 UTC at 60x (~2 min)
python live.py --late-fraction 0.01           # ... with late trades

# or stage by stage, offline:
python replay.py --speed 0                    # backfill bronze as fast as possible
python build_silver.py                        # latest run, 10s allowed lateness
python run_rules.py                           # rules over that build, prints the incidents
python scripts/lateness_experiment.py         # the lateness sweep (~5 min on MotherDuck)
```

The first run downloads ~150 MB of zips (verified against Binance's sha256 checksums) and unpacks
them to ~900 MB of CSV in `data/raw/` (gitignored). Each two-hour run adds ~5M rows to bronze.

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
  hence MotherDuck for live mode.
- **A sleeping Mac breaks time.** In one live run, macOS went to sleep for 4 minutes. The replayer's
  clock (`time.monotonic`) pauses during sleep, but the wall clock doesn't. The rules engine was
  working out "now" from the wall clock, so it computed that alerts were detected four hours late.
  The fix: the replayer publishes its simulated clock as a heartbeat (`bronze.replay_runs.sim_clock`),
  and consumers read "now" from the producer instead of rebuilding it. Also: `caffeinate -i`.
- **Sleep out the rest of the tick, not a whole tick.** The replayer first slept a full tick *after*
  each MotherDuck insert, so batches drifted to ~40 simulated seconds instead of 30.

## Layout

```
config.py            symbols, data URL, default day, allowed lateness, get_connection()
live.py              runs replayer + silver + rules as three processes for one run
replay.py            CLI: replay a day into bronze
src/download.py      fetch daily zip + .CHECKSUM, verify sha256, unzip
src/replayer.py      simulated clock, late-event injection, micro-batch inserts, reconciliation
src/bronze.py        bronze.replay_runs and bronze.trade_events DDL
build_silver.py      CLI: build silver bars for a run at a given allowed lateness
src/silver.py        watermark bar builder (local state), offline build + live follow
rules.yml            rule definitions, baseline settings, incident grouping
run_rules.py         CLI: evaluate rules offline, or --follow live
src/rules.py         features, rule engine (edge-triggered alerts), incident manager, ops DDL
scripts/lateness_experiment.py   lateness sweep vs hindsight truth
```
