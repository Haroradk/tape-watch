# tape-watch

A learning project: replay real days of crypto trades as if they were a live feed, detect unusual
market moves with simple rules, group them into incidents, and have an LLM write a daily briefing
explaining what happened. Think market surveillance desk, or an observability platform where the
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
| 3b | Daily reference profile, rules recalibrated on 5 days and checked on 6 unseen days | done |
| 4 | Daily batch job (GitHub Actions), bronze retention | done |
| 5 | Daily briefing: SQL evidence per incident + one Gemini call per day | done |
| 6 | Streamlit dashboard, updated daily | next |
| 7 | Signal research: do these patterns predict anything? Backtests on history only | |

This is a learning project about the consumption side of market data: detection, explanation, and
later signal research. It never places or recommends trades.

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

`rules.yml` defines three surveillance rules over per-second features. Conditions are pandas
expressions, so thresholds are tuned in YAML, not code, and the exact YAML each evaluation used is
stored in `ops.rule_runs`.

| Rule | Fires when | Role |
|---|---|---|
| `price_shock` | the 60 s move is over 10x the typical 1-minute move *at this hour of day* | **opens incidents** |
| `volume_burst` | the last minute's volume is over 20x the typical minute at this hour | context |
| `one_sided_flow` | over 120 s, >80% of volume is one-sided, on 10x typical volume | context |

"Typical at this hour" comes from the **daily reference profile** (`src/reference.py`): per symbol and
hour of day, the median 1-minute volume and the typical size of a 1-minute return over the previous
7 days, built from Binance's official 1-minute candles (`reference.klines_1m`). It's rebuilt once a
day and never uses the day being scored, because a reference that has seen the day would leak the
answer into any later backtest. It's the desk version of "percent of ADV at this point in the
intraday volume curve". Normal BTC volume at 16:00 UTC is ~4x normal volume at 22:00.

Three layers, the same shape as an observability stack:

- **Alerts** (`ops.alerts`) are edge-triggered: a rule *fires* when its condition goes false → true
  and *clears* after 60 s of being false. Every alert is kept, including context alerts that never
  joined an incident. The signal-research phase needs the alerts that led nowhere too.
- **Incidents** (`ops.incidents`). Only the symptom, price moving, opens an incident. Context alerts
  attach to the symbol's open incident, or to one that opens within 5 minutes after them, so "volume
  moved before price" lands in the evidence. An incident resolves after 15 minutes without a price
  shock. `ops.incident_events` is the append-only timeline.
- **Two times on everything.** `fired_at` / `opened_at` is the market second; `detected_at` is when
  the pipeline knew, on the replay's clock. `price_at_open` is the price *before* the move (60 s
  before the shock fired), otherwise the move that opened the incident is left out of its own size.

### How the thresholds were chosen

The first version compared everything to the **previous 30 minutes**. On a full ordinary day that
gave 108–132 incidents, *more* than the crash day's 104, and the quiet Saturday was the noisiest.
Relative to a quiet half hour, a 0.09% move is "4 sigma". Only 0–2 of those incidents per ordinary day
moved the price 1% or more. Tightening that version still left 59–81 incidents a day.

The second version compares to the daily reference, and calibration showed the rules behave very
differently:

- `price_shock` separates days cleanly.
- `volume_burst` still fires 6–7 times on a normal weekday even at 30x (crypto volume is spiky:
  one large order does it).
- `one_sided_flow` never once marked a move of 1% or more, at any threshold.

So the volume and flow rules became context, not incident openers.

Calibrated on 5 days, then checked on 6 days the thresholds had never seen:

| Day | Set | Alerts | Incidents | Incidents (largest move from pre-shock price) |
|---|---|---:|---:|---|
| Thu 2025-09-18 | unseen | 10 | 1 | BTC 23:00 0.3% |
| Wed 2025-09-24 | calibration | 20 | 3 | ETH 04:11 0.9%, BTC 04:11 0.5%, ETH 21:58 0.6% |
| Sat 2025-09-27 | unseen | 3 | 0 | |
| Wed 2025-10-01 | calibration | 47 | 2 | ETH 08:36 3.2%, BTC 08:46 0.9% |
| Thu 2025-10-02 | unseen | 31 | 0 | |
| Sat 2025-10-04 | calibration | 6 | 0 | |
| Mon 2025-10-06 | unseen | 17 | 0 | (BTC all-time high: a steady climb, no shocks) |
| Tue 2025-10-07 | calibration | 20 | 2 | ETH 15:07 0.9%, ETH 23:50 0.6% |
| Wed 2025-10-08 | unseen | 17 | 0 | |
| **Fri 2025-10-10** | calibration | 112 | 5 | ETH 15:30 2.1%, ETH 19:28 1.5%, **ETH 20:50 −14.4% and BTC 20:50 −12.5% (~3 h each)**, ETH 23:47 2.3% |
| Sat 2025-10-11 | unseen | 32 | 3 | aftershocks: ETH 00:03 0.8%, ETH 02:06 0.9%, BTC 07:00 1.2% |

Ordinary days: 0–3 incidents, and 4 of the 5 unseen ordinary days had none. The crash is now one
incident per coin instead of a dozen fragments. The day after the crash still produced incidents even
though its 7-day reference includes the crash, which makes "normal" wider, so aftershocks had to be
genuinely large.

Known limitation: the reference steps at each hour boundary. Checked on the unseen days, it rarely
matters (a shock at 07:00:32 scores 10.5 against the 07:00 hour and 10.2 against 06:00), but
interpolating between adjacent hours would remove it.

**Data-quality check for free:** our 1-minute bars (built from raw trades) match Binance's own candles
exactly (open, high, low, close, volume and buy volume) for every minute of the day. The first
comparison found one mismatch per coin, at 23:59: the day had been replayed with `--end 23:59:59`,
and since the window end is exclusive, the last second's trades were dropped. Full days now use
`--end 24:00`.

## Production: the daily job

Binance publishes each day's files around 01:30–03:00 UTC the following night, so the production
pipeline is a **batch job**, not a stream. `run_daily.py` (GitHub Actions, `.github/workflows/daily.yml`,
06:23 UTC with a 09:23 retry) processes yesterday end to end with the same code the live demo uses:

1. replay the full day as a fast backfill into bronze (no sleeping, 10 simulated seconds per micro-batch)
2. build silver bars
3. rebuild the reference profile from the previous 7 days and evaluate the rules
4. reconcile: every 1-minute bar against Binance's own candle
5. retention: delete raw trades (bronze) for days more than 7 days old. Silver bars, alerts and
   incidents are small and kept forever, and raw trades can always be downloaded again.

`ops.daily_runs` is the job's audit log: status, trade/alert/incident counts, how many minutes matched
Binance, rows purged, the git commit the job ran, and the error if it failed. It's also the pointer to
the *official* run per day (the latest completed one). Re-running a day with `--force` creates a new
run and moves the pointer; nothing is overwritten in place.

Safeguards:
- **Idempotent.** Days with an official run are skipped, which is why the 09:23 retry is harmless.
- **Loud failure.** A day whose files aren't published yet is recorded as `source_missing`, and the
  job exits non-zero. A failed retry run opens a GitHub issue. The first scheduled attempt doesn't,
  because Binance is sometimes just late.
- **No silent local fallback in CI.** Without `MOTHERDUCK_TOKEN` the code would write to a local
  DuckDB file, which on a CI runner disappears with the machine, so the job refuses to start.

A full ordinary day takes about 1.5 minutes: 1–3M trades, reconciled against 2,880 Binance candles.
The first week (backfilled 2026-09-18 → 09-24) had 3, 1, 1, **7**, 0, 1, 0 incidents per day
(`ops.daily_runs`). The 7 were on 2026-09-21, a genuinely volatile day (BTC's high-low range was 8.1%,
one incident a +2.3% move at 38x the normal size for that hour). Every minute of every day matched
Binance's candles exactly.

```bash
python run_daily.py                          # yesterday
python run_daily.py --date 2026-09-24 --days 7   # backfill a week (skips done days)
python run_daily.py --date 2026-09-24 --force    # re-run a day
```

Setup: add `MOTHERDUCK_TOKEN` as a repository secret (Settings → Secrets and variables → Actions).
The Actions tab has a "Run workflow" button with date / days / force inputs.

## The daily briefing

After the day is processed, `src/briefing.py` writes the briefing:

- **Evidence, by fixed SQL, per incident:**
  - the move relative to the price just before it, and where the price was 30 and 60 minutes later
  - the size versus normal for that hour, for both price and volume
  - the share of sell-initiated volume
  - how concentrated the trades were (largest trade and top-10 trades as a share of volume)
  - what the other coin did, and whether its incidents overlapped
  - the alert timeline, where negative seconds mean a context alert fired before the price shock
- **One LLM call per day** with structured output: a headline, a summary, and per incident a title,
  a pattern from a fixed list (`liquidation_cascade`, `broad_buying`, ...) and a short narrative. The
  prompt allows only numbers from the evidence, no invented causes, and no trade suggestions or
  predictions.
- **Facts come from SQL, interpretation from the model.** Whether an incident is market-wide is
  decided by the overlap in the evidence, not by the model. In the first test it noted two incidents
  overlapped and still labelled both `single_asset`.
- **The answer is untrusted.** The schema guarantees its shape; `validate()` checks its content
  against the evidence (one note per incident, no invented incident numbers) before it's stored.
- **Budget.** Every call is logged in `ops.llm_calls`, with at most 3 per day. Quiet days get a
  written summary without any call. When the budget is spent or the models are unavailable, a day
  is stored as `awaiting_analyst` with its evidence, and later runs catch it up, newest first.
- **Models.** `gemini-3.5-flash-lite`, falling back to `gemini-3.5-flash`. Neither is used by the
  weather projects, and free-tier limits are per model. In the first test, 3.5-flash answered 503
  "high demand" (and without an HTTP timeout, the job hung), while the lite model answered in 2 s.
  Writing up evidence it's handed doesn't need the bigger model.

Output: `gold.daily_briefings` (one per day, with the exact evidence the model saw) and
`gold.incident_briefs` (one per incident).

## Demo: running live as three processes on MotherDuck

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

Detection delay, live at 60x (measured with the first version of the rules): **median 26 simulated seconds, max 87**, against a floor of 11 s
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
- **Positional inserts break when a table grows.** Retention added a `bronze_purged_at` column, and
  the replayer's `INSERT ... VALUES (?, ?, ...)` broke on every new run. The first 7-day backfill
  failed on all 6 days, correctly recorded as `failed` with the error in `ops.daily_runs`. All inserts
  now name their columns (`BY NAME` or a column list), and each table's columns are defined in one place.
- **Sleep out the rest of the tick, not a whole tick.** The replayer first slept a full tick *after*
  each MotherDuck insert, so batches drifted to ~40 simulated seconds instead of 30.

## Layout

```
config.py            symbols, data URL, default day, allowed lateness, get_connection()
run_daily.py         the daily job (production): yesterday, or a range of days
src/daily.py         daily job steps, ops.daily_runs audit log, bronze retention
src/briefing.py      evidence SQL, prompt + schema, validation, budget guard, catch-up
src/llm.py           the one Gemini call site (timeout, retries, structured output)
.github/workflows/daily.yml   runs the daily job on GitHub Actions
live.py              runs replayer + silver + rules as three processes for one run
replay.py            CLI: replay a day into bronze
src/download.py      fetch daily aggTrades / 1m klines zip + .CHECKSUM, verify sha256, unzip
src/replayer.py      simulated clock, late-event injection, micro-batch inserts, reconciliation
src/bronze.py        bronze.replay_runs and bronze.trade_events DDL
build_silver.py      CLI: build silver bars for a run at a given allowed lateness
src/silver.py        watermark bar builder (local state), offline build + live follow
rules.yml            rule definitions, baseline settings, incident grouping
run_rules.py         CLI: evaluate rules offline, or --follow live
src/rules.py         features, rule engine (edge-triggered alerts), incident manager, ops DDL
src/reference.py     daily reference profile from Binance 1m klines; bars-vs-klines reconciliation
scripts/lateness_experiment.py   lateness sweep vs hindsight truth
```
