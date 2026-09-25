"""Rules engine and alert manager: silver 1s bars -> alerts -> incidents.

Three layers, the same as an observability stack:

  features   per symbol, per second: returns, volume and order-flow stats
             against a rolling baseline (see rules.yml for definitions)
  alerts     a rule *fires* when its condition goes from false to true, and
             *clears* only after staying false for clear_after_s. One alert
             per episode, not one per second the condition holds.
  incidents  alerts on the same symbol group into one incident. It stays open
             while any rule is active on that symbol and resolves after
             resolve_after_s of quiet.

Like silver, the engine keeps its working state (a rolling buffer of bars per
symbol, rule states, open incidents) in memory, and writes only changes to
the warehouse. It is driven offline (evaluate()) or live (follow()).

Two timestamps on every alert and incident, kept apart on purpose:
  fired_at / opened_at   the market second the condition became true
  detected_at            when the pipeline knew, on the replay's simulated clock
The gap between them is detection delay: silver's allowed lateness, plus
batching, plus (live) however far behind the processes are running.
"""

import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

RULES_PATH = Path(__file__).parent.parent / "rules.yml"

DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.alerts (
    run_id              VARCHAR,
    allowed_lateness_s  DOUBLE,
    alert_no            INTEGER,
    incident_no         INTEGER,
    symbol              VARCHAR,
    rule                VARCHAR,
    severity            VARCHAR,
    fired_at            TIMESTAMP,   -- market second the condition became true
    detected_at         TIMESTAMP,   -- when the pipeline knew (sim clock)
    cleared_at          TIMESTAMP,
    features            JSON         -- feature values at the moment it fired
);

CREATE TABLE IF NOT EXISTS ops.incidents (
    run_id              VARCHAR,
    allowed_lateness_s  DOUBLE,
    incident_no         INTEGER,
    symbol              VARCHAR,
    status              VARCHAR,     -- open / resolved
    opened_at           TIMESTAMP,
    detected_at         TIMESTAMP,
    last_active_at      TIMESTAMP,
    resolved_at         TIMESTAMP,
    rules               VARCHAR[],
    alert_count         INTEGER,
    price_at_open       DOUBLE,
    price_low           DOUBLE,
    price_high          DOUBLE,
    price_last          DOUBLE,
    max_abs_ret_60_z    DOUBLE,
    max_vol_60_ratio    DOUBLE,
    max_sell_share_120  DOUBLE,
    min_sell_share_120  DOUBLE,
    agent_status        VARCHAR      -- for phase 4: pending / analysed / awaiting_analyst
);

CREATE TABLE IF NOT EXISTS ops.incident_events (
    run_id              VARCHAR,
    allowed_lateness_s  DOUBLE,
    incident_no         INTEGER,
    event               VARCHAR,     -- opened / alert / resolved
    event_time          TIMESTAMP,
    detected_at         TIMESTAMP,
    rule                VARCHAR
);

CREATE TABLE IF NOT EXISTS ops.rule_runs (
    run_id              VARCHAR,
    allowed_lateness_s  DOUBLE,
    started_at          TIMESTAMP,
    finished_at         TIMESTAMP,
    mode                VARCHAR,     -- offline / live
    seconds_evaluated   INTEGER,
    alerts              INTEGER,
    incidents           INTEGER,
    rules_yml           VARCHAR      -- the exact rules this run used
);
"""


def load_rules(path: Path = RULES_PATH) -> tuple:
    text = path.read_text()
    return yaml.safe_load(text), text


# ---------------------------------------------------------------- features

def compute_features(buf: pd.DataFrame, baseline_s: int, warmup_s: int) -> pd.DataFrame:
    """Features for every second in a dense per-symbol buffer (index = second)."""
    f = pd.DataFrame(index=buf.index)
    close, vol, sell = buf["close"], buf["volume"], buf["sell_volume"]

    def baseline(series: pd.Series, exclude_s: int, how: str) -> pd.Series:
        rolled = series.rolling(baseline_s, min_periods=warmup_s)
        return (rolled.std() if how == "std" else rolled.median()).shift(exclude_s)

    f["close"] = close
    f["ret_60"] = close / close.shift(60) - 1
    f["ret_60_z"] = f["ret_60"] / baseline(f["ret_60"], 60, "std").replace(0, np.nan)
    f["vol_60"] = vol.rolling(60).sum()
    f["vol_60_ratio"] = f["vol_60"] / baseline(f["vol_60"], 60, "median").replace(0, np.nan)
    vol_120 = vol.rolling(120).sum()
    f["sell_share_120"] = sell.rolling(120).sum() / vol_120.replace(0, np.nan)
    f["vol_120_ratio"] = vol_120 / baseline(vol_120, 120, "median").replace(0, np.nan)
    return f


# ---------------------------------------------------------------- state

@dataclass
class RuleState:
    active: bool = False
    last_true: datetime = None
    alert: dict = None


@dataclass
class Incident:
    incident_no: int
    symbol: str
    opened_at: datetime
    detected_at: datetime
    price_at_open: float
    status: str = "open"
    last_active_at: datetime = None
    resolved_at: datetime = None
    rules: list = field(default_factory=list)
    alert_count: int = 0
    price_low: float = None
    price_high: float = None
    price_last: float = None
    max_abs_ret_60_z: float = 0.0
    max_vol_60_ratio: float = 0.0
    max_sell_share_120: float = None
    min_sell_share_120: float = None

    def observe(self, t, row) -> None:
        price = float(row["close"])
        self.price_last = price
        self.price_low = price if self.price_low is None else min(self.price_low, price)
        self.price_high = price if self.price_high is None else max(self.price_high, price)
        for attr, value, fn in (("max_abs_ret_60_z", abs(row["ret_60_z"]), max),
                                ("max_vol_60_ratio", row["vol_60_ratio"], max),
                                ("max_sell_share_120", row["sell_share_120"], max),
                                ("min_sell_share_120", row["sell_share_120"], min)):
            if pd.notna(value):
                current = getattr(self, attr)
                setattr(self, attr, float(value) if current is None else fn(current, float(value)))


def _clean(value):
    return None if value is None or (isinstance(value, float) and np.isnan(value)) else value


class RuleEngine:
    def __init__(self, run_id: str, allowed_lateness_s: float, config: dict):
        self.key = [run_id, float(allowed_lateness_s)]
        self.cfg = config
        self.rules = config["rules"]
        self.baseline_s = int(config["features"]["baseline_window_s"])
        self.warmup_s = int(config["features"]["warmup_s"])
        self.resolve_after = timedelta(seconds=config["incidents"]["resolve_after_s"])
        self.keep = timedelta(seconds=self.baseline_s + 600)

        self.buffers = {}        # symbol -> dense DataFrame of close/volume/sell_volume
        self.known_at = pd.Series(dtype="datetime64[ns]")  # second -> when it was known closed
        self.last_eval = {}      # symbol -> last evaluated second
        self.states = {}         # (rule, symbol) -> RuleState
        self.open_incident = {}  # symbol -> Incident
        self.alerts, self.incidents, self.events = [], [], []
        self.changed_alerts, self.changed_incidents = set(), set()
        self.seconds_evaluated = 0

    def ingest(self, bars: pd.DataFrame, detected_now: datetime = None) -> None:
        """Take newly closed 1s bars (any symbols, bar_start ascending) and evaluate every new second.

        detected_now: live mode passes the current sim clock; offline mode uses
        each second's emitted_at (what the pipeline would have known with no
        processing lag at all)."""
        if bars.empty:
            return
        horizon = bars["bar_start"].max()
        # A second is known closed once any bar at or after it has been emitted
        # (silver closes all symbols up to the same watermark).
        known = bars.groupby("bar_start")["emitted_at"].min()
        self.known_at = pd.concat([self.known_at, known]).groupby(level=0).min()

        rows = []
        for symbol in sorted(set(bars["symbol"]) | set(self.buffers)):
            new = bars.loc[bars["symbol"] == symbol].set_index("bar_start")[["close", "volume", "sell_volume"]]
            buf = pd.concat([self.buffers.get(symbol), new]) if symbol in self.buffers else new
            if buf.empty:
                continue
            dense = buf.reindex(pd.date_range(buf.index[0], horizon, freq="1s"))
            dense["close"] = dense["close"].ffill()
            dense[["volume", "sell_volume"]] = dense[["volume", "sell_volume"]].fillna(0.0)

            feats = compute_features(dense, self.baseline_s, self.warmup_s)
            last = self.last_eval.get(symbol)
            fresh = feats if last is None else feats.loc[feats.index > last]
            if not fresh.empty:
                for rule_name, rule in self.rules.items():
                    fresh = fresh.assign(**{f"_{rule_name}": fresh.eval(rule["condition"]).astype(bool)})
                fresh = fresh.assign(symbol=symbol)
                rows.append(fresh)
                self.last_eval[symbol] = fresh.index[-1]
            self.buffers[symbol] = dense.loc[dense.index > horizon - self.keep]

        if not rows:
            return
        known_at = self.known_at.reindex(pd.date_range(self.known_at.index[0], horizon, freq="1s")).bfill()
        self.known_at = self.known_at.loc[self.known_at.index > horizon - self.keep]
        for t, row in pd.concat(rows).sort_index(kind="stable").iterrows():
            detected = detected_now or known_at[t].to_pydatetime()
            self._step(t.to_pydatetime(), row, detected)
            self.seconds_evaluated += 1

    def _step(self, t: datetime, row: pd.Series, detected: datetime) -> None:
        symbol = row["symbol"]
        inc = self.open_incident.get(symbol)
        any_active = False

        for rule_name, rule in self.rules.items():
            st = self.states.setdefault((rule_name, symbol), RuleState())
            if row[f"_{rule_name}"]:
                st.last_true = t
                if not st.active:
                    st.active = True
                    inc = inc or self._open(symbol, t, row, detected)
                    st.alert = self._fire(rule_name, rule, symbol, t, row, detected, inc)
            elif st.active and t - st.last_true >= timedelta(seconds=rule.get("clear_after_s", 60)):
                st.active = False
                st.alert["cleared_at"] = t
                self.changed_alerts.add(st.alert["alert_no"])
            any_active = any_active or st.active

        if inc is None:
            return
        if any_active:
            inc.last_active_at = t
            inc.observe(t, row)
            self.changed_incidents.add(inc.incident_no)
        elif t - inc.last_active_at >= self.resolve_after:
            inc.status, inc.resolved_at = "resolved", t
            self.events.append([inc.incident_no, "resolved", t, detected, None])
            self.changed_incidents.add(inc.incident_no)
            del self.open_incident[symbol]

    def _open(self, symbol, t, row, detected) -> Incident:
        inc = Incident(len(self.incidents) + 1, symbol, t, detected, float(row["close"]), last_active_at=t)
        self.incidents.append(inc)
        self.open_incident[symbol] = inc
        self.events.append([inc.incident_no, "opened", t, detected, None])
        return inc

    def _fire(self, rule_name, rule, symbol, t, row, detected, inc) -> dict:
        features = {k: _clean(round(float(row[k]), 6)) for k in
                    ("close", "ret_60", "ret_60_z", "vol_60", "vol_60_ratio", "sell_share_120", "vol_120_ratio")}
        alert = {"alert_no": len(self.alerts) + 1, "incident_no": inc.incident_no, "symbol": symbol,
                 "rule": rule_name, "severity": rule.get("severity"), "fired_at": t, "detected_at": detected,
                 "cleared_at": None, "features": json.dumps(features)}
        self.alerts.append(alert)
        inc.alert_count += 1
        if rule_name not in inc.rules:
            inc.rules.append(rule_name)
        self.events.append([inc.incident_no, "alert", t, detected, rule_name])
        self.changed_alerts.add(alert["alert_no"])
        return alert

    # ------------------------------------------------------------ persistence

    def flush(self, con) -> None:
        """Write only what changed since the last flush."""
        key = self.key
        if self.changed_alerts:
            nos = sorted(self.changed_alerts)
            df = pd.DataFrame([self.alerts[n - 1] for n in nos])
            df.insert(0, "allowed_lateness_s", key[1])
            df.insert(0, "run_id", key[0])
            _replace(con, "ops.alerts", "alert_no", nos, key, df)
        if self.changed_incidents:
            nos = sorted(self.changed_incidents)
            df = pd.DataFrame([{
                "run_id": key[0], "allowed_lateness_s": key[1], "incident_no": i.incident_no, "symbol": i.symbol,
                "status": i.status, "opened_at": i.opened_at, "detected_at": i.detected_at,
                "last_active_at": i.last_active_at, "resolved_at": i.resolved_at, "rules": list(i.rules),
                "alert_count": i.alert_count, "price_at_open": i.price_at_open, "price_low": i.price_low,
                "price_high": i.price_high, "price_last": i.price_last, "max_abs_ret_60_z": i.max_abs_ret_60_z,
                "max_vol_60_ratio": i.max_vol_60_ratio, "max_sell_share_120": i.max_sell_share_120,
                "min_sell_share_120": i.min_sell_share_120, "agent_status": "pending",
            } for i in (self.incidents[n - 1] for n in nos)])
            _replace(con, "ops.incidents", "incident_no", nos, key, df)
        if self.events:
            df = pd.DataFrame(self.events, columns=["incident_no", "event", "event_time", "detected_at", "rule"])
            df.insert(0, "allowed_lateness_s", key[1])
            df.insert(0, "run_id", key[0])
            con.register("ev", df)
            con.execute("INSERT INTO ops.incident_events SELECT * FROM ev")
            con.unregister("ev")
        self.changed_alerts.clear()
        self.changed_incidents.clear()
        self.events.clear()


def _replace(con, table: str, id_col: str, ids: list, key: list, df: pd.DataFrame) -> None:
    """Upsert by delete + insert (rows are few; keeps it portable to MotherDuck)."""
    con.execute(f"DELETE FROM {table} WHERE run_id = ? AND allowed_lateness_s = ? AND list_contains(?, {id_col})",
                key + [ids])
    con.register("rows", df)
    con.execute(f"INSERT INTO {table} SELECT * FROM rows")
    con.unregister("rows")


# ---------------------------------------------------------------- drivers

BARS_SQL = """
SELECT symbol, bar_start, close::DOUBLE AS close, volume::DOUBLE AS volume,
       sell_volume::DOUBLE AS sell_volume, emitted_at
FROM silver.bars_1s
WHERE run_id = ? AND allowed_lateness_s = ? AND bar_start > ?
ORDER BY bar_start, symbol
"""


def _reset(con, key: list) -> None:
    con.execute(DDL)
    for table in ("alerts", "incidents", "incident_events", "rule_runs"):
        con.execute(f"DELETE FROM ops.{table} WHERE run_id = ? AND allowed_lateness_s = ?", key)


def _record(con, engine: RuleEngine, started: datetime, mode: str, rules_text: str) -> None:
    con.execute("INSERT INTO ops.rule_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                engine.key + [started, datetime.utcnow(), mode, engine.seconds_evaluated,
                              len(engine.alerts), len(engine.incidents), rules_text])


def evaluate(con, run_id: str, allowed_lateness_s: float) -> RuleEngine:
    """Offline: run the rules over a finished silver build."""
    config, text = load_rules()
    engine = RuleEngine(run_id, allowed_lateness_s, config)
    started = datetime.utcnow()
    _reset(con, engine.key)
    engine.ingest(con.execute(BARS_SQL, engine.key + [datetime(1970, 1, 1)]).df())
    engine.flush(con)
    _record(con, engine, started, "offline", text)
    return engine


def follow(con, run_id: str, allowed_lateness_s: float, poll_s: float = 0.25) -> RuleEngine:
    """Live: evaluate bars as silver publishes them, until silver's build is recorded."""
    config, text = load_rules()
    engine = RuleEngine(run_id, allowed_lateness_s, config)
    started = datetime.utcnow()
    _reset(con, engine.key)
    last_bar = datetime(1970, 1, 1)
    print(f"Following silver for run {run_id} (lateness {allowed_lateness_s:g}s)")

    while True:
        # "Now" is the replayer's published clock, not ours - see bronze.ensure_tables.
        run = con.execute("SELECT sim_clock, speed FROM bronze.replay_runs WHERE run_id = ?", [run_id]).fetchone()
        # Check "silver finished" before reading bars, for the same reason silver
        # checks the replay's status before reading bronze.
        done = con.execute("SELECT count(*) FROM silver.builds WHERE run_id = ? AND allowed_lateness_s = ?",
                           engine.key).fetchone()[0]
        bars = con.execute(BARS_SQL, engine.key + [last_bar]).df()

        if not bars.empty:
            last_bar = bars["bar_start"].max().to_pydatetime()
            sim_now = run[0] if run and run[1] > 0 else None  # backfill runs have no live clock
            before = len(engine.incidents), len(engine.alerts)
            engine.ingest(bars, detected_now=sim_now)
            for inc in engine.incidents[before[0]:]:
                print(f"  INCIDENT {inc.incident_no:>3} opened  {inc.symbol} at {inc.opened_at:%H:%M:%S}")
            for alert in engine.alerts[before[1]:]:
                delay = (alert["detected_at"] - alert["fired_at"]).total_seconds()
                print(f"    alert {alert['rule']:<15} {alert['symbol']} at {alert['fired_at']:%H:%M:%S} "
                      f"(detected {delay:.0f} sim s later)")
            engine.flush(con)
        elif done:
            _record(con, engine, started, "live", text)
            print(f"Rules finished: {engine.seconds_evaluated:,} symbol-seconds, "
                  f"{len(engine.alerts)} alerts, {len(engine.incidents)} incidents")
            return engine
        else:
            time.sleep(poll_s)
