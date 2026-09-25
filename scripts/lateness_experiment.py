"""How much does allowed lateness cost, and how much does it buy?

Rebuilds silver for one bronze run at several lateness settings and compares
each against "truth": 1-minute bars computed with hindsight straight from
bronze, where every trade counts no matter when it arrived. A real stream
processor never gets to see truth - we can, because it's a replay.

  python scripts/lateness_experiment.py
  python scripts/lateness_experiment.py --run-id <id> --settings 0 5 30
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from build_silver import latest_run  # noqa: E402
from config import get_connection  # noqa: E402
from src.silver import build_bars  # noqa: E402

TRUTH_SQL = """
CREATE OR REPLACE TEMP TABLE truth_1m AS
SELECT symbol, time_bucket(INTERVAL 1 MINUTE, event_time) AS bar_start,
       arg_min(price, agg_trade_id) AS open, max(price) AS high, min(price) AS low,
       arg_max(price, agg_trade_id) AS close, sum(quantity) AS volume,
       coalesce(sum(quantity) FILTER (WHERE is_buyer_maker), 0) AS sell_volume
FROM bronze.trade_events WHERE run_id = ?
GROUP BY ALL
"""

COMPARE_SQL = """
WITH b AS (SELECT * FROM silver.bars_1m WHERE run_id = ? AND allowed_lateness_s = ?)
SELECT
    count(*) FILTER (WHERE b.open <> t.open OR b.high <> t.high OR b.low <> t.low
                        OR b.close <> t.close OR b.volume <> t.volume)          AS wrong_bars,
    count(*) FILTER (WHERE abs(b.volume - t.volume) / t.volume > 0.01)          AS material_bars,
    count(*)                                                                    AS bars,
    max(abs(b.close - t.close) / t.close) * 10000                              AS worst_close_bps,
    max(abs(b.volume - t.volume) / t.volume) * 100                             AS worst_volume_pct,
    max(abs(b.sell_volume / b.volume - t.sell_volume / t.volume)) * 100        AS worst_sell_share_pp
FROM truth_1m t LEFT JOIN b USING (symbol, bar_start)
"""

LATENCY_SQL = """
SELECT quantile_cont(epoch(emitted_at - (bar_start + INTERVAL 1 SECOND)), 0.5),
       quantile_cont(epoch(emitted_at - (bar_start + INTERVAL 1 SECOND)), 0.95)
FROM silver.bars_1s
WHERE run_id = ? AND allowed_lateness_s = ?
  AND bar_start < (SELECT max(bar_start) - INTERVAL 1 MINUTE FROM silver.bars_1s WHERE run_id = ?)
"""


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--run-id")
    p.add_argument("--settings", type=float, nargs="+", default=[0, 1, 2, 5, 10, 20, 30, 60])
    a = p.parse_args()

    con = get_connection()
    run_id = a.run_id or latest_run(con)
    con.execute(TRUTH_SQL, [run_id])
    print(f"Run {run_id}\n")

    rows = []
    for lateness in a.settings:
        stats = build_bars(con, run_id, lateness, verbose=False)
        wrong, material, bars, close_bps, vol_pct, sell_pp = con.execute(COMPARE_SQL, [run_id, lateness]).fetchone()
        p50, p95 = con.execute(LATENCY_SQL, [run_id, lateness, run_id]).fetchone()
        total = stats["accepted"] + stats["dropped"]
        rows.append((lateness, stats["dropped"] / total * 100, wrong, material, bars, close_bps, vol_pct, sell_pp, p50, p95))
        print(f"  lateness {lateness:>4g}s done")

    print()
    # "not exact" = any difference at all vs truth; "off >1%" = volume wrong by more than 1%.
    print(f"{'lateness':>9} {'dropped':>8} {'1m bars':>10} {'1m bars':>9} {'worst':>8} {'worst':>8} {'worst sell':>11} {'1s bar delay':>14}")
    print(f"{'(s)':>9} {'trades':>8} {'not exact':>10} {'off >1%':>9} {'close':>8} {'volume':>8} {'share':>11} {'p50 / p95 (s)':>14}")
    for lateness, drop_pct, wrong, material, bars, close_bps, vol_pct, sell_pp, p50, p95 in rows:
        print(f"{lateness:>9g} {drop_pct:>7.2f}% {wrong:>4}/{bars:<5} {material:>4}/{bars:<4} {close_bps:>5.1f}bp {vol_pct:>7.1f}% "
              f"{sell_pp:>9.1f}pp {p50:>6.1f} / {p95:<5.1f}")


if __name__ == "__main__":
    main()
