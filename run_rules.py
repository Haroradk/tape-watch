"""Run the surveillance rules (rules.yml) over silver bars and open incidents.

  python run_rules.py                           # latest completed run, default lateness
  python run_rules.py --run-id <id> --follow    # live: evaluate as silver publishes bars
"""

import argparse

from build_silver import latest_run
from config import ALLOWED_LATENESS_S, get_connection
from src import rules


def print_incidents(con, run_id: str, lateness: float) -> None:
    print(con.sql(f"""
        SELECT incident_no AS inc, symbol, status, strftime(opened_at, '%H:%M:%S') AS opened,
               round(epoch(detected_at - opened_at)) AS detect_s,
               round(epoch(coalesce(resolved_at, last_active_at) - opened_at) / 60, 1) AS minutes,
               array_to_string(rules, ', ') AS rules, alert_count AS alerts,
               round((price_low / price_at_open - 1) * 100, 2) AS low_pct,
               round((price_high / price_at_open - 1) * 100, 2) AS high_pct,
               round(max_abs_ret_60_z, 1) AS max_z, round(max_vol_60_ratio, 1) AS max_vol_x
        FROM ops.incidents WHERE run_id = '{run_id}' AND allowed_lateness_s = {lateness}
        ORDER BY incident_no
    """))


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--run-id")
    p.add_argument("--lateness", type=float, default=ALLOWED_LATENESS_S, help="which silver build to read")
    p.add_argument("--follow", action="store_true")
    a = p.parse_args()

    con = get_connection()
    if a.follow:
        if not a.run_id:
            raise SystemExit("--follow needs --run-id")
        rules.follow(con, a.run_id, a.lateness)
    else:
        run_id = a.run_id or latest_run(con)
        engine = rules.evaluate(con, run_id, a.lateness)
        print(f"Run {run_id}: {engine.seconds_evaluated:,} symbol-seconds, "
              f"{len(engine.alerts)} alerts, {len(engine.incidents)} incidents")
        print_incidents(con, run_id, a.lateness)


if __name__ == "__main__":
    main()
