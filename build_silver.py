"""Build silver 1s/1m bars from a bronze replay run.

  python build_silver.py                      # latest completed run, 10s allowed lateness
  python build_silver.py --lateness 30
  python build_silver.py --run-id 20260925T113550-cd6a02
"""

import argparse

from config import get_connection
from src.silver import build_bars


def latest_run(con) -> str:
    row = con.execute(
        "SELECT run_id FROM bronze.replay_runs WHERE status = 'completed' ORDER BY started_at DESC LIMIT 1"
    ).fetchone()
    if not row:
        raise SystemExit("No completed replay runs in bronze - run replay.py first.")
    return row[0]


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--run-id", help="bronze run to build from (default: latest completed)")
    p.add_argument("--lateness", type=float, default=10.0, help="allowed lateness in seconds")
    a = p.parse_args()

    con = get_connection()
    build_bars(con, a.run_id or latest_run(con), a.lateness)


if __name__ == "__main__":
    main()
