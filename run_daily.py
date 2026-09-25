"""The daily job: process finished trading days end to end (see src/daily.py).

  python run_daily.py                        # yesterday (UTC)
  python run_daily.py --date 2026-09-20
  python run_daily.py --days 7               # the 7 days up to and including --date (skips done days)
  python run_daily.py --date 2026-09-20 --force   # re-run a day; the new run becomes the official one

Exit code is non-zero if any day failed or its source files weren't published yet.
"""

import argparse
import os
import sys
from datetime import date, datetime, timedelta

from config import MOTHERDUCK_TOKEN
from src.daily import run_day, write_briefings


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--date", help="trading day YYYY-MM-DD (default: yesterday UTC)")
    p.add_argument("--days", type=int, default=1, help="process this many days ending at --date")
    p.add_argument("--force", action="store_true", help="re-run days that already have an official run")
    p.add_argument("--delete-raw", action="store_true", help="delete the day's downloaded trade CSVs afterwards")
    p.add_argument("--no-briefing", action="store_true", help="skip writing the daily briefings")
    a = p.parse_args()

    # Without a token, get_connection() falls back to a local DuckDB file. On a
    # CI runner that file is thrown away with the machine: a "successful" run
    # would silently lose everything.
    if os.environ.get("GITHUB_ACTIONS") and not MOTHERDUCK_TOKEN:
        sys.exit("MOTHERDUCK_TOKEN is not set - add it as a repository secret (Settings -> Secrets -> Actions).")

    last = date.fromisoformat(a.date) if a.date else datetime.utcnow().date() - timedelta(days=1)
    days = [last - timedelta(days=i) for i in reversed(range(a.days))]
    results = {d: run_day(d, force=a.force, delete_raw=a.delete_raw) for d in days}

    if not a.no_briefing:
        written = write_briefings()
        print("Briefings: " + (", ".join(f"{d} {s}" for d, s in written.items()) or "all up to date"))

    print("\nSummary: " + ", ".join(f"{d} {s}" for d, s in results.items()))
    sys.exit(0 if all(s in ("completed", "skipped") for s in results.values()) else 1)


if __name__ == "__main__":
    main()
