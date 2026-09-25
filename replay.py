"""Replay a historical trading day into bronze as a live-looking stream.

Examples:
  python replay.py                                   # 20:30-22:00 UTC on the crash day, 60x
  python replay.py --speed 0                         # backfill as fast as possible
  python replay.py --late-fraction 0.01              # 1% of trades arrive late (30s on average)
  python replay.py --start 00:00 --end 23:59:59 --speed 600
"""

import argparse
from datetime import datetime

from config import DEFAULT_DATE, SYMBOLS
from src.replayer import replay


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--date", default=DEFAULT_DATE, help="trading day, YYYY-MM-DD (UTC)")
    p.add_argument("--symbols", nargs="+", default=SYMBOLS)
    p.add_argument("--start", default="20:30", help="window start, HH:MM[:SS] UTC")
    p.add_argument("--end", default="22:00", help="window end, HH:MM[:SS] UTC")
    p.add_argument("--speed", type=float, default=60.0, help="sim seconds per wall second; 0 = as fast as possible")
    p.add_argument("--tick", type=float, default=0.5, help="seconds between micro-batches")
    p.add_argument("--late-fraction", type=float, default=0.0, help="share of trades that arrive late")
    p.add_argument("--late-delay", type=float, default=30.0, help="mean delay of late trades, in sim seconds")
    a = p.parse_args()

    def at(hms: str) -> datetime:
        fmt = "%Y-%m-%d %H:%M:%S" if hms.count(":") == 2 else "%Y-%m-%d %H:%M"
        return datetime.strptime(f"{a.date} {hms}", fmt)

    replay(a.date, a.symbols, at(a.start), at(a.end), a.speed, a.tick, a.late_fraction, a.late_delay)


if __name__ == "__main__":
    main()
