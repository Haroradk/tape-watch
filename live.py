"""Run the whole pipeline live: replayer, silver and rules as three processes.

  python live.py                          # 20:00-22:00 UTC on the crash day at 60x (~2 min)
  python live.py --speed 30 --late-fraction 0.01

Each stage is its own process talking only through the warehouse (MotherDuck),
the way separately deployed services would:

  replay.py        -> bronze.trade_events
  build_silver.py  bronze -> silver.bars_1s / bars_1m   (--follow)
  run_rules.py     silver -> ops.alerts / ops.incidents  (--follow)

Output from all three is interleaved here, prefixed by stage. Ctrl-C stops
all three; the replayer marks its run as interrupted and the followers then
flush what they have and exit.
"""

import argparse
import signal
import subprocess
import sys
import threading
from pathlib import Path

from config import ALLOWED_LATENESS_S, MOTHERDUCK_TOKEN, get_connection
from run_rules import print_incidents
from src.replayer import new_run_id

HERE = Path(__file__).parent


def _pipe(proc: subprocess.Popen, name: str) -> None:
    for line in proc.stdout:
        print(f"[{name:<6}] {line}", end="", flush=True)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--speed", default="60")
    p.add_argument("--start", default="20:00")
    p.add_argument("--end", default="22:00")
    p.add_argument("--late-fraction", default="0")
    p.add_argument("--late-delay", default="10")
    p.add_argument("--lateness", default=str(ALLOWED_LATENESS_S), help="silver's allowed lateness")
    a = p.parse_args()

    if not MOTHERDUCK_TOKEN:
        raise SystemExit("live.py needs MOTHERDUCK_TOKEN: a local DuckDB file allows only one writing process.")

    run_id = new_run_id()
    py = [sys.executable, "-u", "-W", "ignore"]
    stages = {
        "replay": py + ["replay.py", "--run-id", run_id, "--speed", a.speed, "--start", a.start, "--end", a.end,
                        "--late-fraction", a.late_fraction, "--late-delay", a.late_delay],
        "silver": py + ["build_silver.py", "--follow", "--run-id", run_id, "--lateness", a.lateness],
        "rules": py + ["run_rules.py", "--follow", "--run-id", run_id, "--lateness", a.lateness],
    }
    print(f"Live run {run_id}")
    # Own sessions, so a terminal Ctrl-C reaches only this script - it then
    # interrupts just the replayer and lets the followers wind down cleanly.
    procs = {name: subprocess.Popen(cmd, cwd=HERE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                                    start_new_session=True)
             for name, cmd in stages.items()}
    threads = [threading.Thread(target=_pipe, args=(proc, name), daemon=True) for name, proc in procs.items()]
    for t in threads:
        t.start()

    try:
        for proc in procs.values():
            proc.wait()
    except KeyboardInterrupt:
        # The followers finish on their own once they see the run marked interrupted.
        procs["replay"].send_signal(signal.SIGINT)
        for proc in procs.values():
            proc.wait()
    for t in threads:
        t.join()

    codes = {name: proc.returncode for name, proc in procs.items()}
    print(f"Done - exit codes {codes}")
    print_incidents(get_connection(read_only=True), run_id, float(a.lateness))


if __name__ == "__main__":
    main()
