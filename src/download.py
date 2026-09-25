"""Download Binance daily files and verify them against their checksums.

Two kinds:
  aggTrades   every trade - the "exchange archive" the replayer reads from.
              Not a medallion layer: bronze is only what the stream delivered.
  klines 1m   Binance's own 1-minute candles - small reference data, used to
              build the rules' daily reference profile (src/reference.py).
"""

import hashlib
import zipfile
from pathlib import Path

import requests

from config import BINANCE_DATA_URL, BINANCE_KLINES_URL, RAW_DIR


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def csv_path(symbol: str, date: str) -> Path:
    return RAW_DIR / symbol / f"{symbol}-aggTrades-{date}.csv"


def klines_path(symbol: str, date: str) -> Path:
    return RAW_DIR / "klines_1m" / symbol / f"{symbol}-1m-{date}.csv"


def download_day(symbol: str, date: str) -> Path:
    """Fetch, verify and unzip one symbol-day of aggTrades. Idempotent."""
    return _fetch(csv_path(symbol, date), f"{BINANCE_DATA_URL}/{symbol}/{symbol}-aggTrades-{date}.zip")


def download_klines(symbol: str, date: str) -> Path:
    """Fetch, verify and unzip one symbol-day of 1-minute klines. Idempotent."""
    return _fetch(klines_path(symbol, date), f"{BINANCE_KLINES_URL}/{symbol}/1m/{symbol}-1m-{date}.zip", quiet=True)


def _fetch(target: Path, url: str, quiet: bool = False) -> Path:
    if target.exists():
        if not quiet:
            print(f"  {target.name}: already downloaded")
        return target

    target.parent.mkdir(parents=True, exist_ok=True)
    name = url.rsplit("/", 1)[1]
    zip_path = target.parent / name

    expected = requests.get(url + ".CHECKSUM", timeout=30)
    expected.raise_for_status()
    expected_sha = expected.text.split()[0]

    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(zip_path, "wb") as f:
            for chunk in r.iter_content(1 << 20):
                f.write(chunk)

    actual_sha = _sha256(zip_path)
    if actual_sha != expected_sha:
        zip_path.unlink()
        raise ValueError(f"{name}: checksum mismatch ({actual_sha} != {expected_sha})")

    with zipfile.ZipFile(zip_path) as z:
        z.extractall(target.parent)
    zip_path.unlink()
    if not quiet:
        print(f"  {target.name}: downloaded, checksum ok, {target.stat().st_size / 1e6:.0f} MB")
    return target
