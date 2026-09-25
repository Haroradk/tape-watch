"""Download Binance daily aggTrades files and verify them against their checksums.

This is the "exchange archive" the replayer reads from. It is not a medallion
layer: bronze is only what the stream actually delivered.
"""

import hashlib
import zipfile
from pathlib import Path

import requests

from config import BINANCE_DATA_URL, RAW_DIR


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def csv_path(symbol: str, date: str) -> Path:
    return RAW_DIR / symbol / f"{symbol}-aggTrades-{date}.csv"


def download_day(symbol: str, date: str) -> Path:
    """Fetch, verify and unzip one symbol-day. Idempotent: skips if the CSV exists."""
    target = csv_path(symbol, date)
    if target.exists():
        print(f"  {symbol} {date}: already downloaded")
        return target

    target.parent.mkdir(parents=True, exist_ok=True)
    name = f"{symbol}-aggTrades-{date}.zip"
    url = f"{BINANCE_DATA_URL}/{symbol}/{name}"
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
    print(f"  {symbol} {date}: downloaded, checksum ok, {target.stat().st_size / 1e6:.0f} MB")
    return target
