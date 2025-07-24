#!/usr/bin/env python3
"""
Download 30-minute OHLCV for ETH/USDT (Binance) from 2022-01-01 to today.
Saves the data as data/eth_30m.csv
"""

from datetime import datetime, timezone
import csv, time, requests, pathlib

START   = "2022-01-01"
SYMBOL  = "ETHUSDT"
INTERVAL= "30m"
OUTFILE = pathlib.Path("data/eth_30m.csv")
URL     = "https://api.binance.com/api/v3/klines"
LIMIT   = 1000            # Binance max rows per request


def iso_to_ms(ts: str) -> int:
    """ISO-8601 date → milliseconds since epoch (UTC)."""
    return int(
        datetime.fromisoformat(ts)
        .replace(tzinfo=timezone.utc)
        .timestamp() * 1000
    )


def fetch(start_iso: str, end_iso: str):
    """Return a list of dicts with 30m bars between two dates (inclusive)."""
    start_ms, end_ms = iso_to_ms(start_iso), iso_to_ms(end_iso)
    rows = []
    params = {
        "symbol": SYMBOL,
        "interval": INTERVAL,
        "startTime": start_ms,
        "endTime": end_ms,
        "limit": LIMIT,
    }

    while True:
        resp = requests.get(URL, params=params, timeout=30)
        resp.raise_for_status()
        chunk = resp.json()
        if not chunk:
            break
        rows += chunk
        last_close_ms = chunk[-1][6]        # closeTime
        if last_close_ms >= end_ms - 60_000:
            break
        params["startTime"] = last_close_ms + 1
        time.sleep(0.2)                     # polite rate-limit

    return [
        {
            "time":   datetime.utcfromtimestamp(c[0] / 1000).isoformat(),
            "open":   c[1],
            "high":   c[2],
            "low":    c[3],
            "close":  c[4],
            "volume": c[5],
        }
        for c in rows
    ]


def main() -> None:
    OUTFILE.parent.mkdir(exist_ok=True)
    today = datetime.utcnow().date().isoformat()
    rows = fetch(START, today)

    with OUTFILE.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    print(f"Saved {len(rows):,} rows → {OUTFILE}")


if __name__ == "__main__":
    main()
