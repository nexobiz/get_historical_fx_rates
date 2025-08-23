
#!/usr/bin/env python3
"""
Fetch daily USD exchange rates from exchangerate.host and upsert into a Supabase table.

- Backfills a date range (default: 2020-01-01 to today) using the /timeframe endpoint (<=365 days per call).
- Optionally limits to a list of currency codes, or fetches ALL supported currencies.
- Bulk upserts rows into the `exchange_rates` table (primary key on (rate_date, base_currency, symbol)).

Usage examples:
  python fetch_usd_rates.py --start 2020-01-01 --end today --symbols CAD,EUR,GBP,AED,TRY
  python fetch_usd_rates.py --start 2020-01-01 --end 2023-12-31 --symbols ALL
  python fetch_usd_rates.py --start 2024-01-01 --end today --batch-days 365 --dry-run

Environment variables required:
  SUPABASE_URL                # e.g., https://YOUR-PROJECT.supabase.co
  SUPABASE_SERVICE_ROLE_KEY   # Service Role key for server-side upserts
  EXCHANGERATE_HOST_KEY       # (Free) API key from https://exchangerate.host (100 requests/mo on free)
"""

import argparse
import datetime as dt
import os
import sys
import time
from typing import Dict, Iterable, List, Optional, Tuple
import yaml

import requests
from dotenv import load_dotenv

try:
    from supabase import create_client, Client
except Exception as e:
    print("Missing dependency 'supabase'. Install with: pip install supabase")
    raise

load_dotenv()

def load_config(path: str = "config.yaml") -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)
    

def parse_date(s: str) -> dt.date:
    if s.lower() == "today":
        return dt.date.today()
    return dt.date.fromisoformat(s)


def daterange_chunks(start: dt.date, end: dt.date, max_span_days: int = 365) -> Iterable[Tuple[dt.date, dt.date]]:
    """Yield (chunk_start, chunk_end) ranges inclusive within [start, end], each <= max_span_days."""
    cur = start
    one_day = dt.timedelta(days=1)
    max_span = dt.timedelta(days=max_span_days - 1)  # inclusive window size
    while cur <= end:
        chunk_end = min(cur + max_span, end)
        yield (cur, chunk_end)
        cur = chunk_end + one_day


def get_all_symbols(session: requests.Session, access_key: str) -> List[str]:
    """Fetch the full list of supported currency codes. Costs 1 API request."""
    url = "https://api.exchangerate.host/list"
    params = {"access_key": access_key}
    r = session.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    if not data.get("success", False):
        raise RuntimeError(f"/list failed: {data}")
    currencies = data["currencies"]
    return sorted(currencies.keys())


def fetch_timeframe(
    session: requests.Session,
    access_key: str,
    start: dt.date,
    end: dt.date,
    symbols: Optional[List[str]] = None,
) -> Dict[str, Dict[str, float]]:
    """
    Call /timeframe endpoint and return {date_str: {CURRENCY: rate, ...}, ...}
    Defaults to base/source USD (free plan default). Each chunk must be <= 365 days.
    """
    url = "https://api.exchangerate.host/timeframe"
    params = {
        "access_key": access_key,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        # Free plan default source is USD; we do not set 'source' to keep default.
    }
    if symbols:
        params["currencies"] = ",".join(symbols)

    for attempt in range(3):
        resp = session.get(url, params=params, timeout=60)
        try:
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            if attempt == 2:
                raise
            time.sleep(2 ** attempt)
            continue

        if not data.get("success", False):
            # Handle throttling and other API errors
            info = data.get("error", {}).get("info") or data
            if attempt == 2:
                raise RuntimeError(f"/timeframe error: {info}")
            time.sleep(2 ** attempt)
            continue

        # Expect keys: success, timeframe, start_date, end_date, source, quotes={date: {USDEUR: x, ...}} OR rates style
        quotes = data.get("quotes")
        if quotes is None:
            # Some variants return 'rates' nested by date -> {symbol: rate}
            quotes = data.get("rates")

        if quotes is None:
            raise RuntimeError(f"Unexpected /timeframe payload shape: {list(data.keys())}")

        # Normalize to {date: {SYM: rate, ...}} (strip leading 'USD' from pair keys if needed)
        normalized = {}
        for date_str, obj in quotes.items():
            inner = {}
            if isinstance(obj, dict):
                for k, v in obj.items():
                    if isinstance(v, (int, float)):
                        # k may be 'USDEUR' or 'EUR'
                        sym = k[3:] if k.startswith("USD") and len(k) == 6 else k
                        inner[sym] = float(v)
            normalized[date_str] = inner
        return normalized

    raise RuntimeError("Unreachable")


def supabase_client() -> "Client":
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise EnvironmentError("Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY in your environment or .env")
    return create_client(url, key)


def chunked(iterable: List[dict], size: int) -> Iterable[List[dict]]:
    for i in range(0, len(iterable), size):
        yield iterable[i : i + size]


def main():
    cfg = load_config()  # load from config.yaml

    start = parse_date(cfg.get("start", "2020-01-01"))
    end = parse_date(cfg.get("end", "today"))
    symbols_arg = cfg.get("symbols", "CAD,EUR,GBP,TRY,AED")
    table = cfg.get("table", "exchange_rates")
    batch_days = cfg.get("batch_days", 365)
    upsert_batch_size = cfg.get("upsert_batch_size", 1000)
    dry_run = cfg.get("dry_run", False)

    if end < start:
        sys.exit("End date must be on/after start date")

    access_key = os.environ.get("EXCHANGERATE_HOST_KEY")
    if not access_key:
        sys.exit("Missing EXCHANGERATE_HOST_KEY in .env")

    session = requests.Session()

    # Symbols to fetch
    if symbols_arg.strip().upper() == "ALL":
        symbols = get_all_symbols(session, access_key)
        print(f"Fetched {len(symbols)} supported symbols.")
    else:
        symbols = [s.strip().upper() for s in symbols_arg.split(",") if s.strip()]
        # Ensure we include USD (to store the identity 1.0 rate as well)
        if "USD" not in symbols:
            symbols.append("USD")

    all_rows: List[dict] = []
    print(f"Fetching USD rates from {start} to {end} in chunks (<= {batch_days} days each)...")

    for chunk_start, chunk_end in daterange_chunks(start, end, max_span_days=batch_days):
        data = fetch_timeframe(session, access_key, chunk_start, chunk_end, symbols=symbols)
        count_days = 0
        for date_str, inner in sorted(data.items()):
            count_days += 1
            for sym, rate in inner.items():
                row = {
                    "rate_date": date_str,
                    "base_currency": "USD",
                    "symbol": sym,
                    "rate": float(rate),
                    "provider": "exchangerate.host",
                    # 'fetched_at' will default on the DB side
                }
                all_rows.append(row)
        total_quotes = sum(len(v) for v in data.values())
        print(f"  - {chunk_start}..{chunk_end}: {count_days} days, {total_quotes} quotes")

    print(f"Prepared {len(all_rows)} rows to upsert into '{table}'.")

    if dry_run:
        print("Dry-run mode: exiting before DB write.")
        return

    client = supabase_client()
    # Bulk upsert in chunks for stability
    total = 0
    for batch in chunked(all_rows, upsert_batch_size):
        # If you created a PK on (rate_date, base_currency, symbol), PostgREST will use it for conflict resolution.
        client.table(table).upsert(batch).execute()
        total += len(batch)
    print(f"Upserted {total} rows into '{table}'. Done.")


if __name__ == "__main__":
    main()
