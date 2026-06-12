#!/usr/bin/env python3
"""Import a TWSE BFI82U CSV/HTML file into the display-only intelligence cache."""

import argparse
import sys
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from intelligence.data_collector import DataCollector  # noqa: E402


def parse_args():
    parser = argparse.ArgumentParser(description="Import TWSE BFI82U CSV/HTML into local cache")
    parser.add_argument("--file", required=True, help="Path to TWSE official BFI82U CSV or HTML file")
    parser.add_argument("--date", required=True, help="Data date in YYYY-MM-DD format")
    return parser.parse_args()


def main():
    args = parse_args()
    path = Path(args.file).expanduser()
    if not path.exists():
        raise SystemExit(f"File not found: {path}")

    try:
        data_date = date.fromisoformat(args.date)
    except ValueError as exc:
        raise SystemExit("--date must be YYYY-MM-DD") from exc

    content = path.read_bytes()
    text = DataCollector._decode_twse_text(content)

    spot = DataCollector._parse_bfi82u_csv(text, data_date)
    source = "TWSE BFI82U imported CSV"
    if not spot:
        spot = DataCollector._parse_bfi82u_html(text, data_date)
        source = "TWSE BFI82U imported HTML"
    if not spot:
        raise SystemExit("Could not parse BFI82U rows from the provided file")

    spot.status = "LIVE"
    spot.source = source
    spot.fetched_at = datetime.now()
    DataCollector._save_bfi82u_cache(spot)

    print(
        f"Imported BFI82U cache {spot.date}: "
        f"foreign={spot.foreign_buy_sell:+.2f}億 "
        f"trust={spot.trust_buy_sell:+.2f}億 "
        f"dealer={spot.dealer_buy_sell:+.2f}億"
    )


if __name__ == "__main__":
    main()
