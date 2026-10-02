"""
download_gharchive.py

Downloads one or more hourly GH Archive files.
GH Archive publishes one gzipped JSON file per hour at:
https://data.gharchive.org/YYYY-MM-DD-H.json.gz  (H = hour, 0-23, no leading zero)

Usage:
    python download_gharchive.py 2026-08-10 15 16 17
    (downloads hours 15, 16, 17 for Aug 10 2026)
"""

import sys
import os
import requests

RAW_DIR = "data/raw"


def download_hour(date_str: str, hour: int):
    os.makedirs(RAW_DIR, exist_ok=True)
    url = f"https://data.gharchive.org/{date_str}-{hour}.json.gz"
    out_path = os.path.join(RAW_DIR, f"{date_str}-{hour}.json.gz")

    if os.path.exists(out_path):
        print(f"Already have {out_path}, skipping.")
        return out_path

    print(f"Downloading {url} ...")
    resp = requests.get(url, stream=True)
    resp.raise_for_status()

    with open(out_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)

    print(f"Saved to {out_path} ({os.path.getsize(out_path) / 1_000_000:.1f} MB)")
    return out_path


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python download_gharchive.py YYYY-MM-DD HOUR [HOUR ...]")
        sys.exit(1)

    date_str = sys.argv[1]
    hours = [int(h) for h in sys.argv[2:]]

    for h in hours:
        download_hour(date_str, h)
