"""
Formatting — Transfermarkt squad values
Reads raw JSON, cleans monetary values (e.g. "€45.20m" → 45.2),
writes Parquet via pandas to data/formatted/transfermarkt/squad_values/
"""
import json
import glob
import os
from datetime import datetime, timezone

import pandas as pd

import sys
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.environ.get("PROJECT_ROOT", os.path.dirname(os.path.dirname(_HERE))))
from config.config import RAW_PATH, FORMATTED_PATH


def parse_value_m(raw: str) -> float:
    """Convert '€45.20m' or '€1.20bn' to float in millions."""
    raw = raw.replace("€", "").replace(",", ".").strip()
    if "bn" in raw:
        return float(raw.replace("bn", "")) * 1000
    if "m" in raw:
        return float(raw.replace("m", ""))
    return 0.0


def safe_open_json(path: str, retries: int = 8, delay: float = 3.0):
    import time
    for i in range(retries):
        try:
            with open(path) as f:
                return json.load(f)
        except OSError:
            if i < retries - 1:
                time.sleep(delay)
            else:
                raise


def parse_file(path: str) -> list:
    rows = safe_open_json(path)

    ingested_at = datetime.now(timezone.utc).isoformat()
    result = []
    for r in rows:
        try:
            result.append({
                "league":        r["league"],
                "season":        int(r["season"]),
                "club_name":     r["club_name"],
                "squad_size":    int(r.get("squad_size", 0) or 0),
                "avg_age":       float(r.get("avg_age", 0) or 0),
                "squad_value_m": parse_value_m(r.get("squad_value_m", "0")),
                "ingested_at":   ingested_at,
            })
        except (ValueError, KeyError):
            continue
    return result


def run():
    import time; time.sleep(5)  # wait for macOS Docker FS locks to release
    all_rows = []
    for path in glob.glob(f"{RAW_PATH}/transfermarkt/squad_values/**/*.json", recursive=True):
        all_rows.extend(parse_file(path))

    if not all_rows:
        print("No raw Transfermarkt data found.")
        return

    df = pd.DataFrame(all_rows)
    df = df.drop_duplicates(subset=["league", "season", "club_name"])
    df["season"]     = df["season"].astype("Int64")
    df["squad_size"] = df["squad_size"].astype("Int64")
    df["avg_age"]    = df["avg_age"].astype(float)
    df["squad_value_m"] = df["squad_value_m"].astype(float)

    out = f"{FORMATTED_PATH}/transfermarkt/squad_values"
    os.makedirs(out, exist_ok=True)
    df.to_parquet(f"{out}/data.parquet", index=False)
    print(f"Written {len(df)} rows to {out}")


if __name__ == "__main__":
    run()
