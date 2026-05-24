"""
Formatting — Football standings
Reads raw JSON, normalizes columns, writes Parquet via pandas.
Output: data/formatted/football/standings/
"""
import json
import os
import glob
from datetime import datetime, timezone

import pandas as pd

import sys
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.environ.get("PROJECT_ROOT", os.path.dirname(os.path.dirname(_HERE))))
from config.config import RAW_PATH, FORMATTED_PATH


def safe_open_json(path: str, retries: int = 8, delay: float = 3.0) -> dict:
    import time
    for i in range(retries):
        try:
            with open(path) as f:
                return json.load(f)
        except OSError as e:
            if i < retries - 1:
                time.sleep(delay)
            else:
                raise


def parse_standings_file(path: str) -> list:
    raw = safe_open_json(path)

    rows = []
    ingested_at = datetime.now(timezone.utc).isoformat()
    for league_data in raw.get("response", []):
        league    = league_data["league"]
        standings = league_data["league"]["standings"]
        for group in standings:
            for team in group:
                rows.append({
                    "league_id":     league["id"],
                    "league_name":   league["name"],
                    "season":        league["season"],
                    "club_name":     team["team"]["name"],
                    "rank":          team["rank"],
                    "points":        team["points"],
                    "played":        team["all"]["played"],
                    "won":           team["all"]["win"],
                    "drawn":         team["all"]["draw"],
                    "lost":          team["all"]["lose"],
                    "goals_for":     team["all"]["goals"]["for"],
                    "goals_against": team["all"]["goals"]["against"],
                    "ingested_at":   ingested_at,
                })
    return rows


def run():
    import time; time.sleep(5)  # wait for macOS Docker FS locks to release
    all_rows = []
    for path in glob.glob(f"{RAW_PATH}/football/standings/**/*.json", recursive=True):
        all_rows.extend(parse_standings_file(path))

    if not all_rows:
        print("No raw standings found.")
        return

    df = pd.DataFrame(all_rows)
    df = df.drop_duplicates(subset=["league_id", "season", "club_name"])

    # Cast types
    for col in ["league_id", "season", "rank", "points", "played",
                "won", "drawn", "lost", "goals_for", "goals_against"]:
        df[col] = df[col].astype("Int64")

    out = f"{FORMATTED_PATH}/football/standings"
    os.makedirs(out, exist_ok=True)
    df.to_parquet(f"{out}/data.parquet", index=False)
    print(f"Written {len(df)} rows to {out}")


if __name__ == "__main__":
    run()
