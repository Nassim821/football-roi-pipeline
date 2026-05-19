"""
Formatting — Champions League parcours
Reads raw CL fixtures, determines the furthest round reached per club per season.
Stage values:
  Group Stage / League Stage = 1
  Knockout Play-offs         = 2
  Round of 16                = 3
  Quarter-finals             = 4
  Semi-finals                = 5
  Final (loser)              = 6
  Final (winner)             = 7
Output: data/formatted/champions_league/parcours/data.parquet
"""
import glob
import json
import os

import pandas as pd

import sys
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.environ.get("PROJECT_ROOT", os.path.dirname(os.path.dirname(_HERE))))
from config.config import RAW_PATH, FORMATTED_PATH

def get_stage_value(round_str: str) -> int:
    r = round_str.lower().strip()
    # Qualifying rounds before group/league stage — exclude
    if "qualifying" in r or "preliminary" in r:
        return 0
    # Play-offs that are qualifying (2022/2023), not knockout
    if r == "play-offs":
        return 0
    # Group stage: "Group A - 1", "Group B - 3", etc.
    if r.startswith("group"):
        return 1
    # League stage: "League Stage - 1" ... "League Stage - 8"
    if r.startswith("league stage"):
        return 1
    # Knockout Round Play-offs (2024 format)
    if "knockout round play-offs" in r:
        return 2
    # Round of 16
    if "round of 16" in r or "1/8" in r:
        return 3
    # Quarter-finals
    if "quarter" in r or "1/4" in r:
        return 4
    # Semi-finals
    if "semi" in r or "1/2" in r:
        return 5
    # Final
    if r == "final":
        return 6
    return 0

STAGE_NAMES = {
    1: "Group / League Stage",
    2: "Knockout Play-offs",
    3: "Round of 16",
    4: "Quarter-finals",
    5: "Semi-finals",
    6: "Final",
    7: "Winner",
}


def parse_fixtures(path: str) -> list[dict]:
    with open(path) as f:
        data = json.load(f)

    team_stages: dict[tuple, dict] = {}

    for fixture in data.get("response", []):
        season     = fixture["league"]["season"]
        round_str  = fixture["league"]["round"]
        stage_val  = get_stage_value(round_str)
        if stage_val == 0:
            continue

        status = fixture["fixture"]["status"]["short"]
        home   = fixture["teams"]["home"]
        away   = fixture["teams"]["away"]

        for team in [home, away]:
            key = (season, team["name"])
            current = team_stages.get(key, {"stage_value": 0, "stage_name": ""})
            if stage_val > current["stage_value"]:
                team_stages[key] = {
                    "season":      season,
                    "club_name":   team["name"],
                    "stage_value": stage_val,
                    "stage_name":  STAGE_NAMES.get(stage_val, round_str),
                }

        # Mark winner of final
        if stage_val == 6 and status == "FT":
            if home.get("winner"):
                winner_key = (season, home["name"])
                team_stages[winner_key]["stage_value"] = 7
                team_stages[winner_key]["stage_name"]  = "Winner"
            elif away.get("winner"):
                winner_key = (season, away["name"])
                team_stages[winner_key]["stage_value"] = 7
                team_stages[winner_key]["stage_name"]  = "Winner"

    return list(team_stages.values())


def run():
    all_rows = []
    for path in glob.glob(f"{RAW_PATH}/champions_league/fixtures/**/*.json", recursive=True):
        all_rows.extend(parse_fixtures(path))

    if not all_rows:
        print("No CL fixture data found.")
        return

    df = pd.DataFrame(all_rows)
    df = df.sort_values(["season", "stage_value"], ascending=[True, False])
    df = df.drop_duplicates(subset=["season", "club_name"], keep="first")

    out = f"{FORMATTED_PATH}/champions_league/parcours"
    os.makedirs(out, exist_ok=True)
    df.to_parquet(f"{out}/data.parquet", index=False)
    print(f"Written {len(df)} rows to {out}")
    print(df[["season", "club_name", "stage_name", "stage_value"]].sort_values(
        ["season", "stage_value"], ascending=[True, False]).head(20))


if __name__ == "__main__":
    run()
