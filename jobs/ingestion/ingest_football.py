"""
Ingestion — API-Football
Fetches standings and results for configured leagues and stores raw JSON
in the datalake under data/raw/football/<entity>/<date>/
"""
import json
import os
import time
import requests
from datetime import date

import sys
# project_root is 3 levels up from this file (jobs/ingestion/ → jobs/ → project_root/)
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.environ.get("PROJECT_ROOT", os.path.dirname(os.path.dirname(_HERE))))
from config.config import API_FOOTBALL_KEY, API_FOOTBALL_BASE_URL, LEAGUES, SEASONS, RAW_PATH

HEADERS = {
    "x-apisports-key": API_FOOTBALL_KEY,
}


def fetch_standings(league_id: int, season: int) -> dict:
    url = f"{API_FOOTBALL_BASE_URL}/standings"
    resp = requests.get(url, headers=HEADERS, params={"league": league_id, "season": season})
    resp.raise_for_status()
    return resp.json()


def fetch_results(league_id: int, season: int) -> dict:
    url = f"{API_FOOTBALL_BASE_URL}/fixtures"
    resp = requests.get(url, headers=HEADERS, params={
        "league": league_id,
        "season": season,
        "status": "FT",
    })
    resp.raise_for_status()
    return resp.json()


def save_raw(data: dict, entity: str, league_id: int) -> str:
    today = date.today().isoformat()
    folder = f"{RAW_PATH}/football/{entity}/{today}"
    os.makedirs(folder, exist_ok=True)
    path = f"{folder}/league_{league_id}.json"
    with open(path, "w") as f:
        json.dump(data, f)
    print(f"Saved {path}")
    return path


def run():
    for season in SEASONS:
        for league_id, league_name in LEAGUES.items():
            print(f"Fetching standings for {league_name} {season}...")
            standings = fetch_standings(league_id, season)
            save_raw(standings, f"standings/{season}", league_id)
            time.sleep(7)

            print(f"Fetching results for {league_name} {season}...")
            results = fetch_results(league_id, season)
            save_raw(results, f"results/{season}", league_id)
            time.sleep(7)


if __name__ == "__main__":
    run()
