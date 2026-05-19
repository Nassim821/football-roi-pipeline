"""
Ingestion — UEFA Champions League fixtures
Fetches all CL fixtures per season to determine each club's furthest round.
Output: data/raw/champions_league/fixtures/<season>/<date>/
"""
import json
import os
import time
import requests
from datetime import date

import sys
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.environ.get("PROJECT_ROOT", os.path.dirname(os.path.dirname(_HERE))))
from config.config import API_FOOTBALL_KEY, API_FOOTBALL_BASE_URL, SEASONS, RAW_PATH

CL_LEAGUE_ID = 2
HEADERS = {"x-apisports-key": API_FOOTBALL_KEY}


def fetch_cl_fixtures(season: int) -> dict:
    url = f"{API_FOOTBALL_BASE_URL}/fixtures"
    resp = requests.get(url, headers=HEADERS, params={
        "league": CL_LEAGUE_ID,
        "season": season,
    })
    resp.raise_for_status()
    return resp.json()


def save_raw(data: dict, season: int) -> str:
    today = date.today().isoformat()
    folder = f"{RAW_PATH}/champions_league/fixtures/{season}/{today}"
    os.makedirs(folder, exist_ok=True)
    path = f"{folder}/cl_fixtures.json"
    with open(path, "w") as f:
        json.dump(data, f)
    print(f"Saved {path} ({data.get('results', 0)} fixtures)")
    return path


def run():
    for season in SEASONS:
        print(f"Fetching CL fixtures for season {season}...")
        data = fetch_cl_fixtures(season)
        save_raw(data, season)
        time.sleep(7)


if __name__ == "__main__":
    run()
