"""
Ingestion — Transfermarkt (scraping)
Scrapes squad market values and transfer spending per club/season.
Stores raw JSON in data/raw/transfermarkt/<entity>/<date>/
"""
import json
import os
import time
from datetime import date

import cloudscraper
from bs4 import BeautifulSoup

import sys
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.environ.get("PROJECT_ROOT", os.path.dirname(os.path.dirname(_HERE))))
from config.config import RAW_PATH, SEASONS

BASE_URL = "https://www.transfermarkt.com"

# Transfermarkt league slugs (path, league_id)
TM_LEAGUES = {
    "ligue-1":       ("ligue-1/startseite/wettbewerb/FR1",   "FR1"),
    "premier-league": ("premier-league/startseite/wettbewerb/GB1", "GB1"),
    "laliga":        ("laliga/startseite/wettbewerb/ES1",    "ES1"),
    "bundesliga":    ("1-bundesliga/startseite/wettbewerb/L1", "L1"),
    "serie-a":       ("serie-a/startseite/wettbewerb/IT1",   "IT1"),
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
}


def scrape_squad_values(league_slug: str, league_path: str, season: int) -> list[dict]:
    scraper = cloudscraper.create_scraper()
    url = f"{BASE_URL}/{league_path}/plus/?saison_id={season}"
    resp = scraper.get(url, headers=HEADERS)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table", {"class": "items"})
    if not table:
        return []

    clubs = []
    for row in table.find("tbody").find_all("tr", class_=["odd", "even"]):
        cols = row.find_all("td")
        if len(cols) < 5:
            continue
        clubs.append({
            "league":        league_slug,
            "season":        season,
            "club_name":     cols[1].get_text(strip=True),
            "squad_size":    cols[2].get_text(strip=True),
            "avg_age":       cols[3].get_text(strip=True),
            "squad_value_m": cols[-1].get_text(strip=True),
        })
    return clubs


def save_raw(data: list, entity: str, league_id: str) -> str:
    today = date.today().isoformat()
    folder = f"{RAW_PATH}/transfermarkt/{entity}/{today}"
    os.makedirs(folder, exist_ok=True)
    path = f"{folder}/{league_id}.json"
    with open(path, "w") as f:
        json.dump(data, f)
    print(f"Saved {path} ({len(data)} clubs)")
    return path


def run():
    for season in SEASONS:
        for slug, (path, league_id) in TM_LEAGUES.items():
            print(f"Scraping {slug} {season}...")
            clubs = scrape_squad_values(slug, path, season)
            save_raw(clubs, f"squad_values/{season}", f"{league_id}_{season}")
            time.sleep(3)


if __name__ == "__main__":
    run()
