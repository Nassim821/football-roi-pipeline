import os

# API-Football — clé chargée depuis .env (cf. .env.example)
API_FOOTBALL_KEY = os.getenv("API_FOOTBALL_KEY")
API_FOOTBALL_BASE_URL = "https://v3.football.api-sports.io"

# Leagues to track (id: name)
LEAGUES = {
    61:  "Ligue 1",
    39:  "Premier League",
    140: "La Liga",
    78:  "Bundesliga",
    135: "Serie A",
}
SEASON  = 2023          # kept for backwards compatibility
SEASONS = [2022, 2023, 2024]

# Datalake paths
BASE_PATH = os.getenv("DATALAKE_PATH", "/Users/nassimelkharroubi/Documents/Projet_data/data")
RAW_PATH       = f"{BASE_PATH}/raw"
FORMATTED_PATH = f"{BASE_PATH}/formatted"
USAGE_PATH     = f"{BASE_PATH}/usage"

# Elasticsearch
ES_HOST  = os.getenv("ES_HOST", "http://localhost:9200")
ES_INDEX = "football_roi"

# Spark
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
