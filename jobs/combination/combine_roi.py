"""
Combination — Calcul du ROI sportif
Lit les parquet formatés via pandas, JOIN + calcul ROI via Spark.
Output: data/usage/roi_sportif/
"""
import os
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

import sys
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.environ.get("PROJECT_ROOT", os.path.dirname(os.path.dirname(_HERE))))
from config.config import FORMATTED_PATH, USAGE_PATH, SPARK_MASTER

CLUB_NAME_MAPPING = {
    # Germany
    "TSG 1899 Hoffenheim":      "1899 Hoffenheim",
    "1.FC Heidenheim 1846":     "FC Heidenheim",
    "1.FC Union Berlin":        "Union Berlin",
    "1.FSV Mainz 05":           "FSV Mainz 05",
    "Bayer 04 Leverkusen":      "Bayer Leverkusen",
    "Borussia Mönchengladbach": "Borussia Monchengladbach",
    "SV Werder Bremen":         "Werder Bremen",
    "VfL Bochum":               "Vfl Bochum",
    # Spain
    "Deportivo Alavés":         "Alaves",
    "UD Almería":               "Almeria",
    "Athletic Bilbao":          "Athletic Club",
    "Atlético de Madrid":       "Atletico Madrid",
    "FC Barcelona":             "Barcelona",
    "Cádiz CF":                 "Cadiz",
    "Celta de Vigo":            "Celta Vigo",
    "Getafe CF":                "Getafe",
    "Girona FC":                "Girona",
    "UD Las Palmas":            "Las Palmas",
    "RCD Mallorca":             "Mallorca",
    "CA Osasuna":               "Osasuna",
    "Real Betis Balompié":      "Real Betis",
    "Valencia CF":              "Valencia",
    "Villarreal CF":            "Villarreal",
    # England
    "Arsenal FC":               "Arsenal",
    "AFC Bournemouth":          "Bournemouth",
    "Brentford FC":             "Brentford",
    "Brighton & Hove Albion":   "Brighton",
    "Burnley FC":               "Burnley",
    "Chelsea FC":               "Chelsea",
    "Everton FC":               "Everton",
    "Fulham FC":                "Fulham",
    "Liverpool FC":             "Liverpool",
    "Luton Town":               "Luton",
    "Newcastle United":         "Newcastle",
    "Sheffield United":         "Sheffield Utd",
    "Tottenham Hotspur":        "Tottenham",
    "West Ham United":          "West Ham",
    "Wolverhampton Wanderers":  "Wolves",
    # France
    "Paris Saint-Germain":      "Paris Saint Germain",
    "Clermont Foot 63":         "Clermont Foot",
    "Le Havre AC":              "LE Havre",
    "RC Lens":                  "Lens",
    "LOSC Lille":               "Lille",
    "FC Lorient":               "Lorient",
    "FC Metz":                  "Metz",
    "AS Monaco":                "Monaco",
    "Montpellier HSC":          "Montpellier",
    "FC Nantes":                "Nantes",
    "OGC Nice":                 "Nice",
    "Olympique Lyon":           "Lyon",
    "Olympique Marseille":      "Marseille",
    "RC Strasbourg Alsace":     "Strasbourg",
    "Stade Reims":              "Reims",
    "Stade Rennais FC":         "Rennes",
    "FC Toulouse":              "Toulouse",
    # Italy
    "Atalanta BC":              "Atalanta",
    "Bologna FC 1909":          "Bologna",
    "Cagliari Calcio":          "Cagliari",
    "FC Empoli":                "Empoli",
    "ACF Fiorentina":           "Fiorentina",
    "Frosinone Calcio":         "Frosinone",
    "Genoa CFC":                "Genoa",
    "Inter Milan":              "Inter",
    "Juventus FC":              "Juventus",
    "SS Lazio":                 "Lazio",
    "US Lecce":                 "Lecce",
    "AC Monza":                 "Monza",
    "SSC Napoli":               "Napoli",
    "US Salernitana 1919":      "Salernitana",
    "US Sassuolo":              "Sassuolo",
    "Torino FC":                "Torino",
    "Udinese Calcio":           "Udinese",
    "Hellas Verona":            "Verona",
}


def run():
    spark = (
        SparkSession.builder
        .appName("combine_roi_sportif")
        .master(SPARK_MASTER)
        .config("spark.hadoop.security.authentication", "simple")
        .config("spark.driver.extraJavaOptions",
                "--add-opens=java.base/javax.security.auth=ALL-UNNAMED "
                "--add-opens=java.base/java.lang=ALL-UNNAMED")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Lire les parquet via pandas (bypass Hadoop FS)
    standings_pd = pd.read_parquet(f"{FORMATTED_PATH}/football/standings/data.parquet")
    transfers_pd = pd.read_parquet(f"{FORMATTED_PATH}/transfermarkt/squad_values/data.parquet")

    # Normaliser les noms de clubs Transfermarkt
    transfers_pd["club_name_norm"] = transfers_pd["club_name"].replace(CLUB_NAME_MAPPING)

    # Convertir en Spark DataFrames pour le JOIN
    standings = spark.createDataFrame(standings_pd)
    transfers = spark.createDataFrame(transfers_pd)

    df = standings.join(
        transfers,
        (standings.club_name == transfers.club_name_norm) &
        (standings.season    == transfers.season),
        how="inner",
    ).select(
        standings.league_name,
        standings.season,
        standings.club_name,
        standings.rank,
        standings.points,
        standings.played,
        transfers.squad_value_m,
        F.round(standings.points / F.greatest(transfers.squad_value_m, F.lit(0.1)), 4)
         .alias("roi"),
    )

    df = df.withColumn(
        "roi_rank",
        F.rank().over(
            Window.partitionBy("league_name", "season").orderBy(F.desc("roi"))
        )
    )

    out = f"{USAGE_PATH}/roi_sportif"
    os.makedirs(out, exist_ok=True)

    result_pd = df.toPandas()
    result_pd.to_parquet(f"{out}/data.parquet", index=False)
    print(f"Written {len(result_pd)} rows to {out}")
    print(result_pd[["club_name", "points", "squad_value_m", "roi", "roi_rank"]].head(10))
    spark.stop()


if __name__ == "__main__":
    run()
