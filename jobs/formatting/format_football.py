"""
Formatting — Football standings
Reads raw JSON, normalizes columns, converts dates to UTC, writes Parquet.
Output: data/formatted/football/standings/
"""
import json
import os
import glob
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

import sys
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.environ.get("PROJECT_ROOT", os.path.dirname(os.path.dirname(_HERE))))
from config.config import RAW_PATH, FORMATTED_PATH, SPARK_MASTER

SCHEMA_STANDINGS = StructType([
    StructField("league_id",     IntegerType()),
    StructField("league_name",   StringType()),
    StructField("season",        IntegerType()),
    StructField("club_name",     StringType()),
    StructField("rank",          IntegerType()),
    StructField("points",        IntegerType()),
    StructField("played",        IntegerType()),
    StructField("won",           IntegerType()),
    StructField("drawn",         IntegerType()),
    StructField("lost",          IntegerType()),
    StructField("goals_for",     IntegerType()),
    StructField("goals_against", IntegerType()),
    StructField("ingested_at",   StringType()),
])


def parse_standings_file(path: str) -> list[dict]:
    with open(path) as f:
        raw = json.load(f)

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
    spark = (
        SparkSession.builder
        .appName("format_football_standings")
        .master(SPARK_MASTER)
        .config("spark.hadoop.security.authentication", "simple")
        .config("spark.driver.extraJavaOptions",
                "--add-opens=java.base/javax.security.auth=ALL-UNNAMED "
                "--add-opens=java.base/java.lang=ALL-UNNAMED")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    all_rows = []
    for path in glob.glob(f"{RAW_PATH}/football/standings/**/*.json", recursive=True):
        all_rows.extend(parse_standings_file(path))

    if not all_rows:
        print("No raw standings found.")
        return

    df = spark.createDataFrame(all_rows, schema=SCHEMA_STANDINGS)
    df = df.dropDuplicates(["league_id", "season", "club_name"])

    out = f"{FORMATTED_PATH}/football/standings"
    os.makedirs(out, exist_ok=True)

    # Save via pandas to bypass Hadoop FileSystem Java 17+ incompatibility
    pandas_df = df.toPandas()
    pandas_df.to_parquet(out + "/data.parquet", index=False)
    print(f"Written {len(pandas_df)} rows to {out}")
    spark.stop()


if __name__ == "__main__":
    run()
