"""
Formatting — Transfermarkt squad values
Reads raw JSON, cleans monetary values (e.g. "€45.20m" → 45.2),
writes Parquet to data/formatted/transfermarkt/squad_values/
"""
import json
import glob
import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

import sys
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.environ.get("PROJECT_ROOT", os.path.dirname(os.path.dirname(_HERE))))
from config.config import RAW_PATH, FORMATTED_PATH, SPARK_MASTER

SCHEMA = StructType([
    StructField("league",        StringType()),
    StructField("season",        IntegerType()),
    StructField("club_name",     StringType()),
    StructField("squad_size",    IntegerType()),
    StructField("avg_age",       FloatType()),
    StructField("squad_value_m", FloatType()),
    StructField("ingested_at",   StringType()),
])


def parse_value_m(raw: str) -> float:
    """Convert '€45.20m' or '€1.20bn' to float in millions."""
    raw = raw.replace("€", "").replace(",", ".").strip()
    if "bn" in raw:
        return float(raw.replace("bn", "")) * 1000
    if "m" in raw:
        return float(raw.replace("m", ""))
    return 0.0


def parse_file(path: str) -> list[dict]:
    with open(path) as f:
        rows = json.load(f)

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
    spark = (
        SparkSession.builder
        .appName("format_transfermarkt")
        .master(SPARK_MASTER)
        .config("spark.hadoop.security.authentication", "simple")
        .config("spark.driver.extraJavaOptions",
                "--add-opens=java.base/javax.security.auth=ALL-UNNAMED "
                "--add-opens=java.base/java.lang=ALL-UNNAMED")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    all_rows = []
    for path in glob.glob(f"{RAW_PATH}/transfermarkt/squad_values/**/*.json", recursive=True):
        all_rows.extend(parse_file(path))

    if not all_rows:
        print("No raw Transfermarkt data found.")
        return

    df = spark.createDataFrame(all_rows, schema=SCHEMA)
    df = df.dropDuplicates(["league", "season", "club_name"])

    out = f"{FORMATTED_PATH}/transfermarkt/squad_values"
    os.makedirs(out, exist_ok=True)

    # Save via pandas to bypass Hadoop FileSystem Java 17+ incompatibility
    pandas_df = df.toPandas()
    pandas_df.to_parquet(out + "/data.parquet", index=False)
    print(f"Written {len(pandas_df)} rows to {out}")
    spark.stop()


if __name__ == "__main__":
    run()
