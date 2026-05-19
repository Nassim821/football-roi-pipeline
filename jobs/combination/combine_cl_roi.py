"""
Combination — ROI Champions League
JOIN parcours CL + squad values Transfermarkt.
cl_roi = stage_value / squad_value_m
Output: data/usage/cl_roi/data.parquet
"""
import os
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

import sys
_HERE = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.environ.get("PROJECT_ROOT", os.path.dirname(os.path.dirname(_HERE)))
sys.path.insert(0, _PROJECT_ROOT)
sys.path.insert(0, os.path.join(_PROJECT_ROOT, "jobs"))
from config.config import FORMATTED_PATH, USAGE_PATH, SPARK_MASTER

from combination.combine_roi import CLUB_NAME_MAPPING


def run():
    spark = (
        SparkSession.builder
        .appName("combine_cl_roi")
        .master(SPARK_MASTER)
        .config("spark.hadoop.security.authentication", "simple")
        .config("spark.driver.extraJavaOptions",
                "--add-opens=java.base/javax.security.auth=ALL-UNNAMED "
                "--add-opens=java.base/java.lang=ALL-UNNAMED")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    parcours_pd  = pd.read_parquet(f"{FORMATTED_PATH}/champions_league/parcours/data.parquet")
    transfers_pd = pd.read_parquet(f"{FORMATTED_PATH}/transfermarkt/squad_values/data.parquet")

    # Normalize CL club names (API-Football CL vs domestic spelling differences)
    CL_NAME_FIXES = {
        "Paris Saint Germain": "Paris Saint-Germain",
        "Bayern München":      "Bayern Munich",
    }
    parcours_pd["club_name"] = parcours_pd["club_name"].replace(CL_NAME_FIXES)

    transfers_pd["club_name_norm"] = transfers_pd["club_name"].replace(CLUB_NAME_MAPPING)

    parcours  = spark.createDataFrame(parcours_pd)
    transfers = spark.createDataFrame(transfers_pd)

    df = parcours.join(
        transfers,
        (parcours.club_name == transfers.club_name_norm) &
        (parcours.season    == transfers.season),
        how="inner",
    ).select(
        parcours.season,
        parcours.club_name,
        parcours.stage_name,
        parcours.stage_value,
        transfers.squad_value_m,
        F.round(parcours.stage_value / F.greatest(transfers.squad_value_m, F.lit(0.1)), 6)
         .alias("cl_roi"),
    )

    df = df.withColumn(
        "cl_roi_rank",
        F.rank().over(
            Window.partitionBy("season").orderBy(F.desc("cl_roi"))
        )
    )

    out = f"{USAGE_PATH}/cl_roi"
    os.makedirs(out, exist_ok=True)

    result_pd = df.toPandas()
    result_pd.to_parquet(f"{out}/data.parquet", index=False)
    print(f"Written {len(result_pd)} rows to {out}")
    print(result_pd[["club_name", "stage_name", "squad_value_m", "cl_roi", "cl_roi_rank"]].head(10))
    spark.stop()


if __name__ == "__main__":
    run()
