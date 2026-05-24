"""
Indexing — Push ROI data to Elasticsearch
Indexes football_roi (domestic leagues) and football_cl_roi (Champions League).
"""
import os
import pandas as pd
from elasticsearch import Elasticsearch, helpers

import sys
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.environ.get("PROJECT_ROOT", os.path.dirname(os.path.dirname(_HERE))))
from config.config import USAGE_PATH, ES_HOST, ES_INDEX

CL_INDEX      = "football_cl_roi"
CLUSTER_INDEX = "football_roi_clusters"

ROI_MAPPING = {
    "mappings": {
        "properties": {
            "league_name":   {"type": "keyword"},
            "season":        {"type": "integer"},
            "club_name":     {"type": "keyword"},
            "rank":          {"type": "integer"},
            "points":        {"type": "integer"},
            "played":        {"type": "integer"},
            "squad_value_m": {"type": "float"},
            "roi":           {"type": "float"},
            "roi_rank":      {"type": "integer"},
        }
    }
}

CL_MAPPING = {
    "mappings": {
        "properties": {
            "season":        {"type": "integer"},
            "club_name":     {"type": "keyword"},
            "stage_name":    {"type": "keyword"},
            "stage_value":   {"type": "integer"},
            "squad_value_m": {"type": "float"},
            "cl_roi":        {"type": "float"},
            "cl_roi_rank":   {"type": "integer"},
        }
    }
}


CLUSTER_MAPPING = {
    "mappings": {
        "properties": {
            "league_name":    {"type": "keyword"},
            "season":         {"type": "integer"},
            "club_name":      {"type": "keyword"},
            "points":         {"type": "integer"},
            "squad_value_m":  {"type": "float"},
            "roi":            {"type": "float"},
            "roi_rank":       {"type": "integer"},
            "cluster":        {"type": "integer"},
            "cluster_label":  {"type": "keyword"},
        }
    }
}


def create_index(es: Elasticsearch, index: str, mapping: dict):
    if not es.indices.exists(index=index):
        es.indices.create(index=index, body=mapping)
        print(f"Index '{index}' created.")
    else:
        print(f"Index '{index}' already exists.")


def run():
    es = Elasticsearch(ES_HOST)

    # --- Domestic leagues ---
    df = pd.read_parquet(f"{USAGE_PATH}/roi_sportif/data.parquet")
    create_index(es, ES_INDEX, ROI_MAPPING)
    actions = [
        {
            "_index": ES_INDEX,
            "_id":    f"{r['league_name']}_{r['season']}_{r['club_name']}",
            "_source": r,
        }
        for r in df.to_dict(orient="records")
    ]
    helpers.bulk(es, actions)
    print(f"Indexed {len(actions)} documents into '{ES_INDEX}'.")

    # --- Champions League ---
    cl = pd.read_parquet(f"{USAGE_PATH}/cl_roi/data.parquet")
    create_index(es, CL_INDEX, CL_MAPPING)
    cl_actions = [
        {
            "_index": CL_INDEX,
            "_id":    f"{r['season']}_{r['club_name']}",
            "_source": r,
        }
        for r in cl.to_dict(orient="records")
    ]
    helpers.bulk(es, cl_actions)
    print(f"Indexed {len(cl_actions)} documents into '{CL_INDEX}'.")

    # --- ML Clusters ---
    cluster_path = f"{USAGE_PATH}/roi_clusters/data.parquet"
    if os.path.exists(cluster_path):
        clusters = pd.read_parquet(cluster_path)
        create_index(es, CLUSTER_INDEX, CLUSTER_MAPPING)
        cluster_actions = [
            {
                "_index": CLUSTER_INDEX,
                "_id":    f"{r['league_name']}_{r['season']}_{r['club_name']}",
                "_source": r,
            }
            for r in clusters.to_dict(orient="records")
        ]
        helpers.bulk(es, cluster_actions)
        print(f"Indexed {len(cluster_actions)} documents into '{CLUSTER_INDEX}'.")
    else:
        print(f"Skipped '{CLUSTER_INDEX}': {cluster_path} not found (run ml_clustering first).")


if __name__ == "__main__":
    run()
