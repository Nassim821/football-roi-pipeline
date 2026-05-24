"""
ML — K-Means Clustering des clubs
Regroupe les clubs en 4 profils selon (squad_value_m, roi, points) :
  - Cluster 0 : "Efficaces"   → petit budget, bon ROI
  - Cluster 1 : "Standards"   → budget moyen, résultats attendus
  - Cluster 2 : "Dominants"   → gros budget ET bons résultats
  - Cluster 3 : "Gaspilleurs" → gros budget, ROI décevant
Output: data/usage/roi_clusters/data.parquet
"""
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.environ.get("PROJECT_ROOT", os.path.dirname(os.path.dirname(_HERE))))

import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

from config.config import USAGE_PATH


CLUSTER_LABELS = {
    0: "Efficaces",
    1: "Standards",
    2: "Dominants",
    3: "Gaspilleurs",
}


def run():
    # Charger les données ROI
    df = pd.read_parquet(f"{USAGE_PATH}/roi_sportif/data.parquet")

    # Features pour le clustering
    features = ["squad_value_m", "roi", "points"]
    X = df[features].copy()

    # Normalisation
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # K-Means avec 4 clusters
    kmeans = KMeans(n_clusters=4, random_state=42, n_init=10)
    df["cluster"] = kmeans.fit_predict(X_scaled)

    # Associer les labels lisibles aux clusters
    # On identifie chaque cluster par ses centroïdes (budget moyen, roi moyen)
    centroids = df.groupby("cluster")[["squad_value_m", "roi", "points"]].mean()
    centroids_sorted = centroids.sort_values("roi", ascending=False)

    # Mapping automatique basé sur ROI et budget
    cluster_map = {}
    ranked = centroids.copy()
    ranked["roi_rank"]    = ranked["roi"].rank(ascending=False)
    ranked["budget_rank"] = ranked["squad_value_m"].rank(ascending=False)

    for c in ranked.index:
        roi_r    = ranked.loc[c, "roi_rank"]
        budget_r = ranked.loc[c, "budget_rank"]
        if roi_r <= 2 and budget_r >= 3:
            cluster_map[c] = "Efficaces"
        elif budget_r <= 2 and roi_r <= 2:
            cluster_map[c] = "Dominants"
        elif budget_r <= 2 and roi_r >= 3:
            cluster_map[c] = "Gaspilleurs"
        else:
            cluster_map[c] = "Standards"

    df["cluster_label"] = df["cluster"].map(cluster_map)

    # Sauvegarde
    out = f"{USAGE_PATH}/roi_clusters"
    os.makedirs(out, exist_ok=True)
    df.to_parquet(f"{out}/data.parquet", index=False)

    print(f"Written {len(df)} rows to {out}")
    print("\nRépartition des clusters :")
    print(df.groupby("cluster_label")[["squad_value_m", "roi", "points"]].mean().round(2))
    print("\nTop 5 clubs par cluster :")
    for label in df["cluster_label"].unique():
        top = df[df["cluster_label"] == label].nlargest(5, "roi")[["club_name", "league_name", "roi", "squad_value_m"]]
        print(f"\n--- {label} ---")
        print(top.to_string(index=False))


if __name__ == "__main__":
    run()
