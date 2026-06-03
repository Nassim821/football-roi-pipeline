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

    # Associer les labels lisibles aux clusters via assignation 1-pour-1.
    # On projette chaque centroïde dans le plan (budget, roi) normalisé puis on
    # calcule sa distance à 4 archétypes (un par coin) ; chaque label n'est
    # attribué qu'une seule fois — pas de collapse possible.
    centroids = df.groupby("cluster")[["squad_value_m", "roi", "points"]].mean()

    norm = (centroids[["squad_value_m", "roi"]] - centroids[["squad_value_m", "roi"]].min()) / \
           (centroids[["squad_value_m", "roi"]].max() - centroids[["squad_value_m", "roi"]].min())

    archetypes = {
        "Efficaces":   (0.0, 1.0),   # petit budget, gros ROI
        "Dominants":   (1.0, 1.0),   # gros budget, gros ROI
        "Gaspilleurs": (1.0, 0.0),   # gros budget, petit ROI
        "Standards":   (0.0, 0.0),   # petit budget, petit ROI
    }

    # Greedy : on assigne en priorité les paires (cluster, archétype) les plus proches.
    pairs = sorted(
        (
            ((norm.loc[c, "squad_value_m"] - bx) ** 2 + (norm.loc[c, "roi"] - by) ** 2,
             c, label)
            for c in norm.index
            for label, (bx, by) in archetypes.items()
        )
    )
    cluster_map: dict = {}
    used_labels: set = set()
    for _dist, c, label in pairs:
        if c in cluster_map or label in used_labels:
            continue
        cluster_map[c] = label
        used_labels.add(label)
        if len(cluster_map) == len(centroids):
            break

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
