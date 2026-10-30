# Football ROI Pipeline

An end-to-end data engineering project that answers one question: **which European football clubs get the most sporting return from their money?**

The pipeline ingests league standings, Champions League fixtures and squad market values, computes a custom **sporting ROI** metric with PySpark, segments clubs with K-Means, and serves the results in Elasticsearch and a Kibana dashboard. Everything is orchestrated by Apache Airflow and runs in Docker.

```
ROI = league points / squad market value (M€)
```

A club that collects many points with a small squad has a high ROI; a club with a huge squad and few points has a low one.

## Architecture

The data lake follows the **medallion pattern** (raw → formatted → usage):

```
 API-Football (standings)    ─┐
 API-Football (Champions L.) ─┼─►  raw (JSON)  ─►  formatted (Parquet)  ─►  usage (ROI, clusters)  ─►  Elasticsearch  ─►  Kibana
 Transfermarkt (scraping)    ─┘
```

| Layer | Content | Tools |
|---|---|---|
| `raw` | Untouched JSON from each source, versioned by date | `requests`, `cloudscraper`, BeautifulSoup |
| `formatted` | Cleaned, typed Parquet tables (no business logic) | pandas, PyArrow |
| `usage` | Business metrics: ROI, Champions League ROI, club clusters | PySpark, scikit-learn |
| Serving | Indexed documents and dashboard | Elasticsearch 9, Kibana 9 |

Data lake layout: `data/<layer>/<group>/<entity>/<date-version>/`.

### The Airflow DAG

The `football_roi_pipeline` DAG (daily at 06:00 UTC) has 10 tasks:

| Stage | Tasks |
|---|---|
| Ingestion | `source_to_raw_1` (standings), `source_to_raw_2` (Transfermarkt), `source_to_raw_3` (Champions League) |
| Formatting | `raw_to_formatted_1`, `raw_to_formatted_2`, `raw_to_formatted_3` |
| Usage | `produce_usage` (ROI join in Spark), `produce_usage_cl` (Champions League ROI), `ml_clustering` (K-Means) |
| Indexing | `index_to_elastic` |

### Machine learning

Clubs are clustered on `squad_value_m`, `roi` and `points` (standardized, K-Means with k=4). Each cluster is mapped to one of four readable profiles — **Efficaces, Dominants, Standards, Gaspilleurs** — through a one-to-one assignment to the four corners of the (budget, ROI) plane, so every label is used exactly once.

## Scope

- 5 leagues: Ligue 1, Premier League, La Liga, Bundesliga, Serie A
- Champions League (ROI based on the furthest stage reached)
- Seasons 2022, 2023 and 2024
- About 250 league club-seasons and 58 Champions League rows indexed

## Tech stack

Python · Apache Airflow 3 · PySpark · pandas · PyArrow · scikit-learn · Elasticsearch 9.0 · Kibana 9.0 · Docker Compose · PostgreSQL · Redis

## Getting started

**Prerequisites:** Docker Desktop (4 GB RAM, 2 CPUs minimum), Elasticsearch 9.0.0 and Kibana 9.0.0 running locally with security disabled, and a free [API-Football](https://dashboard.api-football.com/) key (100 requests/day).

```bash
git clone https://github.com/Nassim821/football-roi-pipeline.git
cd football-roi-pipeline

cp .env.example .env
# edit .env: set API_FOOTBALL_KEY and generate a FERNET_KEY with
#   python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

docker compose up airflow-init
docker compose up -d
```

1. Start Elasticsearch and Kibana on your machine (see [`docs/SETUP_WINDOWS.md`](docs/SETUP_WINDOWS.md) for the exact configuration; the Airflow containers reach Elasticsearch through `host.docker.internal:9200`).
2. Open Airflow at http://localhost:8080 (default login `airflow` / `airflow`), enable the `football_roi_pipeline` DAG and trigger it. A full run takes about 5 minutes.
3. In Kibana (http://localhost:5601), go to *Stack Management → Saved Objects → Import* and load [`kibana/dashboard.ndjson`](kibana/dashboard.ndjson).

A full run makes about 33 API-Football calls, so stay within two runs per day on the free plan.

## Repository structure

```
config/        Shared settings (paths, API, Elasticsearch)
dags/          Airflow DAG definition
jobs/
  ingestion/     source → raw
  formatting/    raw → formatted
  combination/   formatted → usage (ROI)
  ml/            K-Means clustering
  indexing/      usage → Elasticsearch
kibana/        Exported dashboard (8 visualizations)
docs/          Project report (French) and Windows setup guide
Dockerfile, docker-compose.yaml, requirements.txt
```

## Key findings

From the project report ([`docs/RAPPORT.md`](docs/RAPPORT.md), in French):

- Small-budget clubs (Clermont Foot, Rayo Vallecano, Heidenheim) top the ROI ranking, which inverts the classic league table.
- The Premier League has the lowest average ROI of the five leagues despite having the largest budgets.
- The budget-to-points relationship is non-linear and flattens out beyond roughly €600M of squad value.
- K-Means separates four club profiles with very different efficiency levels.

## Challenges solved

- **macOS file-lock deadlock** (`Errno 35`) when PyArrow wrote Parquet on a Docker bind mount, fixed with a named Docker volume.
- **Namespace-package conflict** in Airflow 3 for the `config` module, fixed with an `__init__.py`.
- **Elasticsearch index creation timeout** on a single node, fixed by setting `number_of_replicas` to 0.
- **Cluster labels collapsing** to two values, fixed with a one-to-one assignment.

## Limitations and next steps

- Daily batch only; no streaming (Kafka would be the next step for live match data).
- Single-node Elasticsearch, no high availability.
- No automated tests yet (pytest) and no pipeline monitoring (Prometheus / Grafana).

## Data and legal notes

Market values are scraped from Transfermarkt for educational purposes; check their terms of use before reusing the data. Match data comes from API-Football under its own terms. No data is stored in this repository.
