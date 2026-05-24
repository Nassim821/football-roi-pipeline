"""
Airflow DAG — Football ROI Pipeline
source_to_raw → raw_to_formatted → produce_usage → index_to_elastic
Schedule : quotidien à 6h UTC
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

default_args = {
    "owner":            "airflow",
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="football_roi_pipeline",
    default_args=default_args,
    description="Performance sportive vs investissements transferts",
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["football", "roi", "datalake"],
) as dag:

    # ── Ingestion ────────────────────────────────────────────────────────────
    def run_source_to_raw_1():
        from ingestion.ingest_football import run
        run()

    def run_source_to_raw_2():
        from ingestion.ingest_transfermarkt import run
        run()

    def run_source_to_raw_3():
        from ingestion.ingest_cl import run
        run()

    # ── Formatting ───────────────────────────────────────────────────────────
    def run_raw_to_formatted_1():
        from formatting.format_football import run
        run()

    def run_raw_to_formatted_2():
        from formatting.format_transfermarkt import run
        run()

    def run_raw_to_formatted_3():
        from formatting.format_cl import run
        run()

    # ── Combination ──────────────────────────────────────────────────────────
    def run_produce_usage_roi():
        from combination.combine_roi import run
        run()

    def run_produce_usage_cl():
        from combination.combine_cl_roi import run
        run()

    # ── ML ───────────────────────────────────────────────────────────────────
    def run_ml_clustering():
        from ml.ml_clustering import run
        run()

    # ── Indexing ─────────────────────────────────────────────────────────────
    def run_index_to_elastic():
        from indexing.index_elastic import run
        run()

    # ── Tasks ────────────────────────────────────────────────────────────────
    source_to_raw_1 = PythonOperator(
        task_id="source_to_raw_1",
        python_callable=run_source_to_raw_1,
    )

    source_to_raw_2 = PythonOperator(
        task_id="source_to_raw_2",
        python_callable=run_source_to_raw_2,
    )

    source_to_raw_3 = PythonOperator(
        task_id="source_to_raw_3",
        python_callable=run_source_to_raw_3,
    )

    raw_to_formatted_1 = PythonOperator(
        task_id="raw_to_formatted_1",
        python_callable=run_raw_to_formatted_1,
    )

    raw_to_formatted_2 = PythonOperator(
        task_id="raw_to_formatted_2",
        python_callable=run_raw_to_formatted_2,
    )

    raw_to_formatted_3 = PythonOperator(
        task_id="raw_to_formatted_3",
        python_callable=run_raw_to_formatted_3,
    )

    produce_usage_roi = PythonOperator(
        task_id="produce_usage",
        python_callable=run_produce_usage_roi,
    )

    produce_usage_cl = PythonOperator(
        task_id="produce_usage_cl",
        python_callable=run_produce_usage_cl,
    )

    ml_clustering = PythonOperator(
        task_id="ml_clustering",
        python_callable=run_ml_clustering,
    )

    index_to_elastic = PythonOperator(
        task_id="index_to_elastic",
        python_callable=run_index_to_elastic,
    )

    # ── Dependencies (matching prof's diagram) ───────────────────────────────
    #
    #  source_to_raw_1 → raw_to_formatted_1 ─┐
    #  source_to_raw_2 → raw_to_formatted_2 ─┤→ produce_usage ─┐
    #  source_to_raw_3 → raw_to_formatted_3 ─┘                 │
    #                                          produce_usage_cl ─┤→ index_to_elastic
    #
    # Sources en parallèle
    [source_to_raw_1, source_to_raw_2, source_to_raw_3] >> raw_to_formatted_1

    # Tous les formats strictement séquentiels (évite deadlock macOS Docker)
    raw_to_formatted_1 >> raw_to_formatted_2 >> raw_to_formatted_3

    raw_to_formatted_2 >> produce_usage_roi
    raw_to_formatted_3 >> produce_usage_cl

    # ML puis indexation
    produce_usage_roi >> ml_clustering
    [ml_clustering, produce_usage_cl] >> index_to_elastic
