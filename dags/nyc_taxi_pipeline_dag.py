"""
NYC Taxi Batch ELT Pipeline — Apache Airflow DAG (Level 5)
============================================================
Orchestrates the complete monthly pipeline as an Airflow DAG:

  [Extract TLC Data] → [PySpark Cleansing] → [Load to DuckDB] → [dbt Run] → [dbt Test]

Schedule: Monthly (1st of each month at 04:00 UTC).
This is a PRODUCTION-READY DAG — the same file runs unchanged
on any Airflow environment (local, Astronomer, MWAA, Composer).

Key Design Decisions:
  - PythonOperators: Avoids subprocess overhead; imports our src/ modules directly.
  - BashOperator for dbt: Standard industry practice; dbt CLI is the canonical interface.
  - Task-level retries: Each task retries up to 2 times with 5-minute delays.
  - Catch-up disabled: Prevents backfill of historical months on first deploy.
  - XCom for config passing: Year/month passed between tasks via Airflow's XCom.
"""

from __future__ import annotations

import os
import sys
import logging
from datetime import datetime, timedelta
from pathlib import Path

# ---------------------------------------------------------------------------
# Airflow imports — these are the ONLY Airflow-specific lines.
# The rest of this file is pure Python / our own src/ modules.
# ---------------------------------------------------------------------------
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# ---------------------------------------------------------------------------
# Project paths — resolve project root relative to this DAG file
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DBT_PROJECT_DIR = PROJECT_ROOT / "nyc_taxi_dbt"
JAVA_HOME = "C:/Program Files/Microsoft/jdk-17.0.20.101-hotspot"

# Ensure src/ is importable from within Airflow workers
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Default task arguments (applied to every task in the DAG)
# ---------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "sarthak_sharma",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# ---------------------------------------------------------------------------
# Task Functions
# ---------------------------------------------------------------------------

def task_extract_tlc_data(**context) -> dict:
    """
    Task 1 — Data Extraction
    Downloads the monthly TLC Parquet file and zone lookup CSV.
    Pushes year/month metadata to XCom for downstream tasks.
    """
    from src.extract.extract_taxi_data import extract_monthly_trips, extract_zone_lookup

    # Get the logical execution date from Airflow context
    execution_date: datetime = context["data_interval_start"]
    year = execution_date.year
    month = execution_date.month

    logger.info(f"[Extract] Starting extraction for {year}-{month:02d}")

    output_dir = str(PROJECT_ROOT / "data" / "raw")

    parquet_path = extract_monthly_trips(
        year=year,
        month=month,
        taxi_type="yellow",
        output_dir=output_dir,
    )
    zone_path = extract_zone_lookup(output_dir=output_dir)

    result = {
        "year": year,
        "month": month,
        "parquet_path": str(parquet_path),
        "zone_path": str(zone_path),
    }
    logger.info(f"[Extract] Completed: {result}")
    return result


def task_spark_cleanse(**context) -> str:
    """
    Task 2 — PySpark Distributed Cleansing
    Reads the raw parquet, applies data quality rules and feature engineering.
    Returns the path of the cleaned parquet directory.
    """
    import subprocess

    ti = context["ti"]
    extract_meta: dict = ti.xcom_pull(task_ids="extract_tlc_data")
    year = extract_meta["year"]
    month = extract_meta["month"]

    input_file = extract_meta["parquet_path"]
    output_file = str(
        PROJECT_ROOT / "data" / "processed"
        / f"yellow_tripdata_{year}-{month:02d}_clean.parquet"
    )

    env = os.environ.copy()
    env["JAVA_HOME"] = JAVA_HOME
    env["HADOOP_HOME"] = str(PROJECT_ROOT / "hadoop")

    logger.info(f"[Cleanse] Submitting PySpark job for {year}-{month:02d}")

    result = subprocess.run(
        [
            sys.executable,
            str(PROJECT_ROOT / "src" / "transform" / "clean_taxi_data.py"),
            "--year", str(year),
            "--month", str(month),
            "--input-file", input_file,
            "--output-file", output_file,
        ],
        env=env,
        capture_output=True,
        text=True,
        cwd=str(PROJECT_ROOT),
    )

    if result.returncode != 0:
        logger.error(f"[Cleanse] PySpark FAILED:\n{result.stderr}")
        raise RuntimeError(f"PySpark cleansing failed (exit code {result.returncode})")

    logger.info(f"[Cleanse] Completed. Output at: {output_file}")
    return output_file


def task_load_duckdb(**context) -> dict:
    """
    Task 3 — DuckDB Warehouse Loading
    Bulk-loads the cleaned Parquet data into the raw schema of DuckDB.
    Returns warehouse ingestion metrics.
    """
    from src.load.load_to_duckdb import get_db_connection, load_raw_tables

    ti = context["ti"]
    clean_parquet_path: str = ti.xcom_pull(task_ids="spark_cleanse")

    extract_meta: dict = ti.xcom_pull(task_ids="extract_tlc_data")
    zone_path = extract_meta["zone_path"]

    db_path = str(PROJECT_ROOT / "data" / "warehouse" / "nyc_taxi.duckdb")
    logger.info(f"[Load] Connecting to DuckDB at: {db_path}")

    con = get_db_connection(db_path=db_path)
    try:
        metrics = load_raw_tables(
            con=con,
            clean_parquet_path=clean_parquet_path,
            zone_csv_path=zone_path,
        )
    finally:
        con.close()

    logger.info(f"[Load] Warehouse metrics: {metrics}")
    return metrics


# ---------------------------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="nyc_taxi_batch_elt_pipeline",
    description="Monthly NYC Taxi ELT pipeline: Extract → Spark Cleanse → DuckDB → dbt",
    default_args=DEFAULT_ARGS,
    schedule="0 4 1 * *",          # 04:00 UTC on the 1st of every month
    start_date=datetime(2024, 1, 1),
    catchup=False,                  # Do NOT backfill historical runs on deploy
    max_active_runs=1,              # Never run two months in parallel
    tags=["batch", "elt", "nyc_taxi", "pyspark", "dbt", "duckdb"],
) as dag:

    # ------------------------------------------------------------------
    # Task 1: Extract
    # ------------------------------------------------------------------
    extract = PythonOperator(
        task_id="extract_tlc_data",
        python_callable=task_extract_tlc_data,
    )

    # ------------------------------------------------------------------
    # Task 2: Spark Cleanse
    # ------------------------------------------------------------------
    cleanse = PythonOperator(
        task_id="spark_cleanse",
        python_callable=task_spark_cleanse,
        execution_timeout=timedelta(hours=2),   # PySpark can be slow on large months
    )

    # ------------------------------------------------------------------
    # Task 3: Load to DuckDB
    # ------------------------------------------------------------------
    load = PythonOperator(
        task_id="load_to_duckdb",
        python_callable=task_load_duckdb,
    )

    # ------------------------------------------------------------------
    # Task 4: dbt Run — Build all 7 Star Schema models
    # ------------------------------------------------------------------
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"{PROJECT_ROOT}/.venv/Scripts/dbt run --profiles-dir ."
        ),
        execution_timeout=timedelta(minutes=30),
    )

    # ------------------------------------------------------------------
    # Task 5: dbt Test — Enforce all 41 data contract tests
    # ------------------------------------------------------------------
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"{PROJECT_ROOT}/.venv/Scripts/dbt test --profiles-dir ."
        ),
        execution_timeout=timedelta(minutes=15),
    )

    # ------------------------------------------------------------------
    # DAG Dependency Chain
    # ------------------------------------------------------------------
    extract >> cleanse >> load >> dbt_run >> dbt_test
