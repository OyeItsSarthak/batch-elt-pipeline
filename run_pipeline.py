"""
Local Pipeline Runner (Level 5 — Development Utility)
======================================================
Executes the full ELT pipeline end-to-end without an Airflow server.
This is how you test and demo the pipeline locally.

Usage:
    python run_pipeline.py                          # Run for current month
    python run_pipeline.py --year 2024 --month 1   # Run for specific month

This script mirrors the exact Airflow task sequence:
  Task 1: Extract  →  Task 2: Spark Cleanse  →  Task 3: DuckDB Load  →  Task 4: dbt Run  →  Task 5: dbt Test
"""

import os
import sys
import logging
import argparse
import subprocess
from pathlib import Path
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("pipeline_runner")

PROJECT_ROOT = Path(__file__).resolve().parent
DBT_PROJECT_DIR = PROJECT_ROOT / "nyc_taxi_dbt"
DBT_EXE = PROJECT_ROOT / ".venv" / "Scripts" / "dbt.exe"
PYTHON_EXE = PROJECT_ROOT / ".venv" / "Scripts" / "python.exe"
JAVA_HOME = "C:/Program Files/Microsoft/jdk-17.0.20.101-hotspot"

# Ensure src/ is importable
sys.path.insert(0, str(PROJECT_ROOT))


def run_step(step_name: str, func_or_cmd, is_shell: bool = False):
    """Runs a pipeline step, logs timing, and raises on failure."""
    import time
    logger.info(f"{'='*60}")
    logger.info(f"▶  STEP: {step_name}")
    logger.info(f"{'='*60}")
    start = time.time()
    try:
        if is_shell:
            result = subprocess.run(func_or_cmd, shell=True, capture_output=False, cwd=str(DBT_PROJECT_DIR))
            if result.returncode != 0:
                raise RuntimeError(f"Shell command failed with exit code {result.returncode}")
        else:
            func_or_cmd()
        elapsed = time.time() - start
        logger.info(f"✅ STEP PASSED: {step_name} ({elapsed:.1f}s)\n")
    except Exception as e:
        elapsed = time.time() - start
        logger.error(f"❌ STEP FAILED: {step_name} ({elapsed:.1f}s)\n   Error: {e}")
        raise


def main():
    parser = argparse.ArgumentParser(description="NYC Taxi ELT Local Pipeline Runner")
    parser.add_argument("--year", type=int, default=datetime.now().year)
    parser.add_argument("--month", type=int, default=datetime.now().month)
    parser.add_argument("--skip-extract", action="store_true", help="Skip download if already landed")
    parser.add_argument("--skip-spark", action="store_true", help="Skip PySpark if already processed")
    args = parser.parse_args()

    year, month = args.year, args.month
    logger.info(f"🚀 NYC Taxi ELT Pipeline — {year}-{month:02d}")
    logger.info(f"   Project Root : {PROJECT_ROOT}")
    logger.info(f"   Java Home    : {JAVA_HOME}\n")

    raw_dir = PROJECT_ROOT / "data" / "raw"
    parquet_path = raw_dir / f"yellow_tripdata_{year}-{month:02d}.parquet"
    zone_path = raw_dir / "taxi_zone_lookup.csv"
    clean_path = PROJECT_ROOT / "data" / "processed" / f"yellow_tripdata_{year}-{month:02d}_clean.parquet"
    db_path = PROJECT_ROOT / "data" / "warehouse" / "nyc_taxi.duckdb"

    # ----------------------------------------------------------------
    # Task 1: Extract
    # ----------------------------------------------------------------
    if not args.skip_extract:
        def step_extract():
            from src.extract.extract_taxi_data import extract_monthly_trips, extract_zone_lookup
            extract_monthly_trips(year=year, month=month, taxi_type="yellow", output_dir=str(raw_dir))
            extract_zone_lookup(output_dir=str(raw_dir))

        run_step("Task 1 — Extract TLC Data", step_extract)
    else:
        logger.info("⏭  Skipping Task 1 (--skip-extract)\n")

    # ----------------------------------------------------------------
    # Task 2: PySpark Cleanse
    # ----------------------------------------------------------------
    if not args.skip_spark:
        def step_cleanse():
            env = os.environ.copy()
            env["JAVA_HOME"] = JAVA_HOME
            env["HADOOP_HOME"] = str(PROJECT_ROOT / "hadoop")
            result = subprocess.run(
                [
                    str(PYTHON_EXE),
                    str(PROJECT_ROOT / "src" / "transform" / "clean_taxi_data.py"),
                    "--year", str(year),
                    "--month", str(month),
                    "--input-file", str(parquet_path),
                    "--output-file", str(clean_path),
                ],
                env=env,
                cwd=str(PROJECT_ROOT),
            )
            if result.returncode != 0:
                raise RuntimeError("PySpark job failed")

        run_step("Task 2 — PySpark Distributed Cleansing", step_cleanse)
    else:
        logger.info("⏭  Skipping Task 2 (--skip-spark)\n")

    # ----------------------------------------------------------------
    # Task 3: Load to DuckDB
    # ----------------------------------------------------------------
    def step_load():
        from src.load.load_to_duckdb import get_db_connection, load_raw_tables
        con = get_db_connection(db_path=str(db_path))
        try:
            metrics = load_raw_tables(
                con=con,
                clean_parquet_path=str(clean_path),
                zone_csv_path=str(zone_path),
            )
            logger.info(f"   Warehouse metrics: {metrics}")
        finally:
            con.close()

    run_step("Task 3 — Load to DuckDB Warehouse", step_load)

    # ----------------------------------------------------------------
    # Task 4: dbt Run
    # ----------------------------------------------------------------
    run_step(
        "Task 4 — dbt Run (Build Star Schema Models)",
        f'"{DBT_EXE}" run --profiles-dir .',
        is_shell=True,
    )

    # ----------------------------------------------------------------
    # Task 5: dbt Test
    # ----------------------------------------------------------------
    run_step(
        "Task 5 — dbt Test (Enforce Data Contracts)",
        f'"{DBT_EXE}" test --profiles-dir .',
        is_shell=True,
    )

    logger.info("=" * 60)
    logger.info(f"🏁 PIPELINE COMPLETE — {year}-{month:02d}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
