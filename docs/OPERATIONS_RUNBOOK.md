# Production Operations Runbook & User Guide

This runbook provides step-by-step instructions for operating, maintaining, backfilling, and querying the **NYC Taxi Batch ELT Data Platform**.

---

## 1. Quickstart & Local Setup

### Prerequisites
* **Operating System:** Windows 10/11, macOS, or Linux (Ubuntu 20.04+).
* **Python:** 3.11, 3.12, or 3.13.
* **Java:** OpenJDK 11 or 17 (required for Apache Spark engine).
* **Git:** Installed and configured.

### Step 1: Clone Repository & Create Virtual Environment
```bash
git clone https://github.com/OyeItsSarthak/batch-elt-pipeline.git
cd batch-elt-pipeline

# Create virtual environment
python -m venv .venv

# Activate environment
# On Windows (PowerShell):
.venv\Scripts\Activate.ps1
# On macOS / Linux:
source .venv/bin/activate
```

### Step 2: Install Dependencies
```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### Step 3: Run the End-to-End Pipeline
Execute the full monthly pipeline (extract $\rightarrow$ clean $\rightarrow$ load $\rightarrow$ dbt model $\rightarrow$ dbt test):
```bash
python run_pipeline.py --year 2024 --month 1
```

---

## 2. Command Line Interface (CLI) Capabilities

The local pipeline runner ([run_pipeline.py](file:///c:/Users/sarth/OneDrive/Desktop/Batch%20ELT%20Pipeline/run_pipeline.py)) provides flags to skip stages during development or debugging:

| Command | Purpose |
|---|---|
| `python run_pipeline.py --year 2024 --month 1` | Full end-to-end execution for Jan 2024. |
| `python run_pipeline.py --year 2024 --month 1 --skip-extract` | Skips downloading if raw Parquet is already cached in `data/raw/`. |
| `python run_pipeline.py --year 2024 --month 1 --skip-spark` | Skips PySpark processing if cleaned Parquet is already in `data/processed/`. |
| `python run_pipeline.py --year 2024 --month 1 --skip-dbt` | Skips dbt build/test stages. |

---

## 3. Airflow Orchestration & Production Deployment

The pipeline is packaged as a production-grade Apache Airflow DAG: [dags/nyc_taxi_pipeline_dag.py](file:///c:/Users/sarth/OneDrive/Desktop/Batch%20ELT%20Pipeline/dags/nyc_taxi_pipeline_dag.py).

### DAG Architecture
* **DAG ID:** `nyc_taxi_monthly_elt`
* **Schedule:** `0 4 1 * *` (04:00 UTC on the 1st of every month).
* **Catchup:** `False` (avoids accidental historical runs on initial activation).
* **Retries:** 2 retries with 5-minute exponential backoff.
* **Execution Flow:**
  ```text
  extract_taxi_data >> clean_taxi_data >> load_to_duckdb >> dbt_deps >> dbt_run >> dbt_test
  ```

### Deploying to an Existing Airflow Cluster
1. Copy the DAG file to your Airflow environment:
   ```bash
   cp dags/nyc_taxi_pipeline_dag.py $AIRFLOW_HOME/dags/
   ```
2. Set the environment variables in your Airflow worker or `.env`:
   ```bash
   PROJECT_ROOT=/path/to/batch-elt-pipeline
   DUCKDB_DATABASE_PATH=/path/to/batch-elt-pipeline/data/warehouse/nyc_taxi.duckdb
   ```
3. Enable the DAG in the Airflow Web UI.

---

## 4. Analytical Query Playbook

You can query the DuckDB warehouse directly using Python, the DuckDB CLI, or DBeaver / any SQL client.

### Connecting via Python
```python
import duckdb

con = duckdb.connect("data/warehouse/nyc_taxi.duckdb", read_only=True)
```

### Query 1: Top 5 Boroughs by Revenue & Average Tip
```sql
SELECT 
    z.borough,
    COUNT(*) AS total_trips,
    ROUND(SUM(f.total_amount), 2) AS total_revenue_usd,
    ROUND(AVG(f.tip_percentage), 2) AS avg_tip_pct
FROM marts.fct_trips f
JOIN marts.dim_zones z ON f.pickup_location_id = z.location_id
GROUP BY z.borough
ORDER BY total_revenue_usd DESC
LIMIT 5;
```

### Query 2: Peak Demand by Time of Day
```sql
SELECT 
    time_of_day_segment,
    COUNT(*) AS trip_count,
    ROUND(AVG(trip_distance_miles), 2) AS avg_distance_miles,
    ROUND(AVG(fare_amount), 2) AS avg_fare_usd
FROM marts.fct_trips
GROUP BY time_of_day_segment
ORDER BY trip_count DESC;
```

### Query 3: Weekend vs. Weekday Trip Dynamics
```sql
SELECT 
    d.is_weekend,
    COUNT(*) AS trip_count,
    ROUND(AVG(f.trip_distance_miles), 2) AS avg_distance,
    ROUND(AVG(f.total_amount), 2) AS avg_total_fare
FROM marts.fct_trips f
JOIN marts.dim_date d ON f.pickup_date = d.date_day
GROUP BY d.is_weekend;
```

---

## 5. Maintenance, Backfilling & Disaster Recovery

### Historical Backfills
To backfill multiple historical months, loop through target dates:
```bash
# Example: Backfill Q1 2024
python run_pipeline.py --year 2024 --month 1
python run_pipeline.py --year 2024 --month 2
python run_pipeline.py --year 2024 --month 3
```

### Complete Warehouse Rebuild
If the database file is corrupted or schema alterations require a full refresh:
1. Remove the existing warehouse file:
   ```bash
   rm data/warehouse/nyc_taxi.duckdb*
   ```
2. Re-run ingestion and dbt:
   ```bash
   python run_pipeline.py --year 2024 --month 1 --skip-extract --skip-spark
   ```

---

## 6. Troubleshooting & Common Issues

### Issue 1: Windows PySpark `winutils.exe` or `HADOOP_HOME` Warning
* **Symptom:** `java.io.IOException: Could not locate executable null\bin\winutils.exe in the Hadoop binaries`.
* **Fix:** The repository includes pre-packaged Windows Hadoop binaries under `hadoop/bin/winutils.exe`. Ensure `HADOOP_HOME` is set to the project `hadoop/` directory (handled automatically by `clean_taxi_data.py` and `run_pipeline.py`).

### Issue 2: DuckDB Database Lock (`Resource temporarily unavailable`)
* **Symptom:** `duckdb.IOException: Could not set lock on file ... Database is locked by another process`.
* **Cause:** DuckDB is an embedded database that permits only one read-write process at a time.
* **Fix:** Close active Python shells, Jupyter notebooks, or SQL clients connected to `data/warehouse/nyc_taxi.duckdb` in write mode before running the pipeline or dbt.

### Issue 3: dbt Package Resolution Error
* **Symptom:** `dbt found packages.yml but packages are not installed`.
* **Fix:** Run `dbt deps --profiles-dir .` inside the `nyc_taxi_dbt/` folder.
