# NYC Taxi Batch ELT Data Platform

[![CI / CD Pipeline](https://github.com/OyeItsSarthak/batch-elt-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/OyeItsSarthak/batch-elt-pipeline/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/Python-3.11%20%7C%203.12%20%7C%203.13-blue?logo=python)
![DuckDB](https://img.shields.io/badge/DuckDB-0.10%2B-yellow?logo=duckdb)
![dbt](https://img.shields.io/badge/dbt-1.8%2B-orange?logo=dbt)
![Apache Airflow](https://img.shields.io/badge/Airflow-2.8%2B-017CEE?logo=apache-airflow)
![PySpark](https://img.shields.io/badge/PySpark-3.5%2B-E25A1C?logo=apache-spark)

An end-to-end, production-grade batch ELT pipeline orchestrating millions of NYC Taxi & Limousine Commission (TLC) trip records into analytical data models using modern data engineering practices.

---

## 🏗️ Architecture Overview

```
[NYC TLC Public Dataset]
           │
           ▼
[1. Extraction Layer]
      Python (Requests / Parquet Landing)
           │
           ▼
[2. Distributed Processing & Cleansing]
      PySpark (Schema validation, anomaly filtering, data quality)
           │
           ▼
[3. Analytical Data Warehouse]
      DuckDB (High-performance columnar OLAP engine, zero-copy ingestion)
           │
           ▼
[4. Data Modeling & Transformations]
      dbt (Dimensional Modeling: Staging → Intermediate → Marts [Star Schema])
           │
           ▼
[5. Workflow Orchestration]
      Apache Airflow (Scheduled DAGs, automated retries, dependency management)
           │
           ▼
[6. CI/CD & Automated Testing]
      GitHub Actions (Automated dbt tests, regression checks on PRs)
```

---

## 🛠️ Tech Stack

* **Language**: Python 3.13+
* **Data Processing**: PySpark
* **Data Warehouse (OLAP)**: DuckDB
* **Data Transformation & Modeling**: dbt (Data Build Tool - `dbt-duckdb`)
* **Orchestration**: Apache Airflow
* **CI/CD**: GitHub Actions
* **Version Control**: Git & GitHub

---

## 📂 Project Directory Structure

```text
Batch ELT Pipeline/
├── .github/              # CI/CD Workflows
├── config/               # Pipeline configurations & environment templates
├── dags/                 # Apache Airflow DAGs
├── data/                 # Local data landing zones (ignored in git)
│   ├── raw/              # Raw ingested parquet files
│   ├── processed/        # PySpark cleaned parquet files
│   └── warehouse/        # DuckDB columnar analytical database
├── nyc_taxi_dbt/         # dbt models, tests, and documentation
├── src/                  # Pipeline source scripts
│   ├── extract/          # Extraction modules
│   ├── transform/        # PySpark transformation jobs
│   └── load/             # DuckDB analytical loader modules
├── .gitignore            # Git exclusion rules
├── README.md             # Project documentation & runbook
└── requirements.txt      # Project dependencies
```

---

## 🚦 Roadmap & Implementation Levels

- [x] **Level 0: Setup & Environment** — Git, repository structure, virtual environment, and security exclusions.
- [x] **Level 1: Data Extraction** — Automated ingestion of NYC TLC monthly Parquet datasets into the landing zone.
- [x] **Level 2: Distributed Cleansing** — PySpark job for filtering anomalous records, deduplication, and schema validation.
- [x] **Level 3: Warehouse Loading** — High-performance DuckDB columnar ingestion and raw schema initialization.
- [x] **Level 4: Dimensional Modeling** — dbt project with star schema (`fct_trips`, `dim_zones`, `dim_date`) and data tests.
- [x] **Level 5: Orchestration** — Airflow DAG orchestrating the full monthly pipeline with PythonOperators, BashOperators, retries, and XCom config passing.
- [x] **Level 6: Testing & CI/CD** — GitHub Actions integration running automated validation on every commit (Ruff linter, Pytest unit tests, and 40+ dbt data contract quality tests).
- [x] **Level 7: Production Runbook & Documentation** — System design decisions, trade-offs, and verification steps.

---

## ⚡ Quickstart: Run Locally in 3 Steps

### 1. Setup Environment
```bash
git clone https://github.com/OyeItsSarthak/batch-elt-pipeline.git
cd batch-elt-pipeline
python -m venv .venv
# Activate virtual environment:
# Windows (PowerShell): .venv\Scripts\Activate.ps1 | Linux/macOS: source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

### 2. Execute Monthly Pipeline
Execute the full automated workflow (extract → PySpark clean → DuckDB warehouse load → dbt model → dbt test):
```bash
python run_pipeline.py --year 2024 --month 1
```

### 3. Verify Data Quality & Query Insights
Validate all 40 data quality contracts and query analytical marts:
```bash
cd nyc_taxi_dbt
dbt test --profiles-dir .
```

---

## 📊 Analytical Insights & Sample Queries

The platform loads over **2.85 million clean trips** per month into a Kimball Star Schema. Connect with Python or any SQL client:

```python
import duckdb
con = duckdb.connect("data/warehouse/nyc_taxi.duckdb", read_only=True)
```

### Top 5 Boroughs by Revenue & Average Tip
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
*Sample Result (January 2024):*
| Borough | Total Trips | Total Revenue ($) | Avg Tip % |
|---|---|---|---|
| **Manhattan** | 2,566,063 | $58,355,041.64 | 21.56% |
| **Queens** | 255,384 | $18,495,230.46 | 21.89% |
| **Brooklyn** | 22,049 | $730,555.79 | 6.89% |
| **Unknown** | 9,479 | $255,615.15 | 20.36% |
| **Bronx** | 5,720 | $202,727.61 | 0.71% |

### Peak Demand by Time of Day
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
*Sample Result:*
| Time of Day Segment | Trips | Avg Distance (mi) | Avg Fare ($) |
|---|---|---|---|
| **Midday (10am–4pm)** | 966,026 | 3.14 | $18.40 |
| **Evening Rush (4pm–8pm)** | 767,782 | 3.12 | $18.00 |
| **Night (8pm–12am)** | 552,292 | 3.60 | $18.76 |
| **Morning Rush (6am–10am)** | 318,870 | 3.19 | $18.08 |
| **Late Night / Early Morning (12am–6am)** | 254,265 | 3.97 | $19.85 |

---

## 🧪 Testing & CI/CD Pipeline (Level 6)

Every push or pull request to `main` automatically triggers an end-to-end continuous integration pipeline in **GitHub Actions**:

1. **Static Analysis & Linting**: Enforces strict code standards, imports formatting, and syntax validation via `ruff`.
2. **Unit Testing (`pytest`)**:
   - Extraction idempotency & network stream handling ([tests/test_extract.py](tests/test_extract.py))
   - DuckDB database initialization & schema contracts ([tests/test_load.py](tests/test_load.py))
   - Cleansing logic, metrics schemas, and numerical edge cases ([tests/test_transform.py](tests/test_transform.py))
3. **CI Warehouse Seeding**: Generates a lightweight, deterministic synthetic DuckDB warehouse via [scripts/ci_prepare_test_db.py](scripts/ci_prepare_test_db.py).
4. **dbt Compilation & Build**: Compiles and executes all 7 models across `staging`, `intermediate`, and `marts`.
5. **dbt Contract & Quality Tests**: Executes 40 data quality tests validating foreign key referential integrity, uniqueness, non-null constraints, and business domain assertions.

---

## 📚 Technical Documentation & Runbooks (Level 7)

For deep-dive architectural decisions, trade-offs, and operational playbooks, explore our documentation suite:

* 📖 **[System Architecture & Technical Design Document (ADRs)](docs/ARCHITECTURE_AND_DESIGN.md)** — Architectural decision records (DuckDB vs Snowflake, PySpark for out-of-core scaling, dbt modeling contracts, surrogate key hashing, and partition strategies).
* 🛠️ **[Production Operations Runbook](docs/OPERATIONS_RUNBOOK.md)** — Step-by-step operations guide, Airflow cluster deployment, historical backfilling, disaster recovery, and troubleshooting FAQs.
* 📐 **[dbt Modeling & Data Dictionary](nyc_taxi_dbt/README.md)** — Lineage graph, schema documentation, and Kimball modeling patterns.




