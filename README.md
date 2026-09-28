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
- [ ] **Level 7: Production Runbook & Documentation** — System design decisions, trade-offs, and verification steps.

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

## 🚀 Running dbt Locally

After the warehouse `raw` schema is loaded:

```bash
cd nyc_taxi_dbt
dbt deps --profiles-dir .
dbt run --profiles-dir .
dbt test --profiles-dir .
```

Models: `staging` → `intermediate` → `marts`. Analysts query `marts.fct_trips` joined to `marts.dim_zones` and `marts.dim_date`. Full notes are in `nyc_taxi_dbt/README.md`.

