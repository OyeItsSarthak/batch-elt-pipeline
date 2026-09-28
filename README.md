# NYC Taxi Batch ELT Data Platform

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
- [ ] **Level 5: Orchestration** — Apache Airflow DAG for scheduled, fault-tolerant execution.
- [ ] **Level 6: Testing & CI/CD** — GitHub Actions integration running automated validation on every commit.
- [ ] **Level 7: Production Runbook & Documentation** — System design decisions, trade-offs, and verification steps.

---

## Level 4 — dbt

After the warehouse `raw` schema is loaded:

```bash
cd nyc_taxi_dbt
dbt deps --profiles-dir .
dbt run --profiles-dir .
dbt test --profiles-dir .
```

Models: `staging` → `intermediate` → `marts`. Analysts query `marts.fct_trips` joined to `marts.dim_zones` and `marts.dim_date`. Full notes are in `nyc_taxi_dbt/README.md`.
