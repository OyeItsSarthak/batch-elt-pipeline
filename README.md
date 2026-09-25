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
[3. Cloud Data Warehouse]
      Snowflake (Raw analytical ingestion via COPY INTO staging)
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
* **Data Warehouse**: Snowflake
* **Data Transformation & Modeling**: dbt (Data Build Tool)
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
│   └── processed/        # PySpark cleaned parquet files
├── dbt_project/          # dbt models, tests, and documentation
├── src/                  # Pipeline source scripts
│   ├── extract/          # Extraction modules
│   ├── transform/        # PySpark transformation jobs
│   └── load/             # Snowflake loader modules
├── .gitignore            # Git exclusion rules
├── README.md             # Project documentation & runbook
└── requirements.txt      # Project dependencies
```

---

## 🚦 Roadmap & Implementation Levels

- [x] **Level 0: Setup & Environment** — Git, repository structure, virtual environment, and security exclusions.
- [x] **Level 1: Data Extraction** — Automated ingestion of NYC TLC monthly Parquet datasets into the landing zone.
- [x] **Level 2: Distributed Cleansing** — PySpark job for filtering anomalous records, deduplication, and schema validation.
- [ ] **Level 3: Warehouse Loading** — Secure Snowflake bulk-loading using staging and `COPY INTO`.
- [ ] **Level 4: Dimensional Modeling** — dbt project with Star Schema (`fct_trips`, `dim_zones`) and data contract tests.
- [ ] **Level 5: Orchestration** — Apache Airflow DAG for scheduled, fault-tolerant execution.
- [ ] **Level 6: Testing & CI/CD** — GitHub Actions integration running automated validation on every commit.
- [ ] **Level 7: Production Runbook & Documentation** — System design decisions, trade-offs, and verification steps.
