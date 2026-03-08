# 🍎 apple-retail-etl-pipeline

![Pipeline Status](https://img.shields.io/badge/pipeline-passing-brightgreen)
![Python](https://img.shields.io/badge/Python-3.11-blue)
![Pandas](https://img.shields.io/badge/Pandas-2.0-150458)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791)
![Airflow](https://img.shields.io/badge/Airflow-2.8-017CEE)
![Snowflake](https://img.shields.io/badge/Snowflake-ready-29B5E8)
![License](https://img.shields.io/badge/license-MIT-green)

---

## 📌 Project Overview

A **production-grade ETL (Extract, Transform, Load) pipeline** built entirely in Python for Apple's retail sales data. Raw CSV data from Kaggle is extracted, transformed using **Pandas**, validated, and loaded into a **Star Schema** reporting warehouse on **PostgreSQL** (local) with a migration path to **Snowflake** (cloud).

The pipeline is fully modular, unit tested with **pytest**, orchestrated via **Apache Airflow**, version controlled in **Git**, and deployed via **CI/CD using GitHub Actions**.

---

## 🎯 Business Problem

Apple operates across **40+ countries** with thousands of product variants. Business stakeholders need reliable answers to:

- Which regions and stores drive the most revenue?
- Which products have the highest warranty claim rates?
- How is sales performance trending month-over-month?
- Which product categories are growing year-over-year?

Raw transactional data spread across multiple CSV files cannot answer these questions directly. This pipeline consolidates, cleans, and models that data into an analytics-ready **Star Schema** reporting layer.

---

## 🏗️ Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                     SOURCE LAYER                             │
│          Kaggle — Apple Retail Sales Dataset (CSV)           │
│    sales / products / stores / category / warranty           │
└──────────────────────┬───────────────────────────────────────┘
                       │
                       │  EXTRACT
                       │  extract.py — read CSVs into Pandas DataFrames
                       ↓
┌──────────────────────────────────────────────────────────────┐
│                  PYTHON TRANSFORM LAYER                      │
│                                                              │
│   transform.py    — clean, cast, standardise                 │
│   transform.py    — join all tables, derive fields           │
│   scd_type2.py    — historical product tracking              │
│   validate.py     — data quality checks                      │
│                                                              │
└──────────────────────┬───────────────────────────────────────┘
                       │
                       │  LOAD
                       │  load.py — write to PostgreSQL / Snowflake
                       ↓
┌──────────────────────────────────────────────────────────────┐
│              POSTGRESQL / SNOWFLAKE WAREHOUSE                │
│                                                              │
│   Star Schema:                                               │
│   fact_sales + dim_product (SCD Type 2) + dim_store          │
│   dim_date + dim_category                                    │
│                                                              │
└──────────────────────┬───────────────────────────────────────┘
                       │
                       │  Airflow DAG — daily scheduled runs
                       ↓
┌──────────────────────────────────────────────────────────────┐
│                   REPORTING LAYER                            │
│     Sales / Product / Store / Warranty Analytics             │
└──────────────────────────────────────────────────────────────┘
                       │
                       │  GitHub Actions CI/CD
                       ↓
              PR → lint → pytest → deploy
```

---

## 🛠️ Tech Stack

| Layer | Tool | Version |
|---|---|---|
| Language | Python | 3.11 |
| Extract | Pandas | 2.0 |
| Transform | Pandas + NumPy | 2.0 / 1.24 |
| Load | SQLAlchemy + psycopg2 | 2.0 |
| Warehouse (local) | PostgreSQL | 15 |
| Warehouse (cloud) | Snowflake | — |
| Orchestration | Apache Airflow | 2.8 |
| Testing | pytest | 7.x |
| Version Control | Git + GitHub | — |
| CI/CD | GitHub Actions | — |

---

## 📁 Repository Structure

```
apple-retail-etl-pipeline/
│
├── README.md
├── requirements.txt                    ← All Python dependencies
├── .github/
│   └── workflows/
│       └── etl_ci.yml                  ← GitHub Actions CI/CD
│
├── data/                               ← Kaggle CSV source files
│   ├── sales.csv
│   ├── products.csv
│   ├── stores.csv
│   ├── category.csv
│   └── warranty.csv
│
├── config/
│   ├── database.py                     ← DB connection (swap for Snowflake)
│   └── settings.py                     ← Pipeline config and constants
│
├── etl/
│   ├── __init__.py
│   ├── extract.py                      ← Read CSVs into DataFrames
│   ├── transform.py                    ← Clean, join, enrich, classify
│   ├── scd_type2.py                    ← SCD Type 2 product history logic
│   ├── validate.py                     ← Data quality checks
│   ├── load.py                         ← Write to PostgreSQL / Snowflake
│   └── pipeline.py                     ← Main orchestrator — runs full ETL
│
├── airflow/
│   └── dags/
│       └── apple_etl_pipeline.py       ← Airflow DAG definition
│
├── tests/
│   ├── test_extract.py                 ← Tests for extract functions
│   ├── test_transform.py               ← Tests for transform functions
│   ├── test_validate.py                ← Tests for validation logic
│   └── test_scd.py                     ← Tests for SCD Type 2 logic
│
└── docs/
    ├── architecture.md
    └── data_dictionary.md
```

---

## 📊 Data Model — Star Schema

```
                    ┌─────────────┐
                    │  dim_date   │
                    └──────┬──────┘
                           │
┌──────────────┐    ┌──────┴──────┐    ┌─────────────────┐
│  dim_store   ├────┤  fact_sales ├────┤   dim_product   │
└──────────────┘    └──────┬──────┘    │  (SCD Type 2)   │
                           │           └─────────────────┘
                    ┌──────┴──────┐
                    │ dim_category│
                    └─────────────┘
```

### Tables

| Table | Type | Description |
|---|---|---|
| `fact_sales` | Fact | One row per sales transaction — incremental load |
| `dim_product` | Dimension | Product attributes — SCD Type 2 historical tracking |
| `dim_store` | Dimension | Store details by country and region |
| `dim_date` | Dimension | Calendar spine — date, month, quarter, year |
| `dim_category` | Dimension | Product category hierarchy |

---

## ⚙️ ETL Pipeline — What Each Stage Does

### Extract
- Reads all CSV files into Pandas DataFrames
- Logs row counts per source file
- No transformation at this stage — raw data only

### Transform
- **Clean** — cast data types, trim whitespace, standardise case
- **Deduplicate** — remove duplicate records on primary keys
- **Join** — merge sales with products, stores, and category
- **Derive** — calculate gross_revenue, discount_amount, order_tier
- **Classify** — apply business rules (High / Mid / Low Value orders)

### Validate
- Null checks on critical columns
- Duplicate primary key detection
- Business rule enforcement (no negative revenue)
- Row count reconciliation between source and target
- Logs pass/fail per table — pipeline halts on failure

### Load
- **Fact table** — incremental append, only new records by sale_date
- **Dimension tables** — full refresh
- **SCD Type 2** — detect product attribute changes, expire old records, insert new versions

---

## 🔄 Incremental Loading

The fact table uses watermark-based incremental loading — only records newer than the last loaded date are processed on each daily run:

```python
def load_fact_incremental(df, engine):
    # Get last loaded date
    last_date = pd.read_sql(
        'SELECT MAX(sale_date) FROM fact_sales', engine
    )['max'][0]

    # Filter only new records
    new_records = df[df['sale_date'] > pd.to_datetime(last_date)]

    # Append to fact table
    new_records.to_sql('fact_sales', engine, if_exists='append', index=False)
```

---

## 📸 SCD Type 2 — Product Dimension

Product attributes change over time — pricing, specs, category assignments. SCD Type 2 preserves every version so historical sales always report against the product profile that existed at the time of sale.

```python
# On each pipeline run:
# 1. Detect which products have changed attributes
# 2. Expire old records — set valid_to = now, is_current = False
# 3. Insert new records — set valid_from = now, is_current = True
```

Columns managed automatically:

| Column | Description |
|---|---|
| `valid_from` | Timestamp when this version became active |
| `valid_to` | Timestamp when superseded — NULL means current |
| `is_current` | Boolean flag — TRUE for active record |

---

## ✅ Data Quality Validation

Validation runs after transform and before load. Pipeline halts if any check fails:

```python
def validate(df, table_name):
    # 1. Null checks on critical columns
    # 2. Duplicate primary key check
    # 3. Negative revenue check
    # 4. Row count check vs expected range
    # Returns True (pass) or raises AssertionError (fail)
```

| Check | Tables | Action on Fail |
|---|---|---|
| Null primary key | All | Halt pipeline |
| Duplicate records | fact_sales, dim_product | Halt pipeline |
| Negative total_amount | fact_sales | Halt pipeline |
| Null country | dim_store | Log warning |
| Row count below threshold | All | Halt pipeline |

---

## 🔁 Airflow DAG

```
extract_source_files
        |
transform_data
        |
validate_data
        |
apply_scd_type2
        |
load_fact_incremental
        |
load_dimensions
        |
notify_success / notify_failure
```

- Runs daily at 6am UTC
- Task-level logging — every task independent
- Retry logic — 3 retries with 5-minute delay
- Email alert on pipeline failure
- Backfill support for historical reprocessing

---

## 🧪 Testing — pytest

Every transform function is independently unit tested:

```bash
pytest tests/                    # run all tests
pytest tests/test_transform.py   # run specific module
pytest -v                        # verbose output
```

| Test File | What It Tests |
|---|---|
| `test_extract.py` | File reading, row counts, missing file handling |
| `test_transform.py` | Type casting, cleaning, joins, derived fields |
| `test_validate.py` | Null detection, duplicate detection, business rules |
| `test_scd.py` | Change detection, record expiry, new version insertion |

---

## 🚀 CI/CD — GitHub Actions

Every pull request triggers:

```yaml
steps:
  - Install Python dependencies
  - Run flake8 linting
  - Run pytest unit tests
  - Check test coverage
```

Nothing merges to main without passing all tests.

---

## 📈 Business Reports This Pipeline Enables

| Report | Key Metrics |
|---|---|
| **Sales by Region** | Revenue, units, avg order value by country |
| **Product Performance** | Top products, revenue by category |
| **Store Performance** | Revenue per store, YoY growth |
| **Warranty Analysis** | Claim rate by product and region |
| **Monthly Trends** | MoM and YoY revenue comparisons |
| **Launch Performance** | Sales velocity post new product launch |

---

## 🏃 Getting Started

### Prerequisites
- Python 3.11+
- PostgreSQL 15 (local) or Snowflake account (cloud)
- Apache Airflow 2.8+

### 1. Clone the Repository
```bash
git clone https://github.com/yourusername/apple-retail-etl-pipeline.git
cd apple-retail-etl-pipeline
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Download Dataset
Download from [Kaggle — Apple Retail Sales Dataset](https://www.kaggle.com/datasets/amangarg08/apple-retail-sales-dataset) and place CSV files in the `data/` folder.

### 4. Configure Database
```bash
# Edit config/database.py with your PostgreSQL credentials
# Default: postgresql://user:password@localhost:5432/apple_db
```

### 5. Run the Pipeline
```bash
python etl/pipeline.py
```

### 6. Run Tests
```bash
pytest tests/ -v
```

### 7. Start Airflow (optional)
```bash
airflow db init
airflow webserver --port 8080
airflow scheduler
```

---

## ☁️ Migrate to Snowflake

Change **one file only** — `config/database.py`:

```python
# Install Snowflake connector
pip install snowflake-sqlalchemy

# Update config/database.py
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

def get_engine():
    return create_engine(URL(
        account   = 'your_account.region',
        user      = 'your_user',
        password  = 'your_password',
        database  = 'APPLE_DB',
        schema    = 'PUBLIC',
        warehouse = 'COMPUTE_WH',
    ))
```

All Python transform, validate, and load logic works unchanged on Snowflake.

---

## 📚 Dataset

**Source:** [Apple Retail Sales Dataset — Kaggle](https://www.kaggle.com/datasets/amangarg08/apple-retail-sales-dataset)

| File | Rows | Description |
|---|---|---|
| `sales.csv` | 1M+ | Transaction-level sales records |
| `products.csv` | ~500 | Product master with specifications |
| `stores.csv` | ~50 | Store details by country and region |
| `category.csv` | ~20 | Product category hierarchy |
| `warranty.csv` | ~500K | Warranty claim records |

---

## 💡 Why I Built This

At Infosys I worked on Apple's **PAMS (Product Attributes Management System)** — validating product data and generating quality reports before data published downstream to Apple.com. I was always on the output side — checking data that had already been processed by a pipeline I couldn't see.

Every defect I found raised a question I couldn't answer from where I sat: *was this a source problem, a transformation bug, or a load failure?*

This project is my answer. I built the full pipeline from scratch — extraction, transformation, validation, loading, orchestration — same domain, Apple product and sales data, but now I own every layer. Everything I couldn't see at Infosys, I built here.

---

## 🗺️ Roadmap

- [ ] Add Great Expectations for advanced data profiling
- [ ] Build Streamlit dashboard on mart layer
- [ ] Add CDC (Change Data Capture) for real-time ingestion
- [ ] Migrate transforms to PySpark for large-scale processing
- [ ] Rebuild as ELT using dbt (see sister repo: [apple-retail-elt-pipeline](https://github.com/yourusername/apple-retail-elt-pipeline))

---

## 👤 Author

**[Your Name]**
Data Engineer | Career Returner
[LinkedIn](https://linkedin.com/in/yourprofile) | [GitHub](https://github.com/yourusername)

---

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.
