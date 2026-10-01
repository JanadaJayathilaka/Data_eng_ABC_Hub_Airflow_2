# ABC Hub Data Engineering Pipeline

An end-to-end ELT pipeline built with **Apache Airflow** and **PostgreSQL** implementing a **Medallion Architecture (Bronze &rarr; Silver &rarr; Gold)** for ABC Hub's streaming and media rental platform.

---

## 1. Architecture

```
Operational Source (postgres_raw)
   │
   ▼
Bronze Layer (Raw Snapshots + Metadata: load_timestamp, source_system)
   │ (Airflow Dataset Trigger / Watermark)
   ▼
Silver Layer (Cleaned, Normalized, Deduplicated via Window Functions & Atomic CTEs)
   │ (Airflow Dataset Trigger / Sensors)
   ▼
Gold Layer (Star Schema: Conformed & SCD Type 2 Dimensions + Fact Tables)
```

- **Bronze**: Raw ingest of 26 operational tables with lineage metadata.
- **Silver**: Deduplicated, trimmed, and standardized tables (`silver.*`).
- **Gold**: Analytics-ready Star Schema with SCD Type 2 dimensions (`dim_customer`, `dim_content`, `dim_subscription_plan`, `dim_inventory`) and 3 facts (`fact_customer_daily_activity`, `fact_content_monthly_performance`, `fact_inventory_daily_utilisation`).

---

## 2. Dependencies

- **Python**: 3.10+
- **PostgreSQL**: 14+
- **Apache Airflow**: 2.7+ (with Dataset / Asset support)
- **Key Packages**:
  ```text
  apache-airflow>=2.7.0
  apache-airflow-providers-postgres>=5.6.0
  apache-airflow-providers-common-sql>=1.8.0
  psycopg2-binary>=2.9.9
  ```

---

## 3. Setup & Configuration

### Environment Setup
```bash
# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate       # On Windows: .\.venv\Scripts\Activate.ps1

# Install dependencies
pip install "apache-airflow>=2.7.0" "apache-airflow-providers-postgres" psycopg2-binary
```

### Airflow Configuration
```bash
# Set Airflow directory to project folder
export AIRFLOW_HOME="$(pwd)/ABC_Hub"   # On Windows PowerShell: $env:AIRFLOW_HOME="$PWD\ABC_Hub"
```

### PostgreSQL Connections
Create two connections in Airflow (**Admin &rarr; Connections** or via CLI):
- **`postgres_raw`**: Source OLTP database containing the 26 raw tables (`public` schema).
- **`postgres_dw`**: Data warehouse database hosting `bronze`, `silver`, and `gold` schemas.

```bash
# CLI setup example:
airflow connections add 'postgres_raw' --conn-type 'postgres' --conn-host 'localhost' --conn-login 'postgres' --conn-password 'postgres' --conn-schema 'abc_raw_db' --conn-port 5432
airflow connections add 'postgres_dw'  --conn-type 'postgres' --conn-host 'localhost' --conn-login 'postgres' --conn-password 'postgres' --conn-schema 'abc_dw_db'  --conn-port 5432
```

### Database Initialization
In `abc_dw_db`, create the Medallion schemas:
```sql
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
```

---

## 4. Execution Instructions

### Initial Full Load
1. Start Airflow:
   ```bash
   airflow standalone
   ```
2. In the Airflow UI (`http://localhost:8080`), manually trigger **`full_load_bronze_layer`**.
3. **Automated Cascade**:
   - `full_load_bronze_layer` finishes and emits Dataset `postgres_dw://bronze/full_load`.
   - **`full_load_silver_layer`** triggers automatically, cleans data, and emits `postgres_dw://silver/full_load`.
   - **`full_load_gold_layer`** triggers automatically to build dimensions and facts.

### Historical Backfills
Trigger `full_load_bronze_layer` with custom configuration parameters:
```json
{
  "backfill_mode": true,
  "start_date": "2024-01-01 00:00:00",
  "end_date": "2024-06-30 23:59:59",
  "truncate_table": false
}
```
*The DAG identifies timestamp columns and deletes only the matching date slice before re-inserting.*

### Hourly Incremental Loads
Turn **ON** the following DAGs (scheduled `@hourly`):
1. **`incremental_load_bronze`**: Extracts changes using watermark `GREATEST(MAX(created_at), MAX(updated_at))`.
2. **`incremental_load_silver_layer`**: Senses bronze tasks and executes atomic CTE upserts.
3. **`incremental_load_gold_layer`**: Senses silver completion, updates SCD Type 2 dimensions, and upserts daily facts via `gold.watermark_tracker`.

---

## 5. Assumptions

- **Timestamps**: Operational tables have reliable `created_at`/`updated_at` or event date columns for change capture.
- **Primary Keys**: Source IDs (`customer_id`, `rental_id`, etc.) are stable and immutable.
- **Idempotency**: All DAGs can be safely rerun without creating duplicate records.
- **Time Horizon**: Open-ended SCD Type 2 dimension records use `'9999-12-31'` as `effective_to`.

---

## 6. Technology Decisions

- **Apache Airflow**: Chosen for native DAG dependency modeling, retry resilience, and backfill capabilities.
- **In-Database SQL Pushdown (ELT)**: All deduplication and aggregations run inside PostgreSQL via `PostgresHook` to prevent worker memory (OOM) issues.
- **Data-Aware Scheduling (Datasets)**: Upstream full loads trigger downstream DAGs instantaneously upon dataset updates without cron guesswork.
- **`ExternalTaskSensor` (Reschedule Mode)**: Incremental DAGs release worker slots while waiting for upstream dependencies.
- **SCD Type 2 Modeling**: Accurately preserves historical customer tiers, content attributes, and rental states for point-in-time reporting.
- **`gold.watermark_tracker` Control Table**: Database-managed checkpointing guarantees transactional consistency for delta loads.
