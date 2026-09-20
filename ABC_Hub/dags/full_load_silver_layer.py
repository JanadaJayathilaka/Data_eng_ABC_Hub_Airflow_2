import sys
from datetime import datetime
from pathlib import Path

# --------------------------------------------------------------------
# Make sure the dags folder is on the path BEFORE importing utils
# --------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

try:
    from airflow.datasets import Dataset
except ImportError:
    from airflow.sdk.definitions.asset import Asset as Dataset

from utils.database import truncate_table
from utils.file import read_sql

# --------------------------------------------------------------------
# Configuration & Datasets
# --------------------------------------------------------------------
TARGET_CONN = "postgres_dw"
SQL_DIR = BASE_DIR / "sql"

# Listen to the Dataset produced by full_load_bronze_layer
bronze_full_load_dataset = Dataset("postgres_dw://bronze/full_load")

SILVER_TABLES = [
    # Lookup / Reference Tables
    "country", "city", "content_type", "genre", "artist",
    "device", "payment_method", "support_category", "courier",
    "warehouse", "subscription_plan",

    # Customer
    "customer", "customer_address", "customer_subscription",

    # Content
    "content", "content_genre", "content_artist",

    # Streaming
    "streaming_session",

    # Physical Inventory & Rentals
    "inventory_item", "rental", "delivery",

    # Payments
    "payment",

    # Engagement
    "review", "wishlist", "support_ticket", "recommendation",
]


# --------------------------------------------------------------------
# Generic Silver Loader
# --------------------------------------------------------------------
def load_silver_table(sql_file: str, target_table: str):
    # 1. Truncate target silver table first
    truncate_table(
        conn_id=TARGET_CONN,
        table_name=target_table,
    )

    # 2. Read and execute Silver transformation SQL directly in PostgreSQL
    sql = read_sql(SQL_DIR / sql_file)
    hook = PostgresHook(postgres_conn_id=TARGET_CONN)
    hook.run(sql)
    print(f"Loaded {target_table} successfully!")


# --------------------------------------------------------------------
# DAG Definition - Data-Aware Scheduled on bronze_full_load_dataset
# --------------------------------------------------------------------
default_args = {
    "owner": "ABCHub",
    "retries": 1,
}

with DAG(
    dag_id="full_load_silver_layer",
    description="Build Silver layer from completed Bronze tables via Dataset triggers",
    start_date=datetime(2026, 1, 1),
    schedule=[bronze_full_load_dataset],  # 👈 Data-Aware Scheduling: triggers when Bronze completes!
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    max_active_tasks=4,
    tags=["silver", "full_load", "medallion", "ABC", "dataset"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    silver_tasks = []

    for table in SILVER_TABLES:
        load_silver = PythonOperator(
            task_id=f"load_silver_{table}",
            python_callable=load_silver_table,
            op_kwargs={
                "sql_file": f"silver/full_load/{table}.sql",
                "target_table": f"silver.{table}",
            },
        )
        silver_tasks.append(load_silver)

    # All 26 silver tables execute directly in parallel (throttled by max_active_tasks=4)
    start >> silver_tasks >> end
