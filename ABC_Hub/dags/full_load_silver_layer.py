import sys
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from utils.database import extract_data, truncate_table, load_data
from utils.file import read_sql

# --------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------
SOURCE_CONN = "postgres_dw"
TARGET_CONN = "postgres_dw"

SQL_DIR = BASE_DIR / "sql"
BRONZE_DAG_ID = "full_load_bronze_layer"

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
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    # 1. Truncate FIRST
    truncate_table(
        conn_id=TARGET_CONN,
        table_name=target_table,
    )

    # 2. Read and execute your Silver SQL file directly in PostgreSQL
    sql = read_sql(SQL_DIR / sql_file)
    hook = PostgresHook(postgres_conn_id=TARGET_CONN)
    hook.run(sql)
    print(f"Loaded {target_table} successfully!")

# --------------------------------------------------------------------
# External Bronze Sensor Helper
# --------------------------------------------------------------------
def bronze_sensor(task_id: str, sensor_task_id: str):
    return ExternalTaskSensor(
        task_id=task_id,
        external_dag_id=BRONZE_DAG_ID,
        external_task_id=sensor_task_id,
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="reschedule",
        poke_interval=30,
        timeout=60 * 60,
    )


# --------------------------------------------------------------------
# DAG Definition
# --------------------------------------------------------------------
default_args = {
    "owner": "ABCHub",
    "retries": 1,
}

with DAG(
    dag_id="full_load_silver_layer",
    description="Build Silver layer from completed Bronze tables",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    schedule=None,
    tags=["silver", "training", "medallion"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    silver_tasks = {}

    # Build sensor -> silver loader pipeline for each ABC Hub table
    for table in SILVER_TABLES:
        wait_sensor = bronze_sensor(
            task_id=f"wait_for_bronze_{table}",
            sensor_task_id=f"load_bronze_{table}",
        )

        load_silver = PythonOperator(
            task_id=f"load_silver_{table}",
            python_callable=load_silver_table,
            op_kwargs={
                "sql_file": f"silver/full_load/{table}.sql",
                "target_table": f"silver.{table}",
            },
        )

        start >> wait_sensor >> load_silver >> end
        silver_tasks[table] = load_silver
