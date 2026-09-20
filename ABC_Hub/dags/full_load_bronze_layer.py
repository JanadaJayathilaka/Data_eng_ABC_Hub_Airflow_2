import sys
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from utils.database import extract_data, truncate_table, load_data
from utils.file import read_sql

# --------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------

SOURCE_CONN = "postgres_raw"
TARGET_CONN = "postgres_dw"

SQL_DIR = BASE_DIR / "sql" / "bronze"

# List of all tables configured for full load into bronze layer
FULL_LOAD_TABLES = [
    # Lookup / Reference Tables
    "country",
    "city",
    "content_type",
    "genre",
    "artist",
    "device",
    "payment_method",
    "support_category",
    "courier",
    "warehouse",
    "subscription_plan",

    # Customer
    "customer",
    "customer_address",
    "customer_subscription",

    # Content
    "content",
    "content_genre",
    "content_artist",

    # Streaming
    "streaming_session",

    # Physical Inventory & Rentals
    "inventory_item",
    "rental",
    "delivery",

    # Payments
    "payment",

    # Engagement
    "review",
    "wishlist",
    "support_ticket",
    "recommendation",
]


# --------------------------------------------------------------------
# Generic Bronze Full Loader
# --------------------------------------------------------------------

def load_bronze_table(sql_file: str, target_table: str):
    sql = read_sql(SQL_DIR / sql_file)

    columns, rows = extract_data(
        conn_id=SOURCE_CONN,
        sql=sql,
    )

    truncate_table(
        conn_id=TARGET_CONN,
        table_name=target_table,
    )

    load_data(
        conn_id=TARGET_CONN,
        table_name=target_table,
        columns=columns,
        rows=rows,
    )

    print(f"Loaded {len(rows)} rows into {target_table}")


default_args = {
    "owner": "ABCHub",
    "retries": 1,
}


with DAG(
    dag_id="full_load_bronze_layer",
    description="Full load into bronze layer",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["ABC", "full load bronze"],
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    end = EmptyOperator(
        task_id="end"
    )

    # Dynamically generate a PythonOperator for each full-load table
    load_tasks = [
        PythonOperator(
            task_id=f"load_bronze_{table}",
            python_callable=load_bronze_table,
            op_kwargs={
                "sql_file": f"full_load/{table}.sql",
                "target_table": f"bronze.{table}",
            },
        )
        for table in FULL_LOAD_TABLES
    ]

    start >> load_tasks >> end
