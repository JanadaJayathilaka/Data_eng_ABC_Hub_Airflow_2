import sys
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

try:
    from airflow.datasets import Dataset
except ImportError:
    from airflow.sdk.definitions.asset import Asset as Dataset

from utils.database import extract_data, truncate_table, load_data, get_connection
from utils.file import read_sql

# --------------------------------------------------------------------
# Configuration & Datasets
# --------------------------------------------------------------------
SOURCE_CONN = "postgres_raw"
TARGET_CONN = "postgres_dw"

SQL_DIR = BASE_DIR / "sql" / "bronze"

# Dataset emitted when full bronze load completes
bronze_full_load_dataset = Dataset("postgres_dw://bronze/full_load")

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
# Helpers for Historical Backfilling
# --------------------------------------------------------------------
def get_table_date_column(conn_id: str, table_name: str):
    """Detects timestamp/date column for historical filtering if present."""
    conn = get_connection(conn_id)
    cursor = conn.cursor()
    candidate_cols = ["created_at", "start_time", "rental_date", "payment_date", "opened_date", "review_date", "added_date"]
    
    for candidate in candidate_cols:
        cursor.execute(f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'public' 
              AND table_name = '{table_name}' 
              AND column_name = '{candidate}';
        """)
        if cursor.fetchone():
            cursor.close()
            conn.close()
            return candidate

    cursor.close()
    conn.close()
    return None


def delete_historical_window(target_table: str, start_date: str, end_date: str, date_col: str):
    """Deletes existing records in bronze within the specified window to allow idempotent backfill chunks."""
    conn = get_connection(TARGET_CONN)
    cursor = conn.cursor()
    cursor.execute(f"""
        DELETE FROM {target_table}
        WHERE {date_col} >= %s::timestamp AND {date_col} <= %s::timestamp;
    """, (start_date, end_date))
    conn.commit()
    deleted_count = cursor.rowcount
    cursor.close()
    conn.close()
    print(f"Deleted {deleted_count} rows from {target_table} between {start_date} and {end_date} for idempotent backfill.")


# --------------------------------------------------------------------
# Generic Bronze Loader with Historical & Backfill Support
# --------------------------------------------------------------------
def load_bronze_table(sql_file: str, target_table: str, **context):
    dag_run = context.get("dag_run")
    params = context.get("params", {})
    conf = dag_run.conf if dag_run else {}

    # Extract backfill configurations (from manual trigger conf or UI params)
    backfill_mode = conf.get("backfill_mode", params.get("backfill_mode", False))
    should_truncate = conf.get("truncate_table", params.get("truncate_table", True))

    table_raw_name = target_table.split(".")[-1]
    sql = read_sql(SQL_DIR / sql_file).strip().rstrip(";")

    # Handle Historical Backfilling date filters
    if backfill_mode:
        start_date = conf.get("start_date") or params.get("start_date")
        end_date = conf.get("end_date") or params.get("end_date")

        # Fallback to Airflow data intervals if running via Airflow backfill CLI
        if not start_date and context.get("data_interval_start"):
            start_date = context["data_interval_start"].strftime("%Y-%m-%d %H:%M:%S")
        if not end_date and context.get("data_interval_end"):
            end_date = context["data_interval_end"].strftime("%Y-%m-%d %H:%M:%S")

        date_col = get_table_date_column(SOURCE_CONN, table_raw_name)

        if date_col and start_date and end_date:
            print(f"--- BACKFILLING {target_table} using '{date_col}' [{start_date} to {end_date}] ---")
            sql = f"""
                SELECT * FROM (
                    {sql}
                ) sub
                WHERE {date_col} >= '{start_date}'::timestamp
                  AND {date_col} <= '{end_date}'::timestamp
            """

            # If not truncating entire table, clean only this historical window
            if not should_truncate:
                delete_historical_window(target_table, start_date, end_date, date_col)
        else:
            print(f"--- {target_table} has no date column; performing standard full snapshot ---")

    # 1. Truncate target table if full initial load or explicit truncate requested
    if should_truncate and not (backfill_mode and not conf.get("truncate_table", True)):
        truncate_table(
            conn_id=TARGET_CONN,
            table_name=target_table,
        )

    # 2. Extract data from source
    columns, rows = extract_data(
        conn_id=SOURCE_CONN,
        sql=sql,
    )

    # 3. Load into Bronze target table
    if rows:
        load_data(
            conn_id=TARGET_CONN,
            table_name=target_table,
            columns=columns,
            rows=rows,
        )
        print(f"Loaded {len(rows)} rows into {target_table}")
    else:
        print(f"No records found for {target_table} in the selected period.")


def task_failure_callback(context):
    task_instance = context["task_instance"]
    print(
        f"FAILED: DAG={task_instance.dag_id}, "
        f"TASK={task_instance.task_id}, "
        f"RUN={task_instance.run_id}"
    )


default_args = {
    "owner": "ABCHub",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": task_failure_callback,
}


with DAG(
    dag_id="full_load_bronze_layer",
    description="Full load into bronze layer with historical and backfill support",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=4,
    default_args=default_args,
    params={
        "backfill_mode": Param(False, type="boolean", description="Enable to backfill a specific historical date range"),
        "start_date": Param("2020-01-01 00:00:00", type="string", description="Start date (YYYY-MM-DD HH:MM:SS) for historical backfill"),
        "end_date": Param("2026-12-31 23:59:59", type="string", description="End date (YYYY-MM-DD HH:MM:SS) for historical backfill"),
        "truncate_table": Param(True, type="boolean", description="Set to False when backfilling in chunks to preserve other dates"),
    },
    tags=["ABC", "full load bronze", "backfill"],
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    # Emits bronze_full_load_dataset upon completion to trigger Silver
    end = EmptyOperator(
        task_id="end",
        outlets=[bronze_full_load_dataset],
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
            execution_timeout=timedelta(minutes=30),
        )
        for table in FULL_LOAD_TABLES
    ]

    start >> load_tasks >> end
