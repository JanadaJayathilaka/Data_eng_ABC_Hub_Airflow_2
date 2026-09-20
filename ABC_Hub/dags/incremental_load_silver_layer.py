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
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.file import read_sql

# --------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------
TARGET_CONN = "postgres_dw"

SQL_DIR = BASE_DIR / "sql"
BRONZE_DAG_ID = "incremental_load_bronze"

# List of ABC Hub tables to process incrementally in Silver
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
# Helper: Get last watermark from Silver target table
# --------------------------------------------------------------------
def get_last_watermark(target_table: str):
    sql = f"""
        SELECT
            COALESCE(
                GREATEST(MAX(created_at), MAX(updated_at)),
                '1900-01-01 00:00:00'::timestamp
            ) AS last_watermark
        FROM {target_table};
    """
    hook = PostgresHook(postgres_conn_id=TARGET_CONN)
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql)
    row = cursor.fetchone()
    last_watermark = row[0] if row and row[0] else "1900-01-01 00:00:00"
    cursor.close()
    conn.close()

    print(f"Last watermark for {target_table}: {last_watermark}")
    return last_watermark


# --------------------------------------------------------------------
# Generic Silver Incremental Loader
# --------------------------------------------------------------------
def load_silver_incremental_table(sql_file: str, target_table: str):
    last_watermark = get_last_watermark(target_table)

    # Read incremental SQL template
    sql = read_sql(SQL_DIR / sql_file)

    # Inject dynamic watermark
    sql = sql.replace("{{ last_watermark }}", str(last_watermark))

    # Execute in-database transformation / upsert
    hook = PostgresHook(postgres_conn_id=TARGET_CONN)
    hook.run(sql)

    print(f"Successfully loaded incremental data into {target_table} (watermark: {last_watermark})")


# --------------------------------------------------------------------
# External Bronze Sensor Helper
# --------------------------------------------------------------------
def bronze_sensor(task_id: str, sensor_task_id: str):
    """
    Waits for a specific task in the incremental Bronze DAG to finish
    before allowing the corresponding Silver task to run.
    """
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
    dag_id="incremental_load_silver_layer",
    description="Incremental load and deduplication into Silver layer",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["silver", "incremental", "medallion", "ABC"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    silver_tasks = {}

    for table in SILVER_TABLES:
        wait_sensor = bronze_sensor(
            task_id=f"wait_for_bronze_incremental_{table}",
            sensor_task_id=f"load_bronze_incremental_{table}",
        )

        load_silver = PythonOperator(
            task_id=f"load_silver_incremental_{table}",
            python_callable=load_silver_incremental_table,
            op_kwargs={
                "sql_file": f"silver/incremental_load/{table}.sql",
                "target_table": f"silver.{table}",
            },
        )

        # Wire: start -> wait for bronze incremental task -> run silver incremental load -> end
        start >> wait_sensor >> load_silver >> end
        silver_tasks[table] = load_silver
 