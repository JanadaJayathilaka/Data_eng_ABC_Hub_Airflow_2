"""
DAG: incremental_load_gold_layer
Description: Orchestrates Incremental Load into Gold Layer (Dimensions -> Facts).
Waits for incremental_load_silver_layer to complete, then performs SCD Type 2
and atomic fact upserts.
"""

import sys
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.file import read_sql

TARGET_CONN = "postgres_dw"
SQL_DIR = BASE_DIR / "sql" / "gold" / "incremental_load"
SILVER_DAG_ID = "incremental_load_silver_layer"

# Dimensions and Facts to process
GOLD_DIMENSIONS = [
    "dim_customer",
    "dim_subscription_plan",
    "dim_content",
    "dim_warehouse",
    "dim_inventory",
]

GOLD_FACTS = [
    "fact_customer_daily_activity",
    "fact_content_monthly_performance",
    "fact_inventory_daily_utilisation",
]


# --------------------------------------------------------------------
# Watermark Helper
# --------------------------------------------------------------------
def get_gold_watermark(target_table: str, is_fact: bool = False):
    """
    Retrieves the last watermark timestamp.
    For dimensions: uses GREATEST(MAX(created_at), MAX(updated_at)).
    For facts: uses a dedicated gold.watermark_tracker table.
    """
    hook = PostgresHook(postgres_conn_id=TARGET_CONN)
    conn = hook.get_conn()
    cursor = conn.cursor()

    if is_fact:
        # Create control table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS gold.watermark_tracker (
                table_name VARCHAR(100) PRIMARY KEY,
                last_watermark TIMESTAMP NOT NULL DEFAULT '1900-01-01 00:00:00'
            );
        """)
        conn.commit()

        cursor.execute(
            "SELECT last_watermark FROM gold.watermark_tracker WHERE table_name = %s;",
            (target_table,),
        )
        row = cursor.fetchone()
        last_watermark = row[0] if row else "1900-01-01 00:00:00"
    else:
        sql = f"""
            SELECT COALESCE(
                GREATEST(MAX(created_at), MAX(updated_at)),
                '1900-01-01 00:00:00'::timestamp
            ) AS last_watermark
            FROM {target_table};
        """
        cursor.execute(sql)
        row = cursor.fetchone()
        last_watermark = row[0] if row and row[0] else "1900-01-01 00:00:00"

    cursor.close()
    conn.close()

    print(f"Last watermark for {target_table}: {last_watermark}")
    return str(last_watermark)


def update_fact_watermark(target_table: str, new_watermark: str):
    """Updates the watermark tracker for fact tables upon successful execution."""
    hook = PostgresHook(postgres_conn_id=TARGET_CONN)
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO gold.watermark_tracker (table_name, last_watermark)
        VALUES (%s, %s::timestamp)
        ON CONFLICT (table_name)
        DO UPDATE SET last_watermark = EXCLUDED.last_watermark;
    """, (target_table, new_watermark))
    conn.commit()
    cursor.close()
    conn.close()


# --------------------------------------------------------------------
# Generic Loader for Gold Tables
# --------------------------------------------------------------------
def load_gold_incremental(sql_file: str, target_table: str, is_fact: bool = False):
    last_watermark = get_gold_watermark(target_table, is_fact=is_fact)
    current_run_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Read SQL template and substitute watermark
    sql = read_sql(SQL_DIR / sql_file)
    sql = sql.replace("{{ last_watermark }}", last_watermark)

    # Execute in PostgreSQL
    hook = PostgresHook(postgres_conn_id=TARGET_CONN)
    hook.run(sql)

    # If fact table, advance watermark
    if is_fact:
        update_fact_watermark(target_table, current_run_time)

    print(f"Successfully loaded incremental data into {target_table} (from watermark: {last_watermark})")


# --------------------------------------------------------------------
# DAG Definition
# --------------------------------------------------------------------
default_args = {
    "owner": "data_engineering",
    "retries": 1,
}

with DAG(
    dag_id="incremental_load_gold_layer",
    description="Incremental load into Gold Layer (Dimensions -> Facts)",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    max_active_tasks=4,
    default_args=default_args,
    tags=["gold", "incremental", "medallion", "star_schema", "ABC"],
) as dag:

    start = EmptyOperator(task_id="start")

    # 1. Wait for Silver incremental DAG to finish its run
    wait_for_silver = ExternalTaskSensor(
        task_id="wait_for_silver_incremental",
        external_dag_id=SILVER_DAG_ID,
        external_task_id="end",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="reschedule",
        poke_interval=30,
        timeout=60 * 60,
    )

    # 2. Dimension Tasks
    dim_tasks = {}
    for dim in GOLD_DIMENSIONS:
        task = PythonOperator(
            task_id=f"load_incremental_{dim}",
            python_callable=load_gold_incremental,
            op_kwargs={
                "sql_file": f"{dim}.sql",
                "target_table": f"gold.{dim}",
                "is_fact": False,
            },
        )
        dim_tasks[dim] = task

    # Enforce dependent dimension: dim_inventory waits for dim_content and dim_warehouse
    [dim_tasks["dim_content"], dim_tasks["dim_warehouse"]] >> dim_tasks["dim_inventory"]

    dimensions_completed = EmptyOperator(task_id="dimensions_completed")

    # 3. Fact Tasks (load in parallel after all dimensions finish)
    fact_tasks = []
    for fact in GOLD_FACTS:
        task = PythonOperator(
            task_id=f"load_incremental_{fact}",
            python_callable=load_gold_incremental,
            op_kwargs={
                "sql_file": f"{fact}.sql",
                "target_table": f"gold.{fact}",
                "is_fact": True,
            },
        )
        fact_tasks.append(task)

    end = EmptyOperator(task_id="end")

    # ----------------------------------------------------------------
    # Graph Wiring
    # ----------------------------------------------------------------
    start >> wait_for_silver

    # Independent dimensions start once Silver is complete
    wait_for_silver >> [
        dim_tasks["dim_customer"],
        dim_tasks["dim_subscription_plan"],
        dim_tasks["dim_content"],
        dim_tasks["dim_warehouse"],
    ]

    # All dimensions converge
    [
        dim_tasks["dim_customer"],
        dim_tasks["dim_subscription_plan"],
        dim_tasks["dim_content"],
        dim_tasks["dim_warehouse"],
        dim_tasks["dim_inventory"],
    ] >> dimensions_completed

    # Facts run once dimensions are complete
    dimensions_completed >> fact_tasks >> end
