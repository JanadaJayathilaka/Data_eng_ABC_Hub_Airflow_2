from datetime import datetime
from pathlib import Path
import sys

# --------------------------------------------------------------------
# Make sure the dags folder (or project root) is on the path
# BEFORE importing anything from utils
# --------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from utils.database import extract_data, load_data
from utils.file import read_sql


# --------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------

SOURCE_CONN = "postgres_raw"
TARGET_CONN = "postgres_dw"

SQL_DIR = BASE_DIR / "sql" / "bronze"


# --------------------------------------------------------------------
# Get last watermark from Bronze
# --------------------------------------------------------------------

def get_last_watermark(target_table: str):

    sql = f"""
        SELECT
            GREATEST(
                MAX(created_at),
                MAX(updated_at)
            ) AS last_watermark
        FROM {target_table};
    """

    columns, rows = extract_data(
        conn_id=TARGET_CONN,
        sql=sql,
    )

    last_watermark = rows[0][0]

    print(
        f"Last watermark for {target_table}: "
        f"{last_watermark}"
    )

    return last_watermark


# --------------------------------------------------------------------
# Generic Bronze Incremental Loader
# --------------------------------------------------------------------

def load_bronze_table(sql_file: str, target_table: str):

    # --------------------------------------------------------------
    # Get the last successfully loaded timestamp from Bronze
    # --------------------------------------------------------------

    last_watermark = get_last_watermark(target_table)

    # --------------------------------------------------------------
    # Read incremental SQL template
    # --------------------------------------------------------------

    sql = read_sql(SQL_DIR / sql_file)

    # --------------------------------------------------------------
    # Replace watermark placeholder
    # --------------------------------------------------------------

    sql = sql.replace(
        "{{ last_watermark }}",
        str(last_watermark)
    )

    # --------------------------------------------------------------
    # Extract only new / changed records
    # --------------------------------------------------------------

    columns, rows = extract_data(
        conn_id=SOURCE_CONN,
        sql=sql,
    )

    # --------------------------------------------------------------
    # Append to Bronze
    # --------------------------------------------------------------

    if rows:

        load_data(
            conn_id=TARGET_CONN,
            table_name=target_table,
            columns=columns,
            rows=rows,
        )

        print(
            f"Loaded {len(rows)} incremental rows "
            f"into {target_table}"
        )

    else:

        print(
            f"No new or updated rows found "
            f"for {target_table}"
        )


# --------------------------------------------------------------------
# Incremental Bronze DAG
# --------------------------------------------------------------------

default_args = {
    "owner": "ABCHub",
    "retries": 1,
}

with DAG(
    dag_id="incremental_load_bronze",
    description="Incremental load into Bronze",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    default_args=default_args,
    tags=["ABC", "incremental bronze"],
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    end = EmptyOperator(
        task_id="end"
    )

    # Create PythonOperators for incremental bronze loading
    # You can add or remove tables from this list as needed

    incremental_tasks = [
        PythonOperator(
            task_id=f"load_bronze_incremental_{table}",
            python_callable=load_bronze_table,
            op_kwargs={
                "sql_file": f"incremental_load/{table}.sql",
                "target_table": f"bronze.{table}",
            },
        )
        for table in [
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
            "customer",
            "customer_address",
            "customer_subscription",
            "content",
            "content_genre",
            "content_artist",
            "streaming_session",
            "inventory_item",
            "rental",
            "delivery",
            "payment",
            "review",
            "wishlist",
            "support_ticket",
            "recommendation",
        ]
       
    ]

    start >> incremental_tasks >> end
   
