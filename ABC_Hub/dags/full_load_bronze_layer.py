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
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["ABC", "full load bronze"],
) as dag:

    start = EmptyOperator(
        task_id="start" 
    )

    load_bronze_country = PythonOperator(
        task_id="load_bronze_country",
        python_callable=load_bronze_table,
        op_kwargs={
            "sql_file": "full_load/country.sql",
            "target_table": "bronze.country",
        },
    )

    end = EmptyOperator(
        task_id="end"
    )

    start >> [load_bronze_country] >> end

