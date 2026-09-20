from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator



def hello_world():
    print("===================================")
    print(" Welcome to FreshBank ETL Pipeline ")
    print(" Apache Airflow is Working!")
    print("===================================")


default_args = {
    "owner": "ABCHub",
    "retries": 1,
}


with DAG(
    dag_id="hello_world",
    description="First Airflow DAG",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["training", "ABC"],
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    hello = PythonOperator(
        task_id="hello_world",
        python_callable=hello_world
    )

    end = EmptyOperator(
        task_id="end"
    )

    start >> hello >> end