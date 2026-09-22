"""
DAG: full_load_gold_layer
Description: Orchestrates Full Load for the Gold Layer (Dimensions -> Facts)
"""

import os
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.empty import EmptyOperator

try:
    from airflow.datasets import Dataset
except ImportError:
    from airflow.sdk.definitions.asset import Asset as Dataset

POSTGRES_CONN_ID = "postgres_dw"
SQL_BASE_PATH = str(Path(__file__).resolve().parent / "sql" / "gold" / "full_load")

# Datasets
silver_full_load_dataset = Dataset("postgres_dw://silver/full_load")
gold_full_load_dataset = Dataset("postgres_dw://gold/full_load")

default_args = {
    "owner": "data_engineering",
    "retries": 1,
}

with DAG(
    dag_id="full_load_gold_layer",
    default_args=default_args,
    description="Full Load for Gold Layer Star Schema (Dimensions -> Facts)",
    schedule=[silver_full_load_dataset],  # Automatically triggers when Silver finishes
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_tasks=4,
    template_searchpath=[SQL_BASE_PATH],  # Allows direct filename references without slash issues
    tags=["gold", "full_load", "star_schema", "ABC"],
) as dag:

    start = EmptyOperator(task_id="start")

    # -------------------------------------------------------------
    # 1. Independent Dimensions
    # -------------------------------------------------------------
    load_dim_date = SQLExecuteQueryOperator(
        task_id="load_dim_date",
        conn_id=POSTGRES_CONN_ID,
        sql="dim_date.sql",
    )

    load_dim_month = SQLExecuteQueryOperator(
        task_id="load_dim_month",
        conn_id=POSTGRES_CONN_ID,
        sql="dim_month.sql",
    )

    load_dim_customer = SQLExecuteQueryOperator(
        task_id="load_dim_customer",
        conn_id=POSTGRES_CONN_ID,
        sql="dim_customer.sql",
    )

    load_dim_subscription_plan = SQLExecuteQueryOperator(
        task_id="load_dim_subscription_plan",
        conn_id=POSTGRES_CONN_ID,
        sql="dim_subscription_plan.sql",
    )

    load_dim_content = SQLExecuteQueryOperator(
        task_id="load_dim_content",
        conn_id=POSTGRES_CONN_ID,
        sql="dim_content.sql",
    )

    load_dim_warehouse = SQLExecuteQueryOperator(
        task_id="load_dim_warehouse",
        conn_id=POSTGRES_CONN_ID,
        sql="dim_warehouse.sql",
    )

    # -------------------------------------------------------------
    # 2. Dependent Dimension (dim_inventory needs dim_content & dim_warehouse)
    # -------------------------------------------------------------
    load_dim_inventory = SQLExecuteQueryOperator(
        task_id="load_dim_inventory",
        conn_id=POSTGRES_CONN_ID,
        sql="dim_inventory.sql",
    )

    dimensions_completed = EmptyOperator(task_id="dimensions_completed")

    # -------------------------------------------------------------
    # 3. Fact Tables (Run in parallel after dimensions)
    # -------------------------------------------------------------
    load_fact_customer_daily_activity = SQLExecuteQueryOperator(
        task_id="load_fact_customer_daily_activity",
        conn_id=POSTGRES_CONN_ID,
        sql="fact_customer_daily_activity.sql",
    )

    load_fact_content_monthly_performance = SQLExecuteQueryOperator(
        task_id="load_fact_content_monthly_performance",
        conn_id=POSTGRES_CONN_ID,
        sql="fact_content_monthly_performance.sql",
    )

    load_fact_inventory_daily_utilisation = SQLExecuteQueryOperator(
        task_id="load_fact_inventory_daily_utilisation",
        conn_id=POSTGRES_CONN_ID,
        sql="fact_inventory_daily_utilisation.sql",
    )

    end = EmptyOperator(
        task_id="end",
        outlets=[gold_full_load_dataset],
    )

    # -------------------------------------------------------------
    # Dependency Graph
    # -------------------------------------------------------------
    # Independent dimensions start immediately
    start >> [
        load_dim_date,
        load_dim_month,
        load_dim_customer,
        load_dim_subscription_plan,
        load_dim_content,
        load_dim_warehouse,
    ]

    # Inventory dimension waits for content and warehouse dimensions
    [load_dim_content, load_dim_warehouse] >> load_dim_inventory

    # All dimensions converge before facts start
    [
        load_dim_date,
        load_dim_month,
        load_dim_customer,
        load_dim_subscription_plan,
        load_dim_content,
        load_dim_warehouse,
        load_dim_inventory,
    ] >> dimensions_completed

    # Fact tables run once all dimensions are loaded
    dimensions_completed >> [
        load_fact_customer_daily_activity,
        load_fact_content_monthly_performance,
        load_fact_inventory_daily_utilisation,
    ] >> end
