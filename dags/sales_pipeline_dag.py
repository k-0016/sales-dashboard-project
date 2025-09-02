from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import sys
import pandas as pd
from sqlalchemy import create_engine

sys.path.append("/opt/airflow/dags")

from etl.load_data import main as etl_main
from forecast.revenue_forecast import main as forecast_main
import subprocess

def tests_main():
    subprocess.run(["pytest", "-v", "/opt/airflow/dags/tests/"], check=True)


# Final sanity check function
def check_final_table():
    """Ensure actual_vs_forecast has rows for Tableau"""
    DB_USER = os.getenv("DB_USER", "salesuser")
    DB_PASS = os.getenv("DB_PASS", "salespass")
    DB_NAME = os.getenv("DB_NAME", "salesdb")
    DB_HOST = os.getenv("DB_HOST", "postgres_db")
    DB_PORT = os.getenv("DB_PORT", "5432")

    engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    df = pd.read_sql("SELECT COUNT(*) AS cnt FROM actual_vs_forecast", engine)

    if df["cnt"].iloc[0] == 0:
        raise ValueError("❌ actual_vs_forecast is empty! Tableau has nothing to show.")
    print(f"✅ actual_vs_forecast has {df['cnt'].iloc[0]} rows")


# DAG Definition
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="sales_pipeline",
    default_args=default_args,
    description="End-to-end ETL + Transform + Forecast pipeline for Sales Analytics",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["sales", "analytics", "tableau"],
    template_searchpath="/opt/airflow/dags/",
) as dag:

    etl = PythonOperator(
        task_id="etl_load_data",
        python_callable=etl_main,
    )

    transform = PostgresOperator(
        task_id="transform_sql",
        postgres_conn_id="postgres_sales_conn",
        sql="db/transform.sql",
    )

    forecast = PythonOperator(
        task_id="forecast_revenue",
        python_callable=forecast_main,
    )

    join_view = PostgresOperator(
        task_id="join_actual_forecast",
        postgres_conn_id="postgres_sales_conn",
        sql="db/join_actuals_forecast.sql",
    )

    validate = PythonOperator(
        task_id="validate_tests",
        python_callable=tests_main,
    )

    check_final = PythonOperator(
        task_id="check_final_table",
        python_callable=check_final_table,
    )

    etl >> transform >> forecast >> join_view >> validate >> check_final

