from datetime import datetime, timedelta, UTC
from airflow import DAG
from airflow.operators.python import PythonOperator
from etl.insert_records import load_posthog_to_postgres

default_args = {
    'description': 'Orchestrator DAG for Food Dropship Analytics',
    'start_date': datetime(2025, 1, 1, tzinfo=UTC),
    'catchup': False,
}

dag = DAG(
    dag_id="food_dropship_analytics_orchestrator",
    default_args=default_args,
    schedule=timedelta(minutes=5),  # don’t hammer every minute
)

with dag:
    ingest_posthog = PythonOperator(
        task_id="ingest_posthog_events",
        python_callable=load_posthog_to_postgres,
    )
