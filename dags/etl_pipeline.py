import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from datetime import datetime, timedelta
from scripts.extract import extract_from_api
from scripts.transform import transform_and_load_cleaned_data_to_gcs
from scripts.load import load_gcs_to_bq

from airflow import DAG
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator


default_args = {
    "owner": "etl_pipeline",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
    }

with DAG(
    dag_id='etl_pipeline',
    start_date=datetime(2025, 8, 2),
    schedule="0 21 * * *",
    default_args=default_args,
    catchup=False
    ) as dag:

    task_extract_from_api = PythonOperator(
        task_id="extract_from_api",
        python_callable=extract_from_api,
    )

    task_transform_and_load_cleaned_data_to_gcs = PythonOperator(  
        task_id="transform_and_load_cleaned_data_to_gcs",
        python_callable=transform_and_load_cleaned_data_to_gcs,
    )

    task_load_gcs_to_bq = PythonOperator(
        task_id="load_gcs_to_bq",
        python_callable=load_gcs_to_bq,
    )

task_extract_from_api >> task_transform_and_load_cleaned_data_to_gcs >> task_load_gcs_to_bq