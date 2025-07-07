import os
import requests
from datetime import datetime, timedelta
import pandas as pd

from scripts.extract import extract
from scripts.transform import transform
from scripts.load_to_gcs import load_to_gcs
from scripts.load_to_bq import load_to_bq

from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator

today = pd.Timestamp.today().strftime("%Y-%m-%d")

default_args = {
    "owner": "airflow_etl",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id='etl_pipeline',
    start_date=datetime(2025, 6, 26),
    schedule_interval="@daily",
    default_args=default_args,
    sla=timedelta(hours=2)
) as dag:

    extract_data_from_api = PythonOperator(
        task_id="extract_data_from_api",
        python_callable=extract,
    )

    load_data_to_gcs = PythonOperator(  
        task_id="load_data_to_gcs",
        python_callable=load_to_gcs,
        op_kwargs
    )


    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transform,
        op_kwargs= {
            "input_path": "etl_project1/data/raw_data.csv",
            "output_path": "etl_project1/data/cleaned_data.csv"
        }
    )

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=load,
        op_kwargs= {
            "input_path": "etl_project1/data/cleaned_data.csv",
            "output_path": "bigquery_data_warehouse" 
        }
    )

extract_task >> transform_task >>  >> load_task