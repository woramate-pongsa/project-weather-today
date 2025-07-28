import os
import json
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

def load_gcs_to_bq():
    date = pd.Timestamp.today().strftime("%Y-%m-%d")
    # load_dotenv(dotenv_path=os.path.join(
    #     os.path.dirname(__file__),
    #     "../config/.env"
    # ))

    GCP_PROJECT_ID = "warm-helix-412914"
    BUCKET_NAME = "lake_project"
    BUSINESS_DOMAIN = "weather_today_data"
    DATA_NAME = f"{date}_weather_today"
    LOCATION = "asia-southeast1"
    DATASET = "my_project"

    # Prepare and Load Credentials to Connect to GCP Services
    keyfile_bigquery = os.environ.get("KEYFILE_PATH_BQ")
    service_account_info_bigquery = json.load(open(keyfile_bigquery))
    credentials_bigquery = service_account.Credentials.from_service_account_info(
        service_account_info_bigquery
    )

    # Load data from GCS to BigQuery
    bigquery_client = bigquery.Client(
        project=GCP_PROJECT_ID,
        credentials=credentials_bigquery,
        location=LOCATION,
    )

    source_data_in_gcs = f"gs://{BUCKET_NAME}/{BUSINESS_DOMAIN}/cleaned_data/{date}/*.parquet"
    table_id = f"{GCP_PROJECT_ID}.{DATASET}.{DATA_NAME}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.PARQUET,
        autodetect=True,
        # schema=[
        #     bigquery.SchemaField("province", bigquery.SqlTypeNames.STRING),
        #     bigquery.SchemaField("latitude", bigquery.SqlTypeNames.STRING),
        #     bigquery.SchemaField("longitude", bigquery.SqlTypeNames.STRING),
        #     bigquery.SchemaField("date_time", bigquery.SqlTypeNames.INT64),
        #     bigquery.SchemaField("temperature", bigquery.SqlTypeNames.STRING),
        #     bigquery.SchemaField("max_temperature", bigquery.SqlTypeNames.STRING),
        #     bigquery.SchemaField("min_temperature", bigquery.SqlTypeNames.STRING),
        #     bigquery.SchemaField("wind_speed", bigquery.SqlTypeNames.STRING),
        #     bigquery.SchemaField("month", bigquery.SqlTypeNames.STRING)
        # ],
    )
    job = bigquery_client.load_table_from_uri(
        source_data_in_gcs,
        table_id,
        job_config=job_config,
        location=LOCATION,
    )
    job.result()

    table = bigquery_client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

load_gcs_to_bq()