import os
import json
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound

def load_gcs_to_bq():
    date = pd.Timestamp.today().strftime("%Y-%m-%d")
    # load_dotenv(dotenv_path=os.path.join(
    #     os.path.dirname(__file__),
    #     "../config/.env"
    # ))

    GCP_PROJECT_ID = os.environ.get("PROJECT_ID")
    BUCKET_NAME = "lake_project"
    BUSINESS_DOMAIN = "weather_today_data"
    DATA_NAME = "weather_today_data"
    LOCATION = "asia-southeast1"
    DATASET = "my_project"

    # Prepare and Load Credentials to Connect to GCP Services
    keyfile_bigquery = os.environ.get("GOOGLE_BIGQUERY_APPLICATION_CREDENTIALS")
    service_account_info_bigquery = json.load(open(keyfile_bigquery))
    credentials_bigquery = service_account.Credentials.from_service_account_info(
        service_account_info_bigquery
    )

    ## Load data from GCS to BigQuery
    # Read BQ credential
    bigquery_client = bigquery.Client(
        project=GCP_PROJECT_ID,
        credentials=credentials_bigquery,
        location=LOCATION,
    )
    
    # Soruce of cleaned data in GCS bucket
    source_data_in_gcs = f"gs://{BUCKET_NAME}/{BUSINESS_DOMAIN}/cleaned_data/{date}/*.parquet"
    # Table ID in BQ
    table_id = f"{GCP_PROJECT_ID}.{DATASET}.{DATA_NAME}"

    try:
        bigquery_client.get_table(table_id)
        table_exists = True
    except NotFound:
        table_exists = False

    write_disposition = (
        bigquery.WriteDisposition.WRITE_APPEND if table_exists
        else bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        source_format=bigquery.SourceFormat.PARQUET,
        # autodetect=True,
        schema=[
            bigquery.SchemaField("province", bigquery.SqlTypeNames.STRING),
            bigquery.SchemaField("station_name", bigquery.SqlTypeNames.STRING),
            bigquery.SchemaField("latitude", bigquery.SqlTypeNames.FLOAT),
            bigquery.SchemaField("longitude", bigquery.SqlTypeNames.FLOAT),
            bigquery.SchemaField("date_time", bigquery.SqlTypeNames.TIMESTAMP),
            bigquery.SchemaField("temperature", bigquery.SqlTypeNames.FLOAT),
            bigquery.SchemaField("max_temperature", bigquery.SqlTypeNames.FLOAT),
            bigquery.SchemaField("min_temperature", bigquery.SqlTypeNames.FLOAT),
            bigquery.SchemaField("wind_speed", bigquery.SqlTypeNames.FLOAT)
        ],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="date_time",
        )
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

if __name__ == "__main__":
    load_gcs_to_bq()