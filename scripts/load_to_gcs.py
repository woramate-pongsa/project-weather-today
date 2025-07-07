import json
import pandas as pd
from google.cloud import bigquery, storage
from google.oauth2 import service_account

date = pd.Timestamp.today().strftime("%Y-%m-%d")

GCP_PROJECT_ID = "warm-helix-412914"
DAGS_FOLDER = "etl_project1/data/raw_data"

BUSINESS_DOMAIN = "weather_today_data"
DATA = f"raw_weather_today_{date}"

def load_to_gcs():
    keyfile_gcs = f"{DAGS_FOLDER}/loading-to-gcs.json"
    service_account_info_gcs = json.load(open(keyfile_gcs))
    credentials_gcs = service_account.Credentials.from_service_account_info(
        service_account_info_gcs
    )

    # Load data from local to gcs
    bucket_name = "weather_today"
    storage_client = storage.Client(
        project=GCP_PROJECT_ID,
        credentials=credentials_gcs,
    )

    bucket = storage_client.bucket(bucket_name)

    file_path = f"{DAGS_FOLDER}/{DATA}.csv"
    destination_blob_name = f""

    destination_blob_name = f"{BUSINESS_DOMAIN}/raw_data/{DATA}.csv"
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)