import os
import json
import pandas as pd
from io import StringIO, BytesIO
from dotenv import load_dotenv
from google.cloud import storage
from google.oauth2 import service_account

def transform_and_load_cleaned_data_to_gcs():
    date = pd.Timestamp.today().strftime("%Y-%m-%d")
    # load_dotenv(dotenv_path=os.path.join(
    #     os.path.dirname(__file__),
    #     "../config/.env"
    # ))

    ## Extract raw data from GSC
    # Config
    GCP_PROJECT_ID = os.environ.get("PROJECT_ID")
    BUCKET_NAME = "lake_project"
    BUSINESS_DOMAIN = "weather_today_data"
    DATA_NAME = f"{date}_weather_today"

    keyfile_gcs = os.environ.get("GOOGLE_CLOUD_STORAGE_APPLICATION_CREDENTIALS")

    # Read GCS key file
    service_account_info_gcs = json.load(open(keyfile_gcs))
    credentials_gcs = service_account.Credentials.from_service_account_info(
        service_account_info_gcs
    )

    # Connect to GCS
    storage_client = storage.Client(
        project=GCP_PROJECT_ID,
        credentials=credentials_gcs
    )
    bucket = storage_client.bucket(BUCKET_NAME)

    # Extract data
    source_blob_name = f"{BUSINESS_DOMAIN}/raw_data/{date}/{DATA_NAME}.csv"
    raw_blob = bucket.blob(source_blob_name)
    data_as_string = raw_blob.download_as_string()

    # Decode and use pandas read csv
    csv_file = StringIO(data_as_string.decode("utf-8"))
    raw_data = pd.read_csv(csv_file)

    ## Transform data
    raw_data["date_time"] = pd.to_datetime(raw_data["date_time"])
    raw_data["latitude"] = raw_data["latitude"].round(2)
    raw_data["longitude"] = raw_data["longitude"].round(2)
    raw_data["wind_speed"] = raw_data["wind_speed"].fillna(0.0)
    cleaned_data = raw_data.fillna({
        "latitude": 0.0, "longitude": 0.0, 
        "temperature": 0.0, "max_temperature": 0.0, 
        "min_temperature": 0.0, "wind_speed": 0.0})
    # cleaned_data["date_time"] = cleaned_data["date_time"].dt.strftime('%Y-%m-%d %H:%M:%S')
    cleaned_data["date_time"] = cleaned_data["date_time"].astype("datetime64[us]")

    # Create file memory
    parquet_buffer = BytesIO()
    cleaned_data.to_parquet(parquet_buffer, index=False, compression="snappy")
    parquet_bytes = parquet_buffer.getvalue()

    ## Load cleaned data to GCS in clean zone
    cleaned_data_destination_blob_name = f"{BUSINESS_DOMAIN}/cleaned_data/{date}/{DATA_NAME}.parquet"
    cleaned_blob = bucket.blob(cleaned_data_destination_blob_name)
    cleaned_blob.upload_from_string(
        parquet_bytes,
        content_type="application/octet-stream"
    )

    print("Transform and load cleaned_data in parquet to GCS complete!")

if __name__ == "__main__":
    transform_and_load_cleaned_data_to_gcs()