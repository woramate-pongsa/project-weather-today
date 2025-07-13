import os 
import json
import requests
import pandas as pd
from dotenv import load_dotenv
import xml.etree.ElementTree as ET

from google.cloud import storage
from google.oauth2 import service_account

def extract():
    date = pd.Timestamp.today().strftime("%Y-%m-%d")
    ## Extract data from API
    province = []
    latitude = []
    longitude = []
    date_time = []
    temperature = []
    max_temperature = []
    min_temperature = []
    wind_speed = []
    
    api_key = os.environ.get("API_KEY")
    response = requests.get(api_key)
    if response.status_code == 200:
        root = ET.fromstring(response.text)
        station = root.find("Stations")
        if station is not None:
            for stations in station.findall("Station"):
                # scrap data to variable
                province_scrap = stations.find("Province")
                latitude_scrap = stations.find("Latitude")
                longitude_scrap = stations.find("Longitude")
                
                observation_scrap = stations.find("Observation")
                date_time_scrap = observation_scrap.find("DateTime")
                temperature_scrap = observation_scrap.find("Temperature")
                max_temperature_scrap = observation_scrap.find("MaxTemperature")
                min_temperature_scrap = observation_scrap.find("MinTemperature")
                wind_speed_scrap = observation_scrap.find("WindSpeed")
                
                # append data to list
                province.append(province_scrap.text)
                latitude.append(latitude_scrap.text)
                longitude.append(longitude_scrap.text)
                date_time.append(date_time_scrap.text)
                temperature.append(temperature_scrap.text)
                max_temperature.append(max_temperature_scrap.text)
                min_temperature.append(min_temperature_scrap.text)
                wind_speed.append(wind_speed_scrap.text)
        else:
            print("Not found <station>")
    else:
        print("Request failed", response.status_code)

    raw_data = pd.DataFrame({
        "Province": province,
        "Latitude": latitude,
        "Longitude": longitude,
        "Date_time": date_time,
        "Temperature": temperature,
        "Max_temperature": max_temperature,
        "Min_temperature": min_temperature,
        "Wind_speed": wind_speed
    })

    ## Load raw data to GCS
    # Config
    GCP_PROJECT_ID = "warm-helix-412914"
    BUCKET_NAME = "lake_project"
    BUSINESS_DOMAIN = "weather_today_data"
    DATA_NAME = f"{date}_weather_today"

    load_dotenv()
    keyfile_gcs = os.environ.get("KEYFILE_PATH_GCS")
    
    service_account_info_gcs = json.load(open(keyfile_gcs))
    credentials_gcs = service_account.Credentials.from_service_account_info(service_account_info_gcs)

    # Load data to gcs
    storage_client = storage.Client(project=GCP_PROJECT_ID,credentials=credentials_gcs,)
    bucket = storage_client.bucket(BUCKET_NAME)

    # Destination to upload
    destination_blob_name = f"{BUSINESS_DOMAIN}/raw_data/{DATA_NAME}.csv"
    blob = bucket.blob(destination_blob_name)

    # Destination to get data
    blob.upload_from_filename(raw_data)