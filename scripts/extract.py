import requests
import xml.etree.ElementTree as ET
import pandas as pd

date = pd.Timestamp.today().strftime("%Y-%m-%d")

def extract():
    province = []
    latitude = []
    longitude = []
    date_time = []
    temperature = []
    max_temperature = []
    min_temperature = []
    wind_speed = []

    response = requests.get("https://data.tmd.go.th/api/WeatherToday/V2/?uid=api&ukey=api12345")
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

    df = pd.DataFrame({
        "Province": province,
        "Latitude": latitude,
        "Longitude": longitude,
        "Date_time": date_time,
        "Temperature": temperature,
        "Max_temperature": max_temperature,
        "Min_temperature": min_temperature,
        "Wind_speed": wind_speed
    })

    save_path = f"etl_project1/data/raw_data/raw_weather_today_{date}.csv"
    df.to_csv(save_path, index=False)