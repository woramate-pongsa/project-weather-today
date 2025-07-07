import pandas as pd

date = pd.Timestamp.today().strftime("%Y-%m-%d")

raw_path = "etl_project1/data/raw_data"
cleaned_path = "etl_project1/data/cleaned_data"

def transform(input_path: str, output_path: str, **kwargs):
    data_path = f"{raw_data}/raw_weather_today_{date}.csv"
    raw_data = pd.read_csv(f"{raw_path}/raw_weather_today_{date}.csv")

    raw_data['Date_time'] = pd.to_datetime(raw_data['Date_time'], format="%Y-%m-%d")
    raw_data['Month'] = raw_data['Date_time'].dt.month
    raw_data['Latitude'] = raw_data['Latitude'].round(2)
    raw_data['Longitude'] = raw_data['Longitude'].round(2)
    clean_data = raw_data.dropna()

    save_path = f"{cleaned_path}/cleaned_weather_today_{date}.csv"
    pd.to_csv(save_path, index=False)
