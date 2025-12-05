import pandas as pd
import openmeteo_requests
import requests_cache
from retry_requests import retry
from config.constant import OPENMETEO_URL, HATSUDEN_INFO, MODELS

def _fetch_data(latitude, longitude, models):
	cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
	retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
	openmeteo = openmeteo_requests.Client(session = retry_session)

	params = {
		"latitude": {latitude},
		"longitude": {longitude},
		"start_date": "2023-12-31",
		"end_date": "2025-01-01",
		"hourly": "shortwave_radiation",
		"models": models,
	}

	responses = openmeteo.weather_api(OPENMETEO_URL, params=params)

	for response in responses:
		hourly = response.Hourly()
		hourly_shortwave_radiation = hourly.Variables(0).ValuesAsNumpy()

		# Build timestamp index in JST (and drop tz info for clean CSVs)
		dt_index = pd.date_range(
			start=pd.to_datetime(hourly.Time(), unit="s"),
			end=pd.to_datetime(hourly.TimeEnd(), unit="s"),
			freq=pd.Timedelta(seconds=hourly.Interval()),
			inclusive="left",
		).tz_localize("UTC").tz_convert("Asia/Tokyo").tz_localize(None)

		data = {"date": dt_index}
		data["shortwave_solar_radiation"] = hourly_shortwave_radiation
		hourly_dataframe = pd.DataFrame(data=data)
		
		return hourly_dataframe


def get_data():
	results = {}
	for model in MODELS:
		temp = []
		for plant in HATSUDEN_INFO:
			latitude = plant["latitude"]
			longitude = plant["longitude"]
			romaji_name = plant["romaji_name"]
			plant_name = plant["plant_name"]
			
			hourly_dataframe = _fetch_data(latitude, longitude, model)
			temp.append({
				"romaji_name": romaji_name,
				"plant_name": plant_name,
				"dataframe": hourly_dataframe
			})

		for item in temp:
			item["dataframe"]["plant_name"] = item["plant_name"]
			item["dataframe"]["romaji_name"] = item["romaji_name"]

		combined_dataframe = pd.concat([item["dataframe"] for item in temp], ignore_index=True)
		combined_dataframe = combined_dataframe[
			(combined_dataframe["date"] >= "2024-01-01") & (combined_dataframe["date"] < "2025-01-01")
		].reset_index(drop=True)

		results[model] = combined_dataframe

	return results

if __name__ == "__main__":
	data = get_data()
	for k, v in data.items():
		print(f"Model: {k}")
		print(v)
		v.to_csv(f"data/{k.lower()}_data_2024.csv", index=False)
