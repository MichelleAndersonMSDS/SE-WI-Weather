"""
DATA GATHERING AND PRE-PROCESSING SCRIPT FOR MILWAUKEE WEATHER DATA
STEP 1 of 4

Created on Fri Dec 22 2023
@author: Michelle.Anderson

This code performs the following tasks:
  1.  Pulls historical weather data from the open-meteo API
  2.  Calculates mean daily soil temperature and moisture
  3.  Merge daily and hourly data
  4.  Save processed data as CSV file
"""

################################################################################
# LOAD LIBRARIES
################################################################################

import openmeteo_requests
import requests_cache
import pandas as pd
import datetime as datetime
from retry_requests import retry

################################################################################
# SET PARAMETERS
################################################################################

filepath = ''

today = datetime.date.today()

get_date = today.strftime('%Y-%m-%d')

start_date = "1940-01-01"

end_date = "2022-12-31"

################################################################################
# READ IN DATA
################################################################################

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

# Make sure all required weather variables are listed here
# The order of variables in hourly or daily is important to assign them correctly below
url = "https://archive-api.open-meteo.com/v1/archive"
params = {
	"latitude": 42.9675,
	"longitude": -88.54972222,
	"start_date": start_date,
	"end_date": end_date,
	"hourly": ["soil_temperature_0_to_7cm", "soil_temperature_7_to_28cm", "soil_temperature_28_to_100cm", "soil_temperature_100_to_255cm", "soil_moisture_0_to_7cm", "soil_moisture_7_to_28cm", "soil_moisture_28_to_100cm", "soil_moisture_100_to_255cm"],
	"daily": ["weather_code", "temperature_2m_max", "temperature_2m_min", "temperature_2m_mean", "sunrise", "sunset", "precipitation_sum", "rain_sum", "snowfall_sum"],
	"temperature_unit": "fahrenheit",
	"wind_speed_unit": "mph",
	"precipitation_unit": "inch",
	"timezone": "America/Chicago"
}
responses = openmeteo.weather_api(url, params=params)

# Process first location. Add a for-loop for multiple locations or weather models
response = responses[0]
print(f"Coordinates {response.Latitude()}°E {response.Longitude()}°N")
print(f"Elevation {response.Elevation()} m asl")
print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

# Process hourly data. The order of variables needs to be the same as requested.
hourly = response.Hourly()
hourly_soil_temperature_0_to_7cm = hourly.Variables(0).ValuesAsNumpy()
hourly_soil_temperature_7_to_28cm = hourly.Variables(1).ValuesAsNumpy()
hourly_soil_temperature_28_to_100cm = hourly.Variables(2).ValuesAsNumpy()
hourly_soil_temperature_100_to_255cm = hourly.Variables(3).ValuesAsNumpy()
hourly_soil_moisture_0_to_7cm = hourly.Variables(4).ValuesAsNumpy()
hourly_soil_moisture_7_to_28cm = hourly.Variables(5).ValuesAsNumpy()
hourly_soil_moisture_28_to_100cm = hourly.Variables(6).ValuesAsNumpy()
hourly_soil_moisture_100_to_255cm = hourly.Variables(7).ValuesAsNumpy()

hourly_data = {"date": pd.date_range(
	start = pd.to_datetime(hourly.Time(), unit = "s"),
	end = pd.to_datetime(hourly.TimeEnd(), unit = "s"),
	freq = pd.Timedelta(seconds = hourly.Interval()),
	inclusive = "left"
)}
hourly_data["soil_temperature_0_to_7cm"] = hourly_soil_temperature_0_to_7cm
hourly_data["soil_temperature_7_to_28cm"] = hourly_soil_temperature_7_to_28cm
hourly_data["soil_temperature_28_to_100cm"] = hourly_soil_temperature_28_to_100cm
hourly_data["soil_temperature_100_to_255cm"] = hourly_soil_temperature_100_to_255cm
hourly_data["soil_moisture_0_to_7cm"] = hourly_soil_moisture_0_to_7cm
hourly_data["soil_moisture_7_to_28cm"] = hourly_soil_moisture_7_to_28cm
hourly_data["soil_moisture_28_to_100cm"] = hourly_soil_moisture_28_to_100cm
hourly_data["soil_moisture_100_to_255cm"] = hourly_soil_moisture_100_to_255cm

hourly_df = pd.DataFrame(data = hourly_data)
print(hourly_df)

# Process daily data. The order of variables needs to be the same as requested.
daily = response.Daily()
daily_weather_code = daily.Variables(0).ValuesAsNumpy()
daily_temperature_2m_max = daily.Variables(1).ValuesAsNumpy()
daily_temperature_2m_min = daily.Variables(2).ValuesAsNumpy()
daily_temperature_2m_mean = daily.Variables(3).ValuesAsNumpy()
daily_precipitation_sum = daily.Variables(6).ValuesAsNumpy()
daily_rain_sum = daily.Variables(7).ValuesAsNumpy()
daily_snowfall_sum = daily.Variables(8).ValuesAsNumpy()

daily_data = {"date": pd.date_range(
	start = pd.to_datetime(daily.Time(), unit = "s"),
	end = pd.to_datetime(daily.TimeEnd(), unit = "s"),
	freq = pd.Timedelta(seconds = daily.Interval()),
	inclusive = "left"
)}
daily_data["weather_code"] = daily_weather_code
daily_data["temperature_2m_max"] = daily_temperature_2m_max
daily_data["temperature_2m_min"] = daily_temperature_2m_min
daily_data["temperature_2m_mean"] = daily_temperature_2m_mean
daily_data["precipitation_sum"] = daily_precipitation_sum
daily_data["rain_sum"] = daily_rain_sum
daily_data["snowfall_sum"] = daily_snowfall_sum

daily_df = pd.DataFrame(data = daily_data)
print(daily_df)

################################################################################
# SET VARIABLE TYPES AND MAPPINGS
################################################################################

daily_df['month'] = pd.to_datetime(daily_df['date']).dt.month
daily_df['year'] = pd.to_datetime(daily_df['date']).dt.year
daily_df['date'] = pd.to_datetime(daily_df['date']).dt.date

hourly_df = hourly_df.rename(columns = {'date' : 'datetime'})
hourly_df['date'] = pd.to_datetime(hourly_df['datetime']).dt.date
hourly_df['time'] = pd.to_datetime(hourly_df['datetime']).dt.time

################################################################################
# SUMMARIZE DATA
################################################################################

# Calculate mean daily soil temps
soiltemp = ['soil_temperature_0_to_7cm', 'soil_temperature_7_to_28cm', 'soil_temperature_28_to_100cm', 'soil_temperature_100_to_255cm']

avgsoiltemp = hourly_df.groupby(['date'])[soiltemp].mean()

avgsoiltemp = avgsoiltemp.add_suffix('_mean')

# Calculate mean daily soil moisture
soilmoist = ['soil_moisture_0_to_7cm', 'soil_moisture_7_to_28cm', 'soil_moisture_28_to_100cm', 'soil_moisture_100_to_255cm']

avgsoilmoist = hourly_df.groupby(['date'])[soilmoist].mean()

avgsoilmoist = avgsoilmoist.add_suffix('_mean')

################################################################################
# COMBINE DATA
################################################################################

daily_df = pd.merge(daily_df, avgsoiltemp, on='date')

daily_df = pd.merge(daily_df, avgsoilmoist, on='date')

################################################################################
# SAVE DATA AS CSV
################################################################################

daily_df.to_csv(filepath + 'MKE Weather Data Historical.csv', index=False)
