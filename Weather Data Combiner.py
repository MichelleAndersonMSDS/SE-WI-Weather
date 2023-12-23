"""
DATA GATHERING AND PRE-PROCESSING SCRIPT FOR MILWAUKEE WEATHER DATA
STEP 4 of 4

Created on Fri Dec 22 2023
@author: Michelle.Anderson

This code performs the following tasks:
  1.  Run year-to-date data gathering py script
  2.  Run prediction data gathering py script
  3.  Open historical data CSV file
  4.  Merge historical, year-to-date, and historical data sets
  5.  Conform and augment data
  6.  Calculate 7 and 14 day rolling averages for air and soil temperature
  7.  Calculate 7 day precipitation totals
  8.  Save processed data as CSV file
"""

################################################################################
# LOAD LIBRARIES
################################################################################

import pandas as pd
import numpy as np

################################################################################
# SET PARAMETERS
################################################################################

filepath = '/Users/michelleanderson/Documents/Gardening Dashboard/'

################################################################################
# RUN DATA GATHERER SCRIPTS
################################################################################

with open(filepath + "YTD Weather Data Gatherer.py") as f:
    exec(f.read())
    
with open(filepath + "Prediction Weather Data Gatherer.py") as f:
    exec(f.read())

################################################################################
# READ IN DATA
################################################################################

hist_df = pd.read_csv(filepath + 'MKE Weather Data Historical.csv')

ytd_df = pd.read_csv(filepath + 'MKE Weather Data YTD.csv')

pred_df = pd.read_csv(filepath + 'MKE Weather Data Prediction.csv')

################################################################################
# ADD METADATA
################################################################################

hist_df['Data Source'] = 'Historical'

ytd_df['Data Source'] = 'YTD'

pred_df['Data Source'] = 'Prediction'

################################################################################
# COMBINE DATA
################################################################################

full_df = pd.concat([hist_df, ytd_df, pred_df], ignore_index = True)

################################################################################
# AUGMENT DATA
################################################################################

WMO = {0	: 'Clear sky', 1 : 'Mainly clear', 2 : 'Partly cloudy', 3	: 'Overcast', 45 : 'Fog' , 48 : 'Depositing rime fog', 
51 : 'Drizzle: Light', 53 : 'Drizzle: Moderate', 55 : 'Drizzle: Dense intensity', 56 : 'Freezing Drizzle: Light', 
57 : 'Freezing Drizzle: Dense intensity', 61 : 'Rain: Slight', 63 : 'Rain: Moderate', 65 : 'Rain: Heavy intensity',
66 : 'Freezing Rain: Light', 67 : 'Freezing Rain: Heavy intensity', 71 : 'Snow fall: Slight', 73 : 'Snow fall: Moderate',
75 : 'Snow fall: Heavy intensity', 77 : 'Snow grains', 80 : 'Rain showers: Slight', 81 : 'Rain showers: Moderate',
82 : 'Rain showers: Violent', 85 : 'Snow showers slight', 86 : 'Snow showers heavy', 95 : 'Thunderstorm: Slight or moderate',
96 : 'Thunderstorm with slight hail', 99 : 'Thunderstorm with heavy hail'}

full_df['weather_code_category'] = full_df['weather_code'].map(WMO)

full_df['temperature_2m_7dayavg'] = full_df['temperature_2m_mean'].rolling(7).mean()
full_df['temperature_2m_14dayavg'] = full_df['temperature_2m_mean'].rolling(14).mean()

full_df['soil_temperature_0_to_7cm_7dayavg'] = full_df['soil_temperature_0_to_7cm_mean'].rolling(7).mean()
full_df['soil_temperature_7_to_28cm_7dayavg'] = full_df['soil_temperature_7_to_28cm_mean'].rolling(7).mean()
full_df['soil_temperature_28_to_100cm_7dayavg'] = full_df['soil_temperature_28_to_100cm_mean'].rolling(7).mean()
full_df['soil_temperature_100_to_255cm_7dayavg'] = full_df['soil_temperature_100_to_255cm_mean'].rolling(7).mean()

full_df['soil_temperature_0_to_7cm_14dayavg'] = full_df['soil_temperature_0_to_7cm_mean'].rolling(14).mean()
full_df['soil_temperature_7_to_28cm_14dayavg'] = full_df['soil_temperature_7_to_28cm_mean'].rolling(14).mean()
full_df['soil_temperature_28_to_100cm_14dayavg'] = full_df['soil_temperature_28_to_100cm_mean'].rolling(14).mean()
full_df['soil_temperature_100_to_255cm_14dayavg'] = full_df['soil_temperature_100_to_255cm_mean'].rolling(14).mean()

full_df['precipitation_sum_7day'] = full_df['precipitation_sum'].rolling(7).sum()
full_df['rain_sum_7day'] = full_df['rain_sum'].rolling(7).sum()
full_df['snow_sum_7day'] = full_df['snowfall_sum'].rolling(7).sum()

full_df['relative_date'] = np.where(full_df['date'] == date.today().strftime('%Y-%m-%d'), "Current date", 
                           np.where(full_df['date'] > date.today().strftime('%Y-%m-%d'), "Prediction", 
                           np.where(full_df['date'] < date.today().strftime('%Y-%m-%d'), "Historical", "Unknown")))

################################################################################
# SAVE DATA AS CSV
################################################################################

full_df.to_csv(filepath + 'MKE Weather Data CUMULATIVE.csv', index=False)

