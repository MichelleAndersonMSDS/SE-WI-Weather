# Southeast Wisconsin spring planting tracker
### Description
This project includes code for the data pipeline that supports the [Southeast Wisconsin spring planting tracker dashboard](https://public.tableau.com/views/Gardeningviz/Gardentracker). The dashboard monitors air and soil temperatures in Southeast Wisconsin to help gardeners choose the right time to plant their crops.  The repository includes the following files:

1.  A python script [Historical Weather Data Gatherer.py] to gather, transform, and augment weather data for Southeast Wisconsin as far back as 1940 using the open-meteo API.
2.  A python script [YTD Weather Data Gatherer.py] to gather, transform, and augment weather data for Southeast Wisconsin for the current year to date using the open-meteo API.
3.  A python script [Prediction Weather Data Gatherer.py] to gather, transform, and augment weather prediction data for Southeast Wisconsin for the next 7 days using the open-meteo API.
4.  A python script [Weather Data Combiner.py] that calls the above scripts and combines each data frame into a single dataset for analytics.  
