# Southeast Wisconsin spring planting tracker
### Description
This project includes code for the data pipeline that supports the [Southeast Wisconsin spring planting tracker dashboard](https://public.tableau.com/views/Gardeningviz/Gardentracker). The dashboard monitors air and soil temperatures in Southeast Wisconsin to help gardeners choose the right time to plant their crops.  The repository includes the following files:

A python script [MKE Eviction Data.py] to gather, transform, and augment Milwaukee County eviction filing data from The Eviction Lab.
An R script [ACS5 Data Gathering Script.R] to gather, transform, and augment housing-related American Community Survey data using an US Census Bureau API.
An R script [MKE Shapefile Gatherer.R] to create a shapefile that includes current and historical geometries.
