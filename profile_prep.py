"""
Fetch, structure climate and soil data for AquaCrop model

Portions of this code are adapted from https://github.com/kdmayer/nasa-power-api

Original work Copyright (c) 2023 Kevin Mayer
Modified work Copyright (c) 2025 Phuc Doan

Licensed under the MIT License
"""

import numpy as np
import pandas as pd
from lib.soil_api_client import Soil_client
from lib.util import ( 
    save_data, 
    load_configuration
    )
from iot_extra.lib.weather_prep import (
    fetch_weather_data, 
    clean_weather_data, 
    reformat_climate_data
    )

def profile_prep():
    # Load config
    conf = load_configuration()
    
    # Extract parameters
    latitude = conf.get('lat', 0)
    longitude = conf.get('lon', 0)
    start_date = conf.get('start_date', 0)
    end_date = conf.get('end_date', 0)
    
    # Get and process weather data 
    weather_df = fetch_weather_data(latitude, longitude, start_date, end_date)
    weather_df = clean_weather_data(weather_df)
    climate_df = reformat_climate_data(weather_df, start_date)
    
    # Save weather results
    save_data(weather_df, climate_df)
    
    # Process soil data 
    soil_client = Soil_client(lat=latitude, lon=longitude)
    soil_client.get_data()

if __name__ == '__main__':
    profile_prep()
