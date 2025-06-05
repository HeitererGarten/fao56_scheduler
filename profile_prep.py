"""
Fetch, structure climate and soil data for AquaCrop model

Portions of this code are adapted from https://github.com/kdmayer/nasa-power-api

Original work Copyright (c) 2023 Kevin Mayer
Modified work Copyright (c) 2025 Phuc Doan

Licensed under the MIT License
"""

import os
import numpy as np
import pandas as pd
from lib.soil_api_client import Soil_client
from lib.util import ( 
    save_data, 
    load_configuration
    )
from lib.weather_prep import (
    fetch_weather_data, 
    clean_weather_data, 
    reformat_climate_data
    )

# Define module-level paths, assuming this script is in 'iot_extra'
# and data files are in 'iot_extra\db\'
MODULE_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_WEATHER_REL_PATH = r"db\climate_data.txt"
DEFAULT_SOIL_REL_PATH = r"db\soil_data.csv"
RAW_WEATHER_REL_PATH = r"db\raw_weather_df.csv"

def profile_prep():
    climate_output_abs_path = os.path.join(MODULE_BASE_DIR, DEFAULT_WEATHER_REL_PATH)
    soil_abs_path = os.path.join(MODULE_BASE_DIR, DEFAULT_SOIL_REL_PATH)
    raw_weather_abs_path = os.path.join(MODULE_BASE_DIR, RAW_WEATHER_REL_PATH)    
    
    try:
        conf = load_configuration()
        latitude = conf.get('lat', 0)
        longitude = conf.get('lon', 0)
        start_date_conf = conf.get('start_date', 0)
        end_date_conf = conf.get('end_date', 0)

        weather_df = None

        if os.path.exists(raw_weather_abs_path):
            print(f"Loading existing raw weather data from: {raw_weather_abs_path}")
            weather_df = pd.read_csv(raw_weather_abs_path, sep=";")
        else:
            print(f"Raw weather data file missing at {raw_weather_abs_path}. Fetching and cleaning new data...")
            weather_df = fetch_weather_data(latitude, longitude, start_date_conf, end_date_conf)
            weather_df = clean_weather_data(weather_df)

        if weather_df is None:
            raise RuntimeError("Failed to load or fetch raw weather data (weather_df).")

        climate_df = reformat_climate_data(weather_df, start_date_conf)

        save_data(weather_df, climate_df)

        if not os.path.exists(soil_abs_path):
            print(f"Soil data file missing at {soil_abs_path}. Generating new soil data...")
            soil_client = Soil_client(lat=latitude, lon=longitude)
            soil_client.get_data()
        else:
            print(f"Soil data file already exists at: {soil_abs_path}. Skipping soil data generation.")

        if not os.path.exists(climate_output_abs_path):
            raise FileNotFoundError(
                f"Formatted climate data file ({DEFAULT_WEATHER_REL_PATH}) was not found at {climate_output_abs_path} after processing."
                )
        if not os.path.exists(soil_abs_path):
            raise FileNotFoundError(
                f"Soil data file ({DEFAULT_SOIL_REL_PATH}) was not found at {soil_abs_path} after processing."
                )

        print("Profile preparation completed successfully.")

    except FileNotFoundError:
        raise
    except Exception as e:
        raise RuntimeError(f"An error occurred during profile_prep execution: {e}")

if __name__ == '__main__':
    profile_prep()
