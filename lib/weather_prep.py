"""
Fetch and process weather and climate data for the initial DataFrame
"""

import numpy as np
import pandas as pd
from datetime import datetime 
from lib.power_api import PowerAPI
from lib.util import pm_ops

def fetch_weather_data(latitude, longitude, start_date, end_date):
    """Fetch weather data from NASA Power API"""
    nasa_weather = PowerAPI(start=pd.Timestamp(str(start_date)), 
                          end=pd.Timestamp(str(end_date)), 
                          long=longitude, lat=latitude)
    return nasa_weather.get_weather()

def clean_weather_data(weather_df):
    """Clean weather data by replacing missing values"""
    weather_df.replace(-999.0, np.nan, inplace=True)
    weather_df.ffill(inplace=True)
    return weather_df

def reformat_climate_data(weather_df, start_date):
    """Create climate dataframe in the format required by AquaCrop"""
    formatted_start_date = pd.to_datetime(str(start_date), format="%Y%m%d")
    periods = len(weather_df)
    date_range = pd.date_range(start=formatted_start_date, periods=periods, freq='D')
    
    # Transform datetime to the latest year so daily climate update is easier
    yr_gap = datetime.now().year - date_range[0].year
    # Edit-on-self is ill-advised, hence the copied df
    updated_date_range = date_range.map(lambda dt: dt.replace(year=dt.year + yr_gap))
    
    climate = pd.DataFrame({
        'Day': updated_date_range.day,
        'Month': updated_date_range.month,
        'Year': updated_date_range.year,
        'MinTemp': weather_df['T2M_MIN'],
        'MaxTemp': weather_df['T2M_MAX'],
        'Precipitation': weather_df['PRECTOTCORR'],
        'ReferenceET': np.nan,
    })
    
    climate['ReferenceET'] = weather_df.apply(pm_ops, axis=1)
    return climate.round(2).reset_index(drop=True)
