"""
Various wrapper and utility functions to be used for data processing.
"""

import yaml
from . import aqcrop_eto 
from . import unit_conversion 

def pm_ops(row):
    """
    Apply FAO-56 Penman-Monteith equation to calculate reference evapotranspiration.
    
    Parameters
    ----------
    row : pandas.Series
        Row from a DataFrame containing the following NASA POWER API values:
        - T2M_MAX: Maximum temperature (°C)
        - T2M_MIN: Minimum temperature (°C)
        - T2MDEW: Dew-point temperature (°C)
        - ALLSKY_SFC_SW_DWN: Incoming shortwave radiation (MJ m-2 day-1)
        - ALLSKY_SFC_SW_UP: Outgoing shortwave radiation (MJ m-2 day-1)
        - ALLSKY_SFC_LW_DWN: Incoming longwave radiation (MJ m-2 day-1)
        - ALLSKY_SFC_LW_UP: Outgoing longwave radiation (MJ m-2 day-1)
        - PS: Surface atmosphere pressure (kPa)
        - WS2M: Wind speed at 2 meters (m/s)
    
    Returns
    -------
    float
        Reference evapotranspiration (ETo) in mm/day.
        Returns 0 if calculated ETo is negative.
    """
    ETo = aqcrop_eto.fao56_penman_monteith(
        net_rad=(
            (row['ALLSKY_SFC_SW_DWN'] - row['ALLSKY_SFC_SW_UP']) +  # Net shortwave radiation (positive)
            (row['ALLSKY_SFC_LW_DWN'] - row['ALLSKY_SFC_LW_UP'])    # Net longwave radiation (negative)
        ),
        t=unit_conversion.celsius2kelvin(aqcrop_eto.daily_mean_t(row['T2M_MIN'], row['T2M_MAX'])),
        ws=row['WS2M'],
        svp=aqcrop_eto.svp_from_t(aqcrop_eto.daily_mean_t(row['T2M_MIN'], row['T2M_MAX'])),
        avp=aqcrop_eto.avp_from_tdew(row['T2MDEW']),
        delta_svp=aqcrop_eto.delta_svp(aqcrop_eto.daily_mean_t(row['T2M_MIN'], row['T2M_MAX'])),
        psy=aqcrop_eto.psy_const(row['PS']),
        shf=0  # Soil heat flux, assumed to be 0 for daily calculations
    )
    return max(ETo, 0)  # Return 0 if ETo is negative for practical purposes

def load_configuration(config_path='config.yaml'):
    """Load and return configuration from YAML file"""
    with open(config_path, 'rb') as f:
        conf = yaml.load(f, Loader=yaml.FullLoader)
    return conf

def save_data(weather_df, climate_df, raw_path="db/raw_weather_df.csv", climate_path="db/climate_data.txt"):
    """Save data to specified paths"""
    weather_df.to_csv(raw_path, sep=";")
    climate_df.to_string(buf=climate_path)
