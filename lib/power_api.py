"""
Query the NASA Power API.

Portions of this code are adapted from https://github.com/kdmayer/nasa-power-api

Original work Copyright (c) 2023 Kevin Mayer

Modified work Copyright (c) 2025 Phuc Doan

Licensed under the MIT License
"""


from typing import List, Union, Optional
from pathlib import Path
from datetime import date, datetime
import requests
import pandas as pd
import os
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


class PowerAPI:
    """
    Query the NASA Power API.
    Check https://power.larc.nasa.gov/ for documentation
    Attributes
    ----------
    url : str
        Base URL
    """
    url = "https://power.larc.nasa.gov/api/temporal/daily/point?"

    def __init__(self,
                 start: Union[date, datetime, pd.Timestamp],
                 end: Union[date, datetime, pd.Timestamp],
                 long: float, lat: float,
                 use_long_names: bool = False,
                 parameter: Optional[List[str]] = None):
        """
        Parameters
        ----------
        start: Union[date, datetime, pd.Timestamp]
        end: Union[date, datetime, pd.Timestamp]
        long: float
            Longitude as float
        lat: float
            Latitude as float
        use_long_names: bool
            NASA provides both identifier and human-readable names for the fields. If set to True this will parse
            the data with the latter
        parameter: Optional[List[str]]
            List with the parameters to query.
            Default is ['T2M_MAX', 'T2M_MIN', 'T2MDEW', 'T2M', 'ALLSKY_SFC_SW_DWN', 'ALLSKY_SFC_SW_UP', 
                        'ALLSKY_SFC_LW_DWN', 'ALLSKY_SFC_LW_UP', 'PS', 'WS2M', 'PRECTOTCORR']
        """
        self.start = start
        self.end = end
        self.long = long
        self.lat = lat
        self.use_long_names = use_long_names
        if parameter is None:
            self.parameter = ['T2M_MAX', 'T2M_MIN', 'T2MDEW', 'T2M', 'ALLSKY_SFC_SW_DWN', 'ALLSKY_SFC_SW_UP', 
                              'ALLSKY_SFC_LW_DWN', 'ALLSKY_SFC_LW_UP', 'PS', 'WS2M', 'PRECTOTCORR']

        self.request = self._build_request()

    def _build_request(self) -> str:
        """
        Build the request
        Returns
        -------
        str
            Full request including parameter
        """
        r = self.url
        r += f"parameters={(',').join(self.parameter)}"
        r += '&community=RE'
        r += f"&longitude={self.long}"
        r += f"&latitude={self.lat}"
        r += f"&start={self.start.strftime('%Y%m%d')}"
        r += f"&end={self.end.strftime('%Y%m%d')}"
        r += '&format=JSON'

        return r

    def get_weather(self) -> pd.DataFrame:
        """
        Main method to query the weather data
        
        Returns
        -------
        pd.DataFrame
            Pandas DataFrame with DateTimeIndex. Returns an empty DataFrame
            if the request fails. Check df.empty to verify success and
            df.attrs['error_message'] for error details if empty.
        """
        
        response = requests.get(self.request)
        if response.status_code == 200:
            data_json = response.json()
            records = data_json['properties']['parameter']
            
            df = pd.DataFrame.from_dict(records)
            return df
        else:
            error_df = pd.DataFrame()
            error_df.attrs['error_message'] = f"HTTP {response.status_code}: {response.text}"
            print(f"Error: {response.status_code} - {response.text}")
            return error_df

