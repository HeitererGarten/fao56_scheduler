from httpx import Client
import pandas as pd

def soil_client_test():
    with Client() as client:
        # Get the mean value of the soil property at the queried location and depth
        response = client.get(
            url="https://api.openepi.io/soil/property",
            params={
                "lat": 23,
                "lon": 12,
                "depths": "30-60cm",
                "properties": "soc",
                "values": "mean",
            },
        )

        json = response.json()

        # Get the soil information for the bdod property
        bdod = json["properties"]["layers"][0]

        # Get the soil property unit and name
        bdod_name = bdod["name"]
        bdod_unit = bdod["unit_measure"]["mapped_units"]

        # Get the soil property mean value at depth 0-5cm
        bdod_depth = bdod["depths"][0]["label"]
        bdod_value = bdod["depths"][0]["values"]["mean"]

        print(
            f"Soil property: {bdod_name}, Depth: {bdod_depth}, Value: {bdod_value} {bdod_unit}"
        )
        print(json)

def climate_manip():
    climate_df = pd.read_csv("climate_data.txt", sep=r"\s+")
    print(climate_df.columns.tolist())
    mask = climate_df["Precipitation"] < 1
    climate_df.loc[mask, "Precipitation"] = 0
    climate_df.to_string(buf="climate_data.txt")
    
climate_manip()
