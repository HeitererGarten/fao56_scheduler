import pandas as pd
import os
import asyncio
from httpx import AsyncClient, HTTPError

class Soil_client:
    def __init__(self, lat, lon) -> None:
        self.lat = lat 
        self.lon = lon
        # Store depth details with thickness in meters
        self.depth_details = {
            "0-30cm": 0.3,
            "30-60cm": 0.3,
            "60-100cm": 0.4,
            "100-200cm": 1.0
        }
        self.depths = list(self.depth_details.keys()) # Order of depths for API and processing
        self.api_properties = ["clay", "sand", "soc"] # Properties to request from API
        self.df_properties = ["clay", "sand", "om"] # Properties for DataFrame (om for organic matter)
        self.values = ["mean"]
         
    async def get_data_async(self):
        """Asynchronously fetch soil data for each depth with error handling."""
        results = {}
        
        async with AsyncClient() as client:
            for depth in self.depths:
                try:
                    print(f"Fetching data for depth: {depth}")
                    res = await client.get(
                        url="https://api.openepi.io/soil/property",
                        params={
                            "lat": self.lat,
                            "lon": self.lon,
                            "depths": [depth],
                            "properties": self.api_properties, # Use updated api_properties
                            "values": self.values,
                        },
                        timeout=10.0  
                    )
                    res.raise_for_status()  # Check for HTTP errors
                    data = res.json()
                    results[depth] = data
                    print(f"Successfully fetched data for depth: {depth}")
                    
                    # Add a small delay to avoid overwhelming the API
                    await asyncio.sleep(0.5)
                    
                except HTTPError as e:
                    print(f"HTTP error for depth {depth}: {e}")
                except Exception as e:
                    print(f"Unexpected error for depth {depth}: {e}")
        
        return results

    def extract_and_save_soil_data(self, results):
        """Extract soil properties, convert to percentages, and save to DataFrame."""
        try:
            depth_data = {}
            for depth_str, result in results.items():
                thickness_m = self.depth_details.get(depth_str)
                # Initialize soil_values with depth, thickness, and None for df_properties
                current_soil_values = {'depth': depth_str, 'thickness': thickness_m}
                for prop_key in self.df_properties:
                    current_soil_values[prop_key] = None
                
                layers = result.get('properties', {}).get('layers', [])
                
                if not layers:
                    print(f"No soil data available for depth: {depth_str}")
                    depth_data[depth_str] = current_soil_values # Store initialized values
                    continue
                
                for layer in layers:
                    property_code = layer.get('code')
                    if property_code not in self.api_properties: # Check against API properties
                        continue
                        
                    conversion_factor = layer.get('unit_measure', {}).get('conversion_factor', 10)
                    mean_value = layer.get('depths', [{}])[0].get('values', {}).get('mean')
                    
                    if mean_value is not None:
                        if property_code in ["clay", "sand"]:
                            # Originally in g/kg. Convert to %
                             current_soil_values[property_code] = (mean_value / conversion_factor)
                        elif property_code == "soc":
                            # SOC is in dg/kg, convert to %
                            percentage = mean_value / 100
                            current_soil_values["om"] = percentage 
                
                depth_data[depth_str] = current_soil_values

            if not any(any(v is not None for k, v in data.items() if k in self.df_properties) for data in depth_data.values()):
                print("No valid soil data found across all layers.")
                return None

            ordered_depths = self.depths 
            
            # Ensure all ordered_depths keys exist in depth_data, with thickness
            for depth_key in ordered_depths:
                if depth_key not in depth_data:
                    thickness_m = self.depth_details.get(depth_key)
                    initialized_values = {'depth': depth_key, 'thickness': thickness_m}
                    for prop_key in self.df_properties:
                        initialized_values[prop_key] = None
                    depth_data[depth_key] = initialized_values
                    print(f"Layer {depth_key} was missing from API response, initialized as empty.")

            # Special handling for completely missing layers (filling based on nearest)
            for depth_to_fill in ordered_depths:
                is_empty = all(depth_data[depth_to_fill][prop] is None for prop in self.df_properties)
                
                if is_empty:
                    available_depths_with_data = [
                        d for d in ordered_depths 
                        if d in depth_data and not all(depth_data[d][prop] is None for prop in self.df_properties)
                    ]
                    if available_depths_with_data:
                        nearest_depth_with_data = min(available_depths_with_data, key=lambda x: abs(
                            ordered_depths.index(x) - ordered_depths.index(depth_to_fill)))
                        
                        for prop_to_copy in self.df_properties: # Use df_properties
                            depth_data[depth_to_fill][prop_to_copy] = depth_data[nearest_depth_with_data].get(prop_to_copy)
                        print(f"Filled completely missing layer {depth_to_fill} using data from {nearest_depth_with_data}")

            # Fill missing values within existing layers
            for i, depth in enumerate(ordered_depths):
                current_values = depth_data[depth]
                
                for property_type in self.df_properties: # Iterate through df_properties
                    if current_values.get(property_type) is not None:
                        continue
                    
                    above_value = None
                    below_value = None
                    
                    # Look up for values
                    for j in range(i - 1, -1, -1):
                        prev_depth_key = ordered_depths[j]
                        if prev_depth_key in depth_data and depth_data[prev_depth_key].get(property_type) is not None:
                            above_value = depth_data[prev_depth_key][property_type]
                            break
                    
                    # Look down for values
                    for j in range(i + 1, len(ordered_depths)):
                        next_depth_key = ordered_depths[j]
                        if next_depth_key in depth_data and depth_data[next_depth_key].get(property_type) is not None:
                            below_value = depth_data[next_depth_key][property_type]
                            break
                    
                    if above_value is not None:
                        current_values[property_type] = above_value
                        print(f"Filled missing {property_type} at {depth} with value from above ({ordered_depths[i-1] if i > 0 else 'N/A'})")
                    elif below_value is not None:
                        current_values[property_type] = below_value
                        print(f"Filled missing {property_type} at {depth} with value from below ({ordered_depths[i+1] if i < len(ordered_depths)-1 else 'N/A'})")

            # Construct the data list for DataFrame in the correct order
            final_data_list = []
            for depth_key in ordered_depths:
                if depth_key in depth_data:
                    final_data_list.append(depth_data[depth_key])
                else: 
                    # This case should be handled by the initialization above
                    thickness_m = self.depth_details.get(depth_key)
                    empty_init = {'depth': depth_key, 'thickness': thickness_m}
                    for prop_key in self.df_properties:
                        empty_init[prop_key] = None
                    final_data_list.append(empty_init)

            soil_df = pd.DataFrame(final_data_list)
            # Reorder columns: 'depth', 'thickness', then df_properties
            cols = ['depth', 'thickness'] + [p for p in self.df_properties if p in soil_df.columns]
            soil_df = soil_df[cols]
            
            # Determine file path 
            current_file = os.path.abspath(__file__)
            lib_dir = os.path.dirname(current_file)
            parent_dir = os.path.dirname(lib_dir)
            db_dir = os.path.join(parent_dir, 'db')
                
            # Create db directory if it doesn't exist
            os.makedirs(db_dir, exist_ok=True)
            
            # Save to CSV in the db directory
            filename = f"soil_data.csv"
            filepath = os.path.join(db_dir, filename)
            soil_df.to_csv(filepath, index=False)
            
            print("\nFinal soil composition:")
            print(soil_df)
            
            return soil_df
            
        except Exception as e:
            print(f"Error extracting and saving soil data: {e}")
            return None

    # Update the get_data method to call the new function
    def get_data(self):
        """Synchronous wrapper to fetch soil data for all depths."""
        try:
            # Use asyncio to run the async method
            results = asyncio.run(self.get_data_async())
            
            if results:                
                # Extract and save the data
                soil_df = self.extract_and_save_soil_data(results)
                return soil_df
            else:
                print("No data was collected.")
                return None
                
        except Exception as e:
            print(f"Error in get_data: {e}")
            return None
            
if __name__ == "__main__":
    example_client = Soil_client(23, 12)
    example_client.get_data()



