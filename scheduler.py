import numpy as np
import pandas as pd
from datetime import datetime 
from scipy.optimize import fmin
from aquacrop import AquaCropModel, Soil, Crop, InitialWaterContent, IrrigationManagement
from aquacrop.utils import prepare_weather
import multiprocessing
from functools import partial
import os
from iot_extra.profile_prep import profile_prep

# Determine the absolute path to the directory where this module is located
MODULE_BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Define default relative paths for data files, relative to the module's directory
DEFAULT_WEATHER_REL_PATH = r"db\climate_data.txt"
DEFAULT_SOIL_REL_PATH = r"db\soil_data.csv"
DEFAULT_OUTPUT_REL_PATH = r"db\optimized_irr_schedule.csv"

def _run_model_opt(
    smt_values, 
    max_irr_season_value, 
    sim_year1, 
    sim_year2,
    weather_file_abs_path_str, # Expects absolute path
    soil_data_abs_path_str,   # Expects absolute path
    crop_name_str,
    plant_date_str, # MM/DD format
    soil_type_str,
    soil_dz_list,
    initial_wc_config_dict,
    irrigation_method_int
    ):
    """
    Internal function to run the AquaCrop model for a single evaluation during optimization.
    Returns model simulation results (pandas DataFrame).
    """
    weather_data = prepare_weather(weather_file_abs_path_str)
    soil_data_df = pd.read_csv(soil_data_abs_path_str, sep=',')
    
    crop_obj = Crop(c_name=crop_name_str, planting_date=plant_date_str)
    
    soil_obj = Soil(soil_type=soil_type_str, dz=soil_dz_list)
    for _, layer in soil_data_df.iterrows():
        soil_obj.add_layer_from_texture(
            thickness=layer["thickness"],
            Sand=layer["sand"],
            Clay=layer["clay"],
            OrgMat=layer["om"],
            penetrability=100 # Assumption from notebook
        )
    
    iwc_obj = InitialWaterContent(
        wc_type=initial_wc_config_dict['wc_type'],
        method=initial_wc_config_dict['method'],
        depth_layer=initial_wc_config_dict['depth_layer'],
        value=initial_wc_config_dict['value']
    )
    
    irr_mgnt_obj = IrrigationManagement(
        irrigation_method=irrigation_method_int, 
        SMT=list(smt_values), 
        MaxIrrSeason=max_irr_season_value
    )
    
    model = AquaCropModel(
        sim_start_time=f"{sim_year1}/01/01",
        sim_end_time=f"{sim_year2}/12/31",
        weather_df=weather_data,
        soil=soil_obj,
        crop=crop_obj,
        initial_water_content=iwc_obj,
        irrigation_management=irr_mgnt_obj
    )
    
    model.run_model(till_termination=True)
    return model.get_simulation_results()

def _eval_smt(
    smt_values_to_test, 
    # Args for fmin start here
    max_irr_season_value, 
    sim_year1, 
    sim_year2,
    weather_file_abs_path_str, # Expects absolute path
    soil_data_abs_path_str,   # Expects absolute path
    crop_name_str,
    plant_date_str,
    soil_type_str,
    soil_dz_list,
    initial_wc_config_dict,
    irrigation_method_int,
    is_test_run=False 
    ):
    """
    Evaluates a set of SMTs, used by the optimization algorithm.
    """
    out_results = _run_model_opt(
        smt_values=smt_values_to_test, 
        max_irr_season_value=max_irr_season_value, 
        sim_year1=sim_year1, 
        sim_year2=sim_year2,
        weather_file_abs_path_str=weather_file_abs_path_str,
        soil_data_abs_path_str=soil_data_abs_path_str,
        crop_name_str=crop_name_str,
        plant_date_str=plant_date_str,
        soil_type_str=soil_type_str,
        soil_dz_list=soil_dz_list,
        initial_wc_config_dict=initial_wc_config_dict,
        irrigation_method_int=irrigation_method_int
    )
    
    yld = out_results['Dry yield (tonne/ha)'].mean()
    reward = yld

    if is_test_run: 
        tirr = out_results['Seasonal irrigation (mm)'].mean()
        return yld, tirr, reward
    else:
        return -reward 

# Helper function for parallel execution in _find_start_smt
def _eval_smt_start(xtest_tuple, common_args_tuple):
    """Helper function to evaluate a single SMT set for finding a good starting point."""
    return _eval_smt(xtest_tuple, *common_args_tuple)

def _find_start_smt(
    num_smts, 
    max_irr_season_value, 
    num_searches,
    sim_year1, 
    sim_year2,
    weather_file_abs_path_str, # Expects absolute path
    soil_data_abs_path_str,   # Expects absolute path
    crop_name_str,
    plant_date_str,
    soil_type_str,
    soil_dz_list,
    initial_wc_config_dict,
    irrigation_method_int
    ):
    """
    Finds a good starting SMT set for optimization by random search, potentially in parallel.
    """
    x0list = np.random.rand(num_searches, num_smts) * 100
    
    common_args_for_eval = (
        max_irr_season_value, 
        sim_year1, sim_year2,
        weather_file_abs_path_str, soil_data_abs_path_str,
        crop_name_str, plant_date_str, soil_type_str, soil_dz_list,
        initial_wc_config_dict, irrigation_method_int,
        False # is_test_run = False for optimization
    )

    tasks = [(tuple(xtest), common_args_for_eval) for xtest in x0list]
    rlist = []
    try:
        # Ensure AquaCrop and its dependencies are safe with multiprocessing
        # For CPU-bound tasks like this, 'spawn' or 'forkserver' might be more robust on some platforms
        # than the default 'fork' on Unix. Windows default is 'spawn'.
        # ctx = multiprocessing.get_context('spawn') # Optionally specify context
        # with ctx.Pool(processes=min(num_searches, multiprocessing.cpu_count(), 4)) as pool: # Limit processes if needed
        with multiprocessing.Pool(processes=min(num_searches, multiprocessing.cpu_count())) as pool:
            rlist = pool.starmap(_eval_smt_start, tasks)
    except Exception as e:
        print(f"Multiprocessing for starting point failed: {e}. Falling back to serial execution.")
        rlist = []
        for xtest_tuple, common_args in tasks: # Iterate through prepared tasks
            r = _eval_smt_start(xtest_tuple, common_args)
            rlist.append(r)
            
    if not rlist: # Should not happen if fallback works, but as a safeguard
        print("Error: rlist is empty after attempting to find starting point.")
        # Fallback to a default starting point or raise an error
        return np.array([50.0] * num_smts) # Example default

    x0 = x0list[np.argmin(rlist)]
    return x0

def _opt_smt(
    num_smts, 
    max_irr_season_value, 
    num_searches,
    sim_year1, 
    sim_year2,
    weather_file_abs_path_str, # Expects absolute path
    soil_data_abs_path_str,   # Expects absolute path
    crop_name_str,
    plant_date_str,
    soil_type_str,
    soil_dz_list,
    initial_wc_config_dict,
    irrigation_method_int
    ):
    """
    Optimizes SMTs to maximize yield using scipy.optimize.fmin.
    """
    args_for_evaluate = (
        max_irr_season_value, 
        sim_year1, sim_year2,
        weather_file_abs_path_str, soil_data_abs_path_str,
        crop_name_str, plant_date_str, soil_type_str, soil_dz_list,
        initial_wc_config_dict, irrigation_method_int,
        False 
    )

    x0 = _find_start_smt(
        num_smts, max_irr_season_value, num_searches,
        sim_year1, sim_year2,
        weather_file_abs_path_str, soil_data_abs_path_str,
        crop_name_str, plant_date_str, soil_type_str, soil_dz_list,
        initial_wc_config_dict, irrigation_method_int
    )
    
    res = fmin(_eval_smt, x0, args=args_for_evaluate, disp=0)
    optimal_smts = res.squeeze() 
    return optimal_smts

def generate_schedule(
    sim_start_date: str,  # "YYYY/MM/DD"
    sim_end_date: str,    # "YYYY/MM/DD"
    plant_date: str,      # "MM/DD"
    crop_name: str, 
    soil_type: str, 
    soil_dz: list,        # e.g., [0.3, 0.3, 0.4, 1.0]
    initial_wc_config: dict, 
    irrigation_method: int,
    max_irr_season_for_optimization: float,
    num_smts_to_optimize: int = 4, 
    num_searches_for_starting_point: int = 100 
    ):
    """
    Calculates optimal SMT values using internally defined file paths, 
    runs AquaCrop model with these SMTs, and saves the resulting 
    irrigation schedule to a CSV file (also at an internally defined path).

    Returns:
        tuple: (optimal_smts_array, absolute_output_csv_filepath_str)
    """
    # Construct absolute paths for default files
    weather_abs_path = os.path.join(MODULE_BASE_DIR, DEFAULT_WEATHER_REL_PATH)
    soil_abs_path = os.path.join(MODULE_BASE_DIR, DEFAULT_SOIL_REL_PATH)
    output_abs_csv_filepath = os.path.join(MODULE_BASE_DIR, DEFAULT_OUTPUT_REL_PATH)

    # Verify that input files exist at the determined paths.
    # If files are missing, call profile_prep to try and generate them.
    try:
        profile_prep() 
    except FileNotFoundError as e:
        # If profile_prep raises FileNotFoundError, it means files were missing and could not be generated.
        raise FileNotFoundError(f"Required data files are missing and could not be generated by profile_prep: {e}")
    except RuntimeError as e:
        # If profile_prep raises RuntimeError, it means an error occurred during the generation process.
        raise RuntimeError(f"Error during profile_prep execution: {e}")

    try:
        sim_year1 = int(sim_start_date.split('/')[0])
        sim_year2 = int(sim_end_date.split('/')[0])
    except Exception as e:
        raise ValueError(f"Invalid sim_start_date or sim_end_date format. Expected YYYY/MM/DD. Error: {e}")

    print(f"Optimizing SMTs for {crop_name} with MaxIrrSeason = {max_irr_season_for_optimization} mm...")
    print(f"Using weather data: {weather_abs_path}")
    print(f"Using soil data: {soil_abs_path}")
    
    optimal_smts = _opt_smt(
        num_smts=num_smts_to_optimize,
        max_irr_season_value=max_irr_season_for_optimization,
        num_searches=num_searches_for_starting_point,
        sim_year1=sim_year1,
        sim_year2=sim_year2,
        weather_file_abs_path_str=weather_abs_path,
        soil_data_abs_path_str=soil_abs_path,
        crop_name_str=crop_name,
        plant_date_str=plant_date,
        soil_type_str=soil_type,
        soil_dz_list=soil_dz,
        initial_wc_config_dict=initial_wc_config,
        irrigation_method_int=irrigation_method
    )
    
    optimal_smts = np.clip(optimal_smts, 0, 100)
    print(f"Optimal SMTs found: {optimal_smts}")

    print(f"Running final simulation with optimal SMTs to generate schedule...")
    
    weather_data_final = prepare_weather(weather_abs_path)
    soil_data_df_final = pd.read_csv(soil_abs_path, sep=',')
    
    crop_final = Crop(c_name=crop_name, planting_date=plant_date)
    
    soil_final = Soil(soil_type=soil_type, dz=soil_dz)
    for _, layer in soil_data_df_final.iterrows():
        soil_final.add_layer_from_texture(
            thickness=layer["thickness"], Sand=layer["sand"],
            Clay=layer["clay"], OrgMat=layer["om"], penetrability=100
        )
        
    iwc_final = InitialWaterContent(
        wc_type=initial_wc_config['wc_type'],
        method=initial_wc_config['method'],
        depth_layer=initial_wc_config['depth_layer'],
        value=initial_wc_config['value']
    )
    
    irr_mgnt_final = IrrigationManagement(
        irrigation_method=irrigation_method, 
        SMT=optimal_smts.tolist(), 
        MaxIrrSeason=max_irr_season_for_optimization
    )
    
    final_model = AquaCropModel(
        sim_start_time=sim_start_date,
        sim_end_time=sim_end_date,
        weather_df=weather_data_final,
        soil=soil_final,
        crop=crop_final,
        initial_water_content=iwc_final,
        irrigation_management=irr_mgnt_final
    )
    
    final_model.run_model(till_termination=True)
    
    date_range = pd.date_range(start=sim_start_date, end=sim_end_date, name='Date')
    model_output_flux = final_model._outputs.water_flux
    
    schedule_df = pd.DataFrame(index=date_range)
    irr_day_values = model_output_flux['IrrDay'].values
    len_to_use = min(len(irr_day_values), len(schedule_df))
    schedule_df['IrrDay'] = 0.0  
    schedule_df.loc[schedule_df.index[:len_to_use], 'IrrDay'] = irr_day_values[:len_to_use]
    
    schedule_df.reset_index(inplace=True) 
    schedule_df['Year'] = schedule_df['Date'].dt.year
    schedule_df['Month'] = schedule_df['Date'].dt.month
    schedule_df['Day'] = schedule_df['Date'].dt.day
    
    schedule_df_to_save = schedule_df[['Year', 'Month', 'Day', 'IrrDay']]
    
    os.makedirs(os.path.dirname(output_abs_csv_filepath), exist_ok=True)
    schedule_df_to_save.to_csv(output_abs_csv_filepath, sep=',', index=False)
    print(f"Optimized irrigation schedule saved to {output_abs_csv_filepath}")

    return optimal_smts, output_abs_csv_filepath

if __name__ == '__main__':
    
    curr_yr = datetime.now().year
    # Do not change any params
    sim_parameters = {
        "sim_start_date": f'{curr_yr}/01/01',
        "sim_end_date": f'{curr_yr+1}/12/31',
        "plant_date": '11/15', 
        "crop_name": 'Potato',
        "soil_type": 'ClayLoam',
        "soil_dz": [0.3, 0.3, 0.4, 1.0], 
        "initial_wc_config": {
            'wc_type': 'Prop',
            'method': 'Depth',
            'depth_layer': [1, 2, 3, 4], 
            'value': ["FC", "SAT", "WP", "WP"]
        },
        "irrigation_method": 1, 
        "max_irr_season_for_optimization": 200.0, 
        "num_smts_to_optimize": 4,
        "num_searches_for_starting_point": 5 
    }

    print("Starting schedule generation process...")
    try:
        # Ensure the script and its 'db' subdirectory are structured as expected:
        # MODULE_BASE_DIR/
        # |-- aquacrop_optimizer.py
        # |-- db/
        #     |-- climate_data.txt
        #     |-- soil_data.csv
        #     |-- (optimized_irr_schedule.csv will be created here)

        optimal_smts_result, schedule_file_path = generate_schedule(**sim_parameters)
        
        print(f"\nProcess complete.")
        print(f"Optimal SMTs: {optimal_smts_result}")
        print(f"Schedule saved to: {schedule_file_path}")

    except FileNotFoundError as e:
        print(f"ERROR: A required data file was not found.")
        print(f"Details: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()
