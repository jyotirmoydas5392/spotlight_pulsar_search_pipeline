import os
import sys
import time
import argparse
import logging
import numpy as np

# Get the base directory from the environment variable
base_dir = os.getenv("PULSELINE_DEV_DIR")
if not base_dir:
    print("Error: PULSELINE_DEV_DIR environment variable is not set.")
    sys.exit(1)

# List of relative paths to add dynamically
relative_paths = [
    "input_file_dir_init/scripts",
    "SPOTLIGHT_PULSELINE/scripts",
]

# Loop through and add each path to sys.path
for relative_path in relative_paths:
    full_path = os.path.join(base_dir, relative_path)
    sys.path.insert(0, full_path)

# Import necessary functions
from beam_level_harmonic_optimization import * 
from beam_level_candidate_sifting import *
try:
    from read_input_file_dir import load_parameters
except ImportError as e:
    print("Error importing 'read_input_file_dir'. Ensure the script exists in the specified path.")
    print(e)
    sys.exit(1)

# Define configuration file path
config_file_path = os.path.join(base_dir, "input_file_dir_init/input_dir/input_file_directory.txt")

def final_stage_candidate_sifting(scan_id, data_id, data_type):   

    # Load configuration file for importing paths
    if not os.path.exists(config_file_path):
        print(f"Configuration file not found: {config_file_path}")
        sys.exit(1)

    try:
        params = load_parameters(config_file_path)
    except Exception as e:
        print(f"Error loading parameters from configuration file: {e}")
        sys.exit(1)

    # Extract parameters from config for running beam level sifting
    environ_init_script = params.get('environ_init_script')

    #Input output dircetories
    raw_input_dir = os.path.join(params.get("raw_input_base_dir"), scan_id, params.get("raw_flag"))
    fil_output_dir = os.path.join(params.get("raw_input_base_dir"), scan_id, params.get("fil_flag"))
    aa_input_dir = params.get('aa_input_dir')
    pulseline_input_file_dir = params.get('pulseline_input_file_dir')
    pulseline_output_dir = params.get('pulseline_output_dir')
    search_level_harmonic_opt_flag = params.get('search_level_harmonic_opt_flag')
    beam_level_harmonic_opt_flag = params.get('beam_level_harmonic_opt_flag')

    os.system(f"source {environ_init_script}")

    # Sifted candidates output will be stored here
    sifting_output_path = os.path.join(pulseline_output_dir, data_id)   

    # Setting the header file output path
    header_file_output_path = os.path.join(fil_output_dir, data_id)

    # Handle harmonic optimization based on the flag

    if beam_level_harmonic_opt_flag  == 1:

        print("Harmonic Elimination and Replacement begins now ...")
        # Call harmonic optimization if flag is enabled (1)
        harmonic_elimination(
            input_dir=sifting_output_path,
            output_dir=sifting_output_path,
            input_file_dir=pulseline_input_file_dir, 
            )
        
        #harmonic_replacement(input_dir=sifting_output_path,
        #    output_dir=sifting_output_path,
        #    input_file_dir=pulseline_input_file_dir, 
        #    )

    elif beam_level_harmonic_opt_flag  == 0 and search_level_harmonic_opt_flag == 1:
        # Skip harmonic optimization if flag is disabled (0)
        print("Skipping harmonic optimization, as search level harmonic optimisation is already done.")

    elif beam_level_harmonic_opt_flag  == 0 and search_level_harmonic_opt_flag == 0:
        # Skip harmonic optimization if flag is disabled (0)
        print("Skipping both type (beam and search) of harmonic optimization and going ahead with primarily shifted harmonically unoptimised  outputs.")
    else:
        # Invalid value in the flag
        print("Invalid harmonic flag in input file.")
        sys.exit(1)

    # Path for getting beam sifting parameters
    beam_sifting_par_file_path = os.path.join(pulseline_input_file_dir, "pulseline_master.txt")
    
    # Loading the beam sifting parameter file
    try:
        sift_params = load_parameters(beam_sifting_par_file_path)
    except Exception as e:
        print(f"Error loading beam sifting parameters: {e}")
        sys.exit(1)


    # Extract additional parameters for sifting
    period_tol_beam_sort = sift_params.get('period_tol_beam_sort')
    min_beam_cut = sift_params.get('min_beam_cut')
    start_DM = sift_params.get('start_DM')
    end_DM = sift_params.get('end_DM')
    dm_step = sift_params.get('dm_step')
    DM_filtering_cut_10 = sift_params.get('DM_filtering_cut_10')
    DM_filtering_cut_1000 = sift_params.get('DM_filtering_cut_1000')
    max_harm = sift_params.get('max_harm')
    harmonic_opt_flag = sift_params.get('harmonic_opt_flag')
    period_tol_harm = sift_params.get('period_tol_harm')

    # Executing the beam sifting function
    beam_level_candidate_sifting(
        sifting_output_path, sifting_output_path, data_id, data_type, raw_input_dir, header_file_output_path, 
        pulseline_input_file_dir, harmonic_opt_flag, period_tol_beam_sort, min_beam_cut, start_DM, end_DM, dm_step,
        DM_filtering_cut_10, DM_filtering_cut_1000, max_harm, period_tol_harm
    )

# Run the function if the script is executed
if __name__ == "__main__":
    # Define argument parser
    parser = argparse.ArgumentParser(description="Run the final stage candidate sifting with specified parameters.")
    parser.add_argument("scan_id", type=str, help="Scan ID to use for processing.")  # Added scan_id argument
    parser.add_argument("data_id", type=str, help="Data ID to use for processing.")  # Added data_id argument
    parser.add_argument("data_type", type=float, help="Data Type to use for processing.")  # Added data_id argument

    # Parse arguments and run the function
    args = parser.parse_args()
    final_stage_candidate_sifting(args.scan_id, args.data_id, args.data_type)
