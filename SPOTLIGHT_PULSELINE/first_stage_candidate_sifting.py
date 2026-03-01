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
    "SPOTLIGHT_PULSELINE/scripts"
]

# Loop through and add each path to sys.path
for relative_path in relative_paths:
    full_path = os.path.join(base_dir, relative_path)
    sys.path.insert(0, full_path)

# Import necessary functions
from aa_output_rename import *
from periodicity_search_level_candidate_sifting import *
from acceleration_search_level_candidate_sifting import *
from search_level_harmonic_optimization import *
try:
    from read_input_file_dir import load_parameters
except ImportError as e:
    print("Error importing 'read_input_file_dir'. Ensure the script exists in the specified path.")
    print(e)
    sys.exit(1)

# Define configuration file path
config_file_path = os.path.join(base_dir, "input_file_dir_init/input_dir/input_file_directory.txt")


def first_stage_candidate_sifting(file, node_alias, gpu_id, data_id):
    # Load configuration file for importing paths
    if not os.path.exists(config_file_path):
        print(f"Configuration file not found: {config_file_path}")
        sys.exit(1)

    try:
        params = load_parameters(config_file_path)
    except Exception as e:
        print(f"Error loading parameters from configuration file: {e}")
        sys.exit(1)
    
    # Extract parameters from config
    environ_init_script = params.get('environ_init_script')
    search_type = params.get('search_type')
    aa_output_dir = params.get('aa_output_dir')
    pulseline_input_file_dir = params.get('pulseline_input_file_dir')
    pulseline_input_dir = params.get('pulseline_input_dir')
    pulseline_output_dir = params.get('pulseline_output_dir')
    pulseline_log_dir = params.get('pulseline_log_dir')
    search_level_harmonic_opt_flag = params.get('search_level_harmonic_opt_flag')

    # PULSELINE string setup
    PULSELINE = "PULSELINE"

    # Generate pulseline file name
    pulseline_file_name = f"{PULSELINE}_{file.replace('AA_', '').strip()}"
    print(f"Pulseline file path: {os.path.join(pulseline_input_file_dir, pulseline_file_name)}")
    
    # Check if pulseline input file exists
    if not os.path.exists(os.path.join(pulseline_input_file_dir, pulseline_file_name)):
        print(f"Pulseline input file not found: {os.path.join(pulseline_input_file_dir, pulseline_file_name)}")
        sys.exit(1)

    # Reload parameters from pulseline input file
    try:
        params = load_parameters(os.path.join(pulseline_input_file_dir, pulseline_file_name))
    except Exception as e:
        print(f"Error loading parameters from configuration file: {e}")
        sys.exit(1)
    
    # Extract and print 'fil_file' from params
    fil_file_path = params.get('fil_file')
    if fil_file_path:
        fil_file = os.path.basename(fil_file_path)
        print(f"Extracted fil file name: {fil_file}")
    else:
        print("fil_file parameter not found in the loaded parameters.")
    file_name = fil_file.replace(".fil", "")

    # Define rename paths
    rename_input_path = os.path.join(aa_output_dir, data_id, "output", file.replace(".txt", ""))
    rename_output_dir = os.path.join(pulseline_input_dir, data_id)
    rename_output_path = os.path.join(rename_output_dir, file.replace(".txt", ""))

    # Ensure output path exists
    os.makedirs(rename_output_path, exist_ok=True)

    # Perform AA output renaming
    aa_output_rename(rename_input_path, rename_output_path, fil_file, search_type)

    # Set up parameters for candidate sifting
    sifting_input_path = os.path.join(rename_output_dir, file.replace(".txt", ""))
    sifting_output_path = os.path.join(pulseline_output_dir, data_id)

    # Run candidate sifting
    if search_type == 0:
        periodicity_search_level_sift_candidates(sifting_input_path, sifting_output_path, file_name, 
                                    params.get('start_DM'), params.get('end_DM'), 
                                    params.get('low_period'), params.get('high_period'),
                                    params.get('dm_step'), params.get('DM_filtering_cut_10'),
                                    params.get('DM_filtering_cut_1000'), params.get('SNR_cut'),
                                    params.get('period_tol_init_sort'), params.get('harm_cut'))
    elif search_type == 1:
        acceleartion_search_level_sift_candidates(sifting_input_path, sifting_output_path, file_name, 
                                    params.get('start_DM'), params.get('end_DM'), 
                                    params.get('low_period'), params.get('high_period'),
                                    params.get('dm_step'), params.get('DM_filtering_cut_10'),
                                    params.get('DM_filtering_cut_1000'), params.get('SNR_cut'),
                                    params.get('period_tol_init_sort'), params.get('harm_cut'))
    else:
        print("Select appropriate search type flag...")

    # Handle harmonic optimization based on the flag
    if search_level_harmonic_opt_flag == 1:
        # Call harmonic optimization if flag is enabled (1)
        harmonic_filtering(
            input_dir=sifting_output_path,
            output_dir=sifting_output_path,
            file_name=file_name,
            period_tol_harm=params.get('period_tol_harm'),
            max_harm=params.get('max_harm'),
            DM_filtering_cut_10=params.get('DM_filtering_cut_10'),
            DM_filtering_cut_1000=params.get('DM_filtering_cut_1000')
        )
    elif search_level_harmonic_opt_flag == 0:
        # Skip harmonic optimization if flag is disabled (0)
        print("Skipping search level harmonic optimization.")
    else:
        # Invalid value in the flag
        print("Invalid search level harmonic flag in input file.")
        sys.exit(1)

    print(f"CPU function for first stage sorting executed successfully for file {file}.")
if __name__ == "__main__":
    # Define argument parser
    parser = argparse.ArgumentParser(description="Run the PULSELINE function with specified parameters.")
    parser.add_argument("file", type=str, help="Input file name to process.")
    parser.add_argument("node_alias", type=str, help="Node alias where the function is running.")
    parser.add_argument("gpu_id", type=int, help="GPU ID to use for processing.")
    parser.add_argument("data_id", type=str, help="Data ID to use for processing.")  # Added data_id argument

    # Parse arguments and run the function
    args = parser.parse_args()
    first_stage_candidate_sifting(args.file, args.node_alias, args.gpu_id, args.data_id)
