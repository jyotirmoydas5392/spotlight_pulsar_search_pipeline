import os
import sys
import time
import argparse
import logging
import numpy as np

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s [%(levelname)s]: %(message)s',
                    handlers=[logging.StreamHandler()])

# Get the base directory from the environment variable
base_dir = os.getenv("PULSELINE_DEV_DIR")
if not base_dir:
    logging.error("PULSELINE_DEV_DIR environment variable is not set.")
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
try:
    from summary_plots import *
    from read_input_file_dir import load_parameters
except ImportError as e:
    logging.error("Error importing required modules. Ensure the scripts exist in the specified paths.")
    logging.error(e)
    sys.exit(1)

# Define configuration file path
config_file_path = os.path.join(base_dir, "input_file_dir_init/input_dir/input_file_directory.txt")


def generate_summary_plots(data_id):
    """Generate summary plots for a given data_id."""

    # Load configuration file for importing paths
    if not os.path.exists(config_file_path):
        logging.error(f"Configuration file not found: {config_file_path}")
        sys.exit(1)

    try:
        params = load_parameters(config_file_path)
    except Exception as e:
        logging.error(f"Error loading parameters from configuration file: {e}")
        sys.exit(1)

    # Extract parameters from config
    input_file_dir = params.get('pulseline_input_file_dir')
    total_sorting_stage = int(params.get('total_sorting_stage'))

    pulseline_output_dir = params.get('pulseline_output_dir')
    candidates_files_dir = os.path.join(pulseline_output_dir, data_id)

    classifier_output_dir = params.get('classifier_output_dir')
    plots_output_dir = os.path.join(classifier_output_dir, data_id)
    # Create the directory if it doesn't exist
    os.makedirs(plots_output_dir, exist_ok=True)

    
    # Generate the summary plots for different sorting stages
    summary_plots(candidates_files_dir, plots_output_dir, input_file_dir, total_sorting_stage)

# Run the function if the script is executed
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate summary plots for given data_id.")
    parser.add_argument("data_id", type=str, help="Data ID for which to generate summary plots.")
    
    args = parser.parse_args()
    generate_summary_plots(args.data_id)