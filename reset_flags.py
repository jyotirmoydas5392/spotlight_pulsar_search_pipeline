import os
import sys
import logging
import numpy as np
import subprocess

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]: %(message)s")

# Get the base directory from the environment variable
base_dir = os.getenv("PULSELINE_DEV_DIR")
if not base_dir:
    logging.error("Error: PULSELINE_DEV_DIR environment variable is not set.")
    sys.exit(1)

# Import load_parameters function
sys.path.insert(0, os.path.join(base_dir, "input_file_dir_init/scripts"))
try:
    from read_input_file_dir import load_parameters
except ImportError:
    logging.error("Error importing required modules.")
    sys.exit(1)

# Define configuration file path
config_file_path = os.path.join(base_dir, "input_file_dir_init/input_dir/input_file_directory.txt")

def main():
    # Load input parameters
    try:
        params = load_parameters(config_file_path)
        print("Loaded parameters from configuration file.")
    except Exception as e:
        logging.error(f"Error loading parameters from configuration file: {e}")
        sys.exit(1)

    # Define status file paths
    status_path = os.path.join(base_dir, "analysis_status.log")

    # Write 'analysis_status = OFF' to both
    with open(status_path, "w") as status_file:
        status_file.write("analysis_status = OFF\n")
    print(f"Wrote 'analysis_status = OFF' to local file: {status_path}")

if __name__ == "__main__":
    main()
    print("Flags resetted successfully.")
