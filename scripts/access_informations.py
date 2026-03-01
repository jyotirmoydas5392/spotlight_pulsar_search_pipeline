import os
import sys
import re
import argparse
import subprocess
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Load base directory from environment variable
base_dir = os.getenv("PULSELINE_DEV_DIR")
if not base_dir:
    logging.error("Error: PULSELINE_DEV_DIR environment variable is not set.")
    sys.exit(1)

# Add required paths to sys.path
required_paths = [
    "input_file_dir_init/scripts"
]
for relative_path in required_paths:
    full_path = os.path.join(base_dir, relative_path)
    if os.path.exists(full_path):
        sys.path.insert(0, full_path)
        logging.info(f"Added to sys.path: {full_path}")
    else:
        logging.warning(f"Path does not exist: {full_path}")

# Import required modules
try:
    from read_input_file_dir import load_parameters  # Import load_parameters
    logging.info("Modules imported successfully.")
except ImportError:
    logging.error("Error importing required modules.", exc_info=True)
    sys.exit(1)



def read_flag_from_remote():
    try:
        result = subprocess.run(
            ["ssh", "spotlight@login02", "cat /tmp/spltcontrol_status.log"],
            capture_output=True,
            text=True,
            check=True
        )
        line = result.stdout.strip()
        if line.startswith("splt_stat"):
            _, flag = line.split('=')
            return flag.strip().upper()
    except subprocess.CalledProcessError as e:
        print(f"Error reading remote flag: {e}")
    return None


def extract_gtac_obs_informations(file_path):
    """
    Extracts GTAC observation details from a given file.

    Returns:
        - GTAC_scan: list of scan IDs
        - Target: list of target names
        - Total_beams: list of total beam counts (as integers)
        - uGMRT_band: list of bands
        - Input_file: list of input file paths
    """
    GTAC_scan = []
    Target = []
    Total_beams = []
    uGMRT_band = []
    Input_file = []

    try:
        with open(file_path, 'r') as file:
            lines = file.readlines()
    except Exception as e:
        print(f"Error reading file: {e}")
        return GTAC_scan, Target, Total_beams, uGMRT_band, Input_file

    if not lines:
        print("Warning: File is empty.")
        return GTAC_scan, Target, Total_beams, uGMRT_band, Input_file

    # Process each line skipping header, empty lines, and comments
    for idx, line in enumerate(lines[1:], start=2):
        line = line.strip()
        if not line or line.startswith("#"):
            continue

        parts = line.split()

        if len(parts) >= 5:
            GTAC_scan.append(parts[0])
            Target.append(parts[1])
            try:
                Total_beams.append(int(parts[2]))
            except ValueError:
                print(f"Warning: Non-integer beam count on line {idx}: {parts[2]}")
                Total_beams.append(0)
            uGMRT_band.append(parts[3])
            Input_file.append(parts[4])
        else:
            print(f"Warning: Skipping incomplete line {idx}: {line}")

    return GTAC_scan, Target, Total_beams, uGMRT_band, Input_file
