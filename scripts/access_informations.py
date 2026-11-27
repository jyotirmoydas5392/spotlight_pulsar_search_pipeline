import os
import sys
import re
import argparse
import subprocess
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Base directory and sys.path are defined once in pulseline_config.py
from pulseline_config import base_dir


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
    """
    GTAC_scan = []
    Target = []
    Total_beams = []
    uGMRT_band = []

    try:
        with open(file_path, 'r') as file:
            lines = file.readlines()
    except Exception as e:
        print(f"Error reading file: {e}")
        return GTAC_scan, Target, Total_beams, uGMRT_band

    if not lines:
        print("Warning: File is empty.")
        return GTAC_scan, Target, Total_beams, uGMRT_band

    # Process each line skipping header, empty lines, and comments
    for idx, line in enumerate(lines[1:], start=2):
        line = line.strip()
        if not line or line.startswith("#"):
            continue

        parts = line.split()

        if len(parts) >= 4:
            GTAC_scan.append(parts[0])
            Target.append(parts[1])
            try:
                Total_beams.append(int(parts[2]))
            except ValueError:
                print(f"Warning: Non-integer beam count on line {idx}: {parts[2]}")
                Total_beams.append(0)
            uGMRT_band.append(parts[3])
        else:
            print(f"Warning: Skipping incomplete line {idx}: {line}")

    return GTAC_scan, Target, Total_beams, uGMRT_band
