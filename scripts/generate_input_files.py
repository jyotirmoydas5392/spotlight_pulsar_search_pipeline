import os
import sys
import re
import argparse
import subprocess
import logging
import numpy as np
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Load base directory from environment variable
base_dir = os.getenv("PULSELINE_VER1_DIR")
if not base_dir:
    logging.error("Error: PULSELINE_VER1_DIR environment variable is not set.")
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

def generate_input_files(input_file_generator_dir, scan_id, data_id, band_type):
    """Generate input files with additional arguments."""
    logging.info("Generating input files ...")
    try:
        subprocess.run(
            ["python3", f"{input_file_generator_dir}/input_file_generator.py", scan_id, data_id, band_type],
            check=True,
        )
        logging.info("Input files generated successfully.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error generating input files: {e}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Generate input files.")
    
    # Define positional arguments (no "--" prefixes)
    parser.add_argument("scan_id", type=str, help="The ID of the annalysing SCAN.")
    parser.add_argument("data_id", type=str, help="The unique target id for a perticular SCAN.")
    parser.add_argument("input_gen", type=str, help="Directory for input file generation.")
    parser.add_argument("band_id", type=str, help="The uGMRT band used for the perticular scan.")

    
    args = parser.parse_args()

    # Run input file generation (mandatory)
    generate_input_files(args.input_gen, args.scan_id, args.data_id, args.band_id)

if __name__ == "__main__":
    main()
