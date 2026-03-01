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

def process_raw_files(config_path, scan_id, data_id, total_beams):
    """Process raw files into filterbank files using load_parameters."""

    # Load parameters from the TXT file
    params = load_parameters(config_path)

    # Get necessary parameters
    input_dir = os.path.join(params.get("raw_input_base_dir"), scan_id, params.get("raw_flag"))
    output_dir = os.path.join(params.get("raw_input_base_dir"), scan_id)

    # Ensure the input directory exists
    if not Path(input_dir).exists():
        raise FileNotFoundError(f"Input directory '{input_dir}' does not exist.")

    # Escape special characters in data_id for regex
    escaped_target_id = re.escape(data_id)

    # Build regex pattern for `.raw.<number>` files containing data_id
    pattern = re.compile(rf".*{escaped_target_id}.*\.raw\.\d+$")

    # Collect matching raw files with full paths
    raw_files = [
        os.path.join(input_dir, f)
        for f in os.listdir(input_dir)
        if os.path.isfile(os.path.join(input_dir, f)) and pattern.match(f)
    ]

    print(f"Raw files {raw_files}")

    # Other parameters
    nbeams = int(np.divide(int(total_beams), int(len(raw_files))))
    njobs = int(params.get("num_jobs", -1))


    if not raw_files:
        raise ValueError("No matching `.raw.<number>` files found in the input directory.")


    # Build the command
    command = [
        "xtract2fil",
        *raw_files,  # Unpack the raw_files list into separate arguments
        "--dual",
        "--scan", str(data_id),
        "--tbin", str(10),
        "--fbin", str(4),
        "--offset", str(0),
        "--njobs", str(njobs),
        "--nbeams", str(nbeams),
        "--output", output_dir
    ]

    # Execute the command
    logging.info(f"Extracting {nbeams * len(raw_files)} beams from {len(raw_files)} raw files...")
    try:
        logging.info("Final xtract2fil command:")
        logging.info(" ".join(command))
        subprocess.run(command, check=True)
        logging.info("Processing completed successfully.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error during processing: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")


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
    parser = argparse.ArgumentParser(description="Process raw files and generate input files.")
    
    # Define positional arguments (no "--" prefixes)
    parser.add_argument("config", type=str, help="Path to the configuration TXT file.")
    parser.add_argument("scan_id", type=str, help="The ID of the annalysing SCAN.")
    parser.add_argument("data_id", type=str, help="The unique target id for a perticular SCAN.")
    parser.add_argument("beams", type=str, help="The name of the beams recorded for the pericular target.")
    parser.add_argument("data_type", type=int, choices=[0, 1], help="0: Run raw_to_fil, 1: Skip raw_to_fil.")
    parser.add_argument("input_gen", type=str, help="Directory for input file generation.")
    parser.add_argument("band_id", type=str, help="The uGMRT band used for the perticular scan.")

    
    args = parser.parse_args()

    # Run raw file processing only if data_type == 0
    if args.data_type == 0:
        process_raw_files(args.config, args.scan_id, args.data_id, args.beams)
    else:
        logging.info("Skipping raw_to_fil as data_type is set to FIL.")

    # Run input file generation (mandatory)
    generate_input_files(args.input_gen, args.scan_id, args.data_id, args.band_id)

if __name__ == "__main__":
    main()
