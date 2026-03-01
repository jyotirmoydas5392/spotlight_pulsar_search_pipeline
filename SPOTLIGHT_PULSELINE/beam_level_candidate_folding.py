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
    from generate_folding_candidates import *
    from do_folding import *
    from batch_convert_ps_to_png import *
    from read_input_file_dir import load_parameters
except ImportError as e:
    logging.error("Error importing required modules. Ensure the scripts exist in the specified paths.")
    logging.error(e)
    sys.exit(1)

# Define configuration file path
config_file_path = os.path.join(base_dir, "input_file_dir_init/input_dir/input_file_directory.txt")


def beam_level_candidate_folding(node_alias, gpu_id, data_id, scan_id):
    """
    Handles candidate folding operations and PNG generation for a given file.

    :param file: Input file name to process.
    :param node_alias: Alias of the node where processing is performed.
    :param gpu_id: ID of the GPU to use for processing.
    """
    # Load configuration file for importing paths
    if not os.path.exists(config_file_path):
        logging.error(f"Configuration file not found: {config_file_path}")
        sys.exit(1)

    try:
        params = load_parameters(config_file_path)
    except Exception as e:
        logging.error(f"Error loading parameters from configuration file: {e}")
        sys.exit(1)


   # Extract parameters from config using .get()
    environ_init_script = params.get('environ_init_script')
    presto_folding_flag = params.get('presto_folding_flag')
    pulsarx_folding_flag = params.get('pulsarx_folding_flag')
    pulsarx_folding_template_path = params.get('pulsarx_folding_template_path')
    fil_file_dir_path = os.path.join(params.get('raw_input_base_dir'), scan_id, params.get('fil_flag'), data_id)
    pulseline_input_file_dir = params.get('pulseline_input_file_dir')
    aa_input_file_dir = params.get('aa_input_file_dir')
    pulseline_output_dir = params.get('pulseline_output_dir')
    workers_per_node = int(params.get('workers_per_node'))


    # Set up parameters for candidate folding
    folded_outputs = "folded_outputs/"  # Define the subdirectory name
    folding_input_path = os.path.join(pulseline_output_dir, data_id)
    folding_output_path = os.path.join(folding_input_path, folded_outputs)

    # Create the output directory if it doesn't exist
    os.makedirs(folding_output_path, exist_ok=True)

    if pulsarx_folding_flag == 1 and presto_folding_flag == 0:

        # Generate the candidate list files for each node, each GPUs for pulsarx folding
        generate_pulsarx_folding_candidates_per_beam(folding_input_path, folding_input_path, aa_input_file_dir, pulseline_input_file_dir,
    node_alias, gpu_id)

        # Run candidate folding for each node, each GPUs
        try:
            pulsarx_folding_from_file(node_alias, gpu_id, folding_input_path, pulsarx_folding_template_path, fil_file_dir_path, folding_output_path, workers_per_node)
            logging.info(f"Folding completed successfully for node id {node_alias}.")
        except Exception as e:
            logging.error(f"Error during folding operation: {e}")
            sys.exit(1)
        
    elif pulsarx_folding_flag == 0 and presto_folding_flag == 1:

        # Generate the candidate list for each node, each GPUs for presto folding
        generate_presto_folding_candidates_per_node(folding_input_path, folding_input_path, aa_input_file_dir, pulseline_input_file_dir,
        node_alias, gpu_id)

        # Run candidate folding for each node, each GPUs
        try:
            presto_folding_from_file(node_alias, gpu_id, folding_input_path, folding_output_path, workers_per_node)
            logging.info(f"Folding completed successfully for node id {node_alias}.")
        except Exception as e:
            logging.error(f"Error during folding operation: {e}")
            sys.exit(1)

        # Generate PNG files from the folded PS files
        input_ps_files_path = folding_output_path
        output_png_files_path = folding_output_path

        try:
            batch_convert_ps_to_png(
                input_ps_files_path, output_png_files_path,
                params.get('worker_per_node'), keyword=node_alias
            )
            logging.info("PNG files created successfully for all folded PS files.")
        except Exception as e:
            logging.error(f"Error during PNG conversion: {e}")
            sys.exit(1)

if __name__ == "__main__":
    # Define argument parser
    parser = argparse.ArgumentParser(description="Run the PULSELINE function with specified parameters.")
    parser.add_argument("node_alias", type=str, help="Node alias where the function is running.")
    parser.add_argument("gpu_id", type=int, help="GPU ID to use for processing.")
    parser.add_argument("data_id", type=str, help="Data ID to use for processing.")  # Added data_id argument
    parser.add_argument("scan_id", type=str, help="Scan ID to use for processing.")  # Added scan_id argument


    # Parse arguments and run the function
    args = parser.parse_args()
    beam_level_candidate_folding(args.node_alias, args.gpu_id, args.data_id, args.scan_id)