import os
import sys
import time
import logging
import subprocess
from multiprocessing import Process

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Load base directory from environment variable
base_dir = os.getenv("PULSELINE_DEV_DIR")
if not base_dir:
    logging.error("Error: PULSELINE_DEV_DIR environment variable is not set.")
    sys.exit(1)

# Add required paths to sys.path
required_paths = [
    "input_file_dir_init/scripts",
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
except ImportError as e:
    logging.error("Error importing required modules.", exc_info=True)
    sys.exit(1)

# Define configuration file path
config_file_path = os.path.join(base_dir, "input_file_dir_init/input_dir/input_file_directory.txt")

# Load configuration parameters
if not os.path.exists(config_file_path):
    logging.error(f"Configuration file not found: {config_file_path}")
    sys.exit(1)

try:
    params = load_parameters(config_file_path)
    environ_init_script = params.get('environ_init_script')
    beam_level_folding_path = params.get('beam_level_folding_path')
    logging.info("Configuration parameters loaded successfully.")
except Exception as e:
    logging.error(f"Error loading parameters from configuration file: {e}", exc_info=True)
    sys.exit(1)


def parallel_folding(node_alias, gpu_id, data_id, scan_id, log_dir, file_processing_delay, command_template):
    """
    Process the task on the given GPU of the given node.
    """

    os.makedirs(log_dir, exist_ok=True)

    try:
        command = command_template.format(
            node_alias=node_alias,
            log_dir=log_dir,
            gpu_id=gpu_id,
            data_id = data_id,
            scan_id = scan_id,
            environ_init_script=environ_init_script,
            beam_level_folding_path=beam_level_folding_path
        )
        print(command)

        logging.info(f"Executing command for {node_alias} GPU {gpu_id}: {command}")
        subprocess.run(command, shell=True, check=True)
        logging.info(f"Processing completed for on node {node_alias}, GPU {gpu_id}.")
        

    except subprocess.CalledProcessError as e:
        logging.error(f"Error processing on GPU {gpu_id}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error processing for file of node {node_alias}: {e}")


def multi_node_folding(node_alias, data_id, scan_id, log_dir, gpu_0_start_delay, gpu_1_start_delay, file_processing_delay, command_template):
    """
    Process files for both GPUs of the node concurrently with separate delays.
    """
    processes = []

    # Start GPU 0 process with its specific delay
    time.sleep(gpu_0_start_delay)
    p_gpu_0 = Process(
        target=parallel_folding,
        args=(node_alias, 0, data_id, scan_id, log_dir, file_processing_delay, command_template)
    )
    processes.append(p_gpu_0)
    p_gpu_0.start()
    logging.info(f"Started processing for GPU 0 on {node_alias}.")

    # Start GPU 1 process with its specific delay
    time.sleep(gpu_1_start_delay)
    p_gpu_1 = Process(
        target=parallel_folding,
        args=(node_alias, 1, data_id, scan_id, log_dir, file_processing_delay, command_template)
    )
    processes.append(p_gpu_1)
    p_gpu_1.start()
    logging.info(f"Started processing for GPU 1 on {node_alias}.")

    # Wait for all processes to complete
    for p in processes:
        p.join()
    logging.info(f"Processing completed for both GPUs on {node_alias}.")