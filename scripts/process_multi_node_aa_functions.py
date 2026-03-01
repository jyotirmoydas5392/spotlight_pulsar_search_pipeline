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
    aa_executable_file_dir = params.get('aa_executable_file_dir')
    logging.info("Configuration parameters loaded successfully.")
except Exception as e:
    logging.error(f"Error loading parameters from configuration file: {e}", exc_info=True)
    sys.exit(1)


def process_files_for_gpu(node_alias, gpu_id, data_id, input_file_dir, output_dir, log_dir, file_processing_delay, command_template):
    """
    Process files sequentially on the given GPU of the given node.
    """
    files_to_process = [
        f for f in os.listdir(input_file_dir)
        if f.startswith("AA") and f.endswith(f"node_{node_alias}_gpu_id_{gpu_id}.txt")
    ]

    if not files_to_process:
        logging.info(f"No files found for node {node_alias}, GPU {gpu_id}. Skipping...")
        return

    logging.info(f"Files to process for node {node_alias}, GPU {gpu_id}: {files_to_process}")
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(log_dir, exist_ok=True)

    for i, file in enumerate(sorted(files_to_process)):
        try:
            input_file_path = os.path.join(input_file_dir, file)
            command = command_template.format(
                node_alias=node_alias,
                input_file_path=input_file_path,
                file=file,
                output_dir=output_dir,
                log_dir=log_dir,
                gpu_id=gpu_id,
                data_id = data_id,
                environ_init_script=environ_init_script,
                aa_executable_file_dir=aa_executable_file_dir
            )
            print(command)

            logging.info(f"Executing command for {node_alias} GPU {gpu_id}: {command}")
            subprocess.run(command, shell=True, check=True)
            logging.info(f"Processing completed for {file} on node {node_alias}, GPU {gpu_id}.")

            if i < len(files_to_process) - 1:
                logging.info(f"Waiting for {file_processing_delay} seconds before processing the next file...")
                time.sleep(file_processing_delay)

        except subprocess.CalledProcessError as e:
            logging.error(f"Error processing {file} on GPU {gpu_id}: {e}")
        except Exception as e:
            logging.error(f"Unexpected error processing {file}: {e}")


def process_node(node_alias, data_id, input_file_dir, output_dir, log_dir, gpu_0_start_delay, gpu_1_start_delay, file_processing_delay, command_template):
    """
    Process files for both GPUs of the node concurrently with separate delays.
    """
    processes = []

    # Start GPU 0 process with its specific delay
    time.sleep(gpu_0_start_delay)
    p_gpu_0 = Process(
        target=process_files_for_gpu,
        args=(node_alias, 0, data_id, input_file_dir, output_dir, log_dir, file_processing_delay, command_template)
    )
    processes.append(p_gpu_0)
    p_gpu_0.start()
    logging.info(f"Started processing for GPU 0 on {node_alias}.")

    # Start GPU 1 process with its specific delay
    time.sleep(gpu_1_start_delay)
    p_gpu_1 = Process(
        target=process_files_for_gpu,
        args=(node_alias, 1, data_id, input_file_dir, output_dir, log_dir, file_processing_delay, command_template)
    )
    processes.append(p_gpu_1)
    p_gpu_1.start()
    logging.info(f"Started processing for GPU 1 on {node_alias}.")

    # Wait for all processes to complete
    for p in processes:
        p.join()
    logging.info(f"Processing completed for both GPUs on {node_alias}.")