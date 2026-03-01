import os
import sys
import time
import logging
import subprocess
from multiprocessing import Process

# ----------------------------- Logging Configuration -----------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ----------------------------- Load Base Environment -----------------------------
base_dir = os.getenv("PULSELINE_DEV_DIR")
if not base_dir:
    logging.error("Error: PULSELINE_DEV_DIR environment variable is not set.")
    sys.exit(1)

# Add required relative paths to sys.path
required_paths = ["input_file_dir_init/scripts"]
for relative_path in required_paths:
    full_path = os.path.join(base_dir, relative_path)
    if os.path.exists(full_path):
        sys.path.insert(0, full_path)
        logging.info(f"Added to sys.path: {full_path}")
    else:
        logging.warning(f"Path does not exist: {full_path}")

# ----------------------------- Import Required Modules -----------------------------
try:
    from read_input_file_dir import load_parameters
    logging.info("Modules imported successfully.")
except ImportError as e:
    logging.error("Error importing required modules.", exc_info=True)
    sys.exit(1)

# ----------------------------- Load Config Parameters -----------------------------
config_file_path = os.path.join(base_dir, "input_file_dir_init/input_dir/input_file_directory.txt")
if not os.path.exists(config_file_path):
    logging.error(f"Configuration file not found: {config_file_path}")
    sys.exit(1)

try:
    params = load_parameters(config_file_path)
    environ_init_script = params.get('environ_init_script')
    rfi_clean_module_path = params.get('rfi_clean_module_path')
    logging.info("Configuration parameters loaded successfully.")
except Exception as e:
    logging.error(f"Error loading parameters from configuration file: {e}", exc_info=True)
    sys.exit(1)

# ----------------------------- Core Processing Function -----------------------------
def parallel_rfi_mitigation(node_alias, gpu_id, data_id, scan_id, band_id, input_file_dir, log_dir, file_processing_delay, command_template):
    """
    Process files for the specified node and GPU. Collects relevant .txt files,
    extracts the fil_file field, then builds and executes a command for RFI mitigation.
    """
    fil_files = []

    # Collect input file names for this node and GPU
    files_to_process = [
        f for f in os.listdir(input_file_dir)
        if f.startswith("PULSELINE") and f.endswith(f"node_{node_alias}_gpu_id_{gpu_id}.txt")
    ]

    # Extract fil_file names from each input config
    for fname in files_to_process:
        fpath = os.path.join(input_file_dir, fname)
        try:
            params = load_parameters(fpath)
            fil_file = params.get("fil_file")
            if fil_file:
                fil_files.append(os.path.basename(fil_file))  # Store only filename
        except Exception as e:
            logging.warning(f"Error loading parameters from {fname}: {e}")

    if not fil_files:
        logging.info(f"No fil_files found for node {node_alias}, GPU {gpu_id}. Skipping...")
        return

    logging.info(f"fil_files to process for node {node_alias}, GPU {gpu_id}: {fil_files}")
    os.makedirs(log_dir, exist_ok=True)

    try:
        # Build the command string
        command = command_template.format(
            fil_files=",".join(fil_files),
            rfi_clean_module_path = rfi_clean_module_path,
            data_id=data_id,
            scan_id=scan_id,
            band_id = band_id,
            log_dir=log_dir,
            node_alias = node_alias,
            gpu_id = gpu_id,
            environ_init_script=environ_init_script
        )

        logging.info(f"Executing command for {node_alias} GPU {gpu_id}: {command}")
        subprocess.run(command, shell=True, check=True)
        logging.info(f"Processing completed on node {node_alias}, GPU {gpu_id}.")

        logging.info(f"Waiting {file_processing_delay} seconds before finishing...")
        time.sleep(file_processing_delay)

    except subprocess.CalledProcessError as e:
        logging.error(f"Subprocess failed on node {node_alias}, GPU {gpu_id}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error on node {node_alias}, GPU {gpu_id}: {e}")

# ----------------------------- Main Multi-GPU Launcher -----------------------------
def multi_node_rfi_mitigation(node_alias, data_id, scan_id, band_id, input_file_dir, log_dir, gpu_0_start_delay, gpu_1_start_delay, file_processing_delay, command_template):
    """
    Run parallel_rfi_mitigation on GPU 0 and GPU 1 using multiprocessing.
    Applies start delays for staggered execution.
    """
    processes = []

    # GPU 0 process
    time.sleep(gpu_0_start_delay)
    p_gpu_0 = Process(
        target=parallel_rfi_mitigation,
        args=(node_alias, 0, data_id, scan_id, band_id, input_file_dir, log_dir, file_processing_delay, command_template)
    )
    processes.append(p_gpu_0)
    p_gpu_0.start()
    logging.info(f"Started processing for GPU 0 on {node_alias}.")

    # GPU 1 process
    time.sleep(gpu_1_start_delay)
    p_gpu_1 = Process(
        target=parallel_rfi_mitigation,
        args=(node_alias, 1, data_id, scan_id, band_id, input_file_dir, log_dir, file_processing_delay, command_template)
    )
    processes.append(p_gpu_1)
    p_gpu_1.start()
    logging.info(f"Started processing for GPU 1 on {node_alias}.")

    # Wait for both to finish
    for p in processes:
        p.join()
    logging.info(f"All GPU processes completed on {node_alias}.")