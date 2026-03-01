import os
import sys
import logging
import numpy as np
import subprocess

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]: %(message)s")

base_dir = os.getenv("PULSELINE_DEV_DIR")
if not base_dir:
    logging.error("Error: PULSELINE_DEV_DIR environment variable is not set.")
    sys.exit(1)

sys.path.insert(0, os.path.join(base_dir, "input_file_dir_init/scripts"))
try:
    from read_input_file_dir import load_parameters  
except ImportError:
    logging.error("Error importing required modules.")
    sys.exit(1)

config_file_path = os.path.join(base_dir, "input_file_dir_init/input_dir/input_file_directory.txt")

def main():
    try:
        params = load_parameters(config_file_path)
    except Exception as e:
        logging.error(f"Error loading parameters from configuration file: {e}")
        sys.exit(1)

    user_id = params.get("user_id", "spotlight").strip()
    avail_gpus_file_dir = params.get("avail_gpus_file_dir")

    if not user_id or not avail_gpus_file_dir:
        logging.error("Missing required parameters (user_id or avail_gpus_file_dir).")
        sys.exit(1)

    try:
        gpu_nodes = np.loadtxt(os.path.join(avail_gpus_file_dir, "avail_gpu_nodes.txt"), dtype=str)
        if isinstance(gpu_nodes, str):
            gpu_nodes = [gpu_nodes]
        elif gpu_nodes.ndim == 0:
            gpu_nodes = [str(gpu_nodes)]
    except Exception as e:
        logging.error(f"Error reading available GPU nodes: {e}")
        sys.exit(1)

    for node in gpu_nodes:
        logging.info(f"Attempting to kill processes for user '{user_id}' on node '{node}'...")
        try:
            command = f'ssh {node} "pkill -9 -u {user_id}"'
            subprocess.run(command, shell=True, check=True)
            logging.info(f"Successfully killed processes for user '{user_id}' on node '{node}'.")
        except subprocess.CalledProcessError as e:
            logging.warning(f"Failed to kill processes on node '{node}'. Error: {e}")

if __name__ == "__main__":
    main()