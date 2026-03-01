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

# Store results
gpu_status_summary = []

def check_gpus_on_node(node):
    node_result = {
        "node": node,
        "reachable": True,
        "gpu_0": False,
        "gpu_1": False,
        "gpu_names": {}
    }

    try:
        result = subprocess.run(
            ["ssh", node, "nvidia-smi --query-gpu=index,name --format=csv,noheader"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=10
        )

        if result.returncode != 0:
            stderr_msg = result.stderr.strip()
            node_result["reachable"] = False
            if "Permission denied" in stderr_msg:
                logging.error(f"{node}: SSH authentication failed.")
            elif "Could not resolve hostname" in stderr_msg:
                logging.error(f"{node}: Hostname could not be resolved.")
            elif "Connection refused" in stderr_msg:
                logging.error(f"{node}: SSH connection refused.")
            elif "Connection timed out" in stderr_msg:
                logging.error(f"{node}: SSH connection timed out.")
            else:
                logging.error(f"{node}: SSH error: {stderr_msg or 'Unknown error'}")
            gpu_status_summary.append(node_result)
            return

        # Parse GPU output
        output_lines = result.stdout.strip().splitlines()
        gpu_info = {
            int(line.split(',')[0].strip()): line.split(',')[1].strip()
            for line in output_lines if line and ',' in line
        }

        logging.info(f"{node}: GPU status:")
        for gpu_id in [0, 1]:
            if gpu_id in gpu_info:
                gpu_name = gpu_info[gpu_id]
                node_result[f"gpu_{gpu_id}"] = True
                node_result["gpu_names"][gpu_id] = gpu_name
                logging.info(f"  GPU {gpu_id}: {gpu_name}")
            else:
                logging.warning(f"  GPU {gpu_id}: Not detected")

    except subprocess.TimeoutExpired:
        logging.error(f"{node}: SSH command timed out.")
        node_result["reachable"] = False
    except Exception as e:
        logging.error(f"{node}: Unexpected error: {e}")
        node_result["reachable"] = False

    gpu_status_summary.append(node_result)


def print_summary():
    print("\n====== GPU Status Summary ======")
    total_nodes = len(gpu_status_summary)
    unreachable_nodes = [n["node"] for n in gpu_status_summary if not n["reachable"]]
    total_reachable = total_nodes - len(unreachable_nodes)

    gpu_0_missing = []
    gpu_1_missing = []
    total_gpus_detected = 0

    for entry in gpu_status_summary:
        if entry["reachable"]:
            if entry["gpu_0"]:
                total_gpus_detected += 1
            else:
                gpu_0_missing.append(entry["node"])

            if entry["gpu_1"]:
                total_gpus_detected += 1
            else:
                gpu_1_missing.append(entry["node"])

    print(f"Total nodes checked       : {total_nodes}")
    print(f"Reachable nodes           : {total_reachable}")
    print(f"Unreachable nodes         : {len(unreachable_nodes)}")
    if unreachable_nodes:
        print("  -", ", ".join(unreachable_nodes))

    print(f"Total GPUs detected       : {total_gpus_detected}")
    print(f"Nodes missing GPU 0       : {len(gpu_0_missing)}")
    if gpu_0_missing:
        print("  -", ", ".join(gpu_0_missing))
    print(f"Nodes missing GPU 1       : {len(gpu_1_missing)}")
    if gpu_1_missing:
        print("  -", ", ".join(gpu_1_missing))
    print("================================\n")


def main():
    # Load input parameters
    try:
        params = load_parameters(config_file_path)
    except Exception as e:
        logging.error(f"Error loading parameters from configuration file: {e}")
        sys.exit(1)

    avail_gpus_file_dir = params.get("avail_gpus_file_dir")
    if not avail_gpus_file_dir:
        logging.error("Error: Missing required parameter 'avail_gpus_file_dir'.")
        sys.exit(1)

    gpu_file_path = os.path.join(avail_gpus_file_dir, "avail_gpu_nodes.txt")
    if not os.path.exists(gpu_file_path):
        logging.error(f"GPU nodes file not found at: {gpu_file_path}")
        sys.exit(1)

    try:
        gpu_nodes = np.loadtxt(gpu_file_path, dtype=str)
    except Exception as e:
        logging.error(f"Error reading available GPU nodes from {gpu_file_path}: {e}")
        sys.exit(1)

    if gpu_nodes.ndim == 0:
        gpu_nodes = [gpu_nodes.item()]

    for node in gpu_nodes:
        logging.info(f"Checking node: {node}")
        check_gpus_on_node(node)

    print_summary()


if __name__ == "__main__":
    main()
