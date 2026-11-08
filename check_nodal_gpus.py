import os
import sys
import logging
import numpy as np
import subprocess

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]: %(message)s")

# Get base directory
base_dir = os.getenv("PULSELINE_VER1_DIR")
if not base_dir:
    logging.error("Error: PULSELINE_VER1_DIR environment variable is not set.")
    sys.exit(1)

# Import config loader
sys.path.insert(0, os.path.join(base_dir, "input_file_dir_init/scripts"))
try:
    from read_input_file_dir import load_parameters
except ImportError:
    logging.error("Error importing required modules.")
    sys.exit(1)

# Config file path
config_file_path = os.path.join(base_dir, "input_file_dir_init/input_dir/input_file_directory.txt")

# Result containers
gpu_status_summary = []
remaining_gpu_status_summary = []


def check_gpus_on_node(node, summary_list):
    node_result = {
        "node": node,
        "reachable": True,
        "gpu_0": False,
        "gpu_1": False,
        "gpu_names": {}
    }

    try:
        result = subprocess.run(
            [
                "ssh",
                "-o", "BatchMode=yes",                   # Disable password prompt
                "-o", "PreferredAuthentications=publickey",
                "-o", "ConnectTimeout=5",
                "-o", "StrictHostKeyChecking=no",
                node,
                "nvidia-smi --query-gpu=index,name --format=csv,noheader"
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=10
        )

        if result.returncode != 0:
            stderr_msg = result.stderr.strip()
            node_result["reachable"] = False

            if "Permission denied" in stderr_msg:
                logging.error(f"{node}: SSH key authentication failed (password required).")
            elif "Host key verification failed" in stderr_msg:
                logging.error(f"{node}: Host key verification failed.")
            elif "Could not resolve hostname" in stderr_msg:
                logging.error(f"{node}: Hostname could not be resolved.")
            elif "Connection refused" in stderr_msg:
                logging.error(f"{node}: SSH connection refused.")
            elif "Connection timed out" in stderr_msg:
                logging.error(f"{node}: SSH connection timed out.")
            else:
                logging.error(f"{node}: SSH error: {stderr_msg or 'Unknown error'}")

            summary_list.append(node_result)
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

    summary_list.append(node_result)


def print_summary(summary, title):
    print(f"\n====== {title} ======")

    total_nodes = len(summary)
    unreachable_nodes = [n["node"] for n in summary if not n["reachable"]]
    total_reachable = total_nodes - len(unreachable_nodes)

    gpu_0_missing = []
    gpu_1_missing = []
    total_gpus_detected = 0

    for entry in summary:
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
    # Load parameters
    try:
        params = load_parameters(config_file_path)
    except Exception as e:
        logging.error(f"Error loading parameters: {e}")
        sys.exit(1)

    avail_gpus_file_dir = params.get("avail_gpus_file_dir")
    if not avail_gpus_file_dir:
        logging.error("Missing 'avail_gpus_file_dir'")
        sys.exit(1)

    # -------------------------
    # Load pipeline nodes
    # -------------------------
    avail_file = os.path.join(avail_gpus_file_dir, "avail_gpu_nodes.txt")
    if not os.path.exists(avail_file):
        logging.error(f"{avail_file} not found")
        sys.exit(1)

    gpu_nodes = np.loadtxt(avail_file, dtype=str)
    if gpu_nodes.ndim == 0:
        gpu_nodes = [gpu_nodes.item()]

    # -------------------------
    # Load all nodes
    # -------------------------
    all_file = os.path.join(avail_gpus_file_dir, "all_gpu_nodes.txt")
    if not os.path.exists(all_file):
        logging.error(f"{all_file} not found")
        sys.exit(1)

    all_gpu_nodes = np.loadtxt(all_file, dtype=str)
    if all_gpu_nodes.ndim == 0:
        all_gpu_nodes = [all_gpu_nodes.item()]

    # -------------------------
    # Compute remaining nodes
    # -------------------------
    remaining_nodes = sorted(list(set(all_gpu_nodes) - set(gpu_nodes)))

    # -------------------------
    # Check pipeline nodes
    # -------------------------
    logging.info("=== Checking PIPELINE GPU nodes ===")
    for node in gpu_nodes:
        logging.info(f"Checking node: {node}")
        check_gpus_on_node(node, gpu_status_summary)

    print_summary(gpu_status_summary, "PIPELINE GPU NODES STATUS")

    # -------------------------
    # Check remaining nodes
    # -------------------------
    logging.info("=== Checking REMAINING GPU nodes ===")
    for node in remaining_nodes:
        logging.info(f"Checking node: {node}")
        check_gpus_on_node(node, remaining_gpu_status_summary)

    print_summary(remaining_gpu_status_summary, "REMAINING GPU NODES STATUS")


if __name__ == "__main__":
    main()
