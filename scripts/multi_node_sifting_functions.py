import os
import sys
import time
import logging
import subprocess
from multiprocessing import Pool, Process

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Load base directory from environment variable
base_dir = os.getenv("PULSELINE_DEV_DIR")
if not base_dir:
    logging.error("Error: PULSELINE_DEV_DIR environment variable is not set.")
    sys.exit(1)

# Add required paths to sys.path
required_paths = ["input_file_dir_init/scripts"]
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
    environ_init_script = params.get("environ_init_script")
    first_stage_sifting_path = params.get("first_stage_sifting_path")
    multi_node_sifting_script_path = params.get("multi_node_sifting_script_path")
    workers = int(params.get("workers_per_node", 4))  # Default to 4 CPUs if not specified
    logging.info("Configuration parameters loaded successfully.")
except Exception as e:
    logging.error(f"Error loading parameters from configuration file: {e}", exc_info=True)
    sys.exit(1)

def sifting(command):
    """Executes a system command."""
    os.system(command)

def execute_siftings(commands, workers):
    """Executes multiple commands in parallel using multiprocessing."""
    with Pool(workers) as p:
        p.map(sifting, commands)

def parallel_sifting(node_alias, gpu_id, data_id, input_file_dir, log_dir, command_template):
    """
    SSH into node and execute sifting commands in parallel.

    :param node_alias: Node to SSH into.
    :param gpu_id: GPU ID.
    :param input_file_dir: Directory containing input files.
    :param log_dir: Directory for logging.
    :param command_template: Command template for execution.
    """
    files_to_process = [
        f for f in os.listdir(input_file_dir)
        if f.startswith("AA") and f.endswith(f"node_{node_alias}_gpu_id_{gpu_id}.txt")
    ]

    if not files_to_process:
        logging.info(f"No files found for node {node_alias}, GPU {gpu_id}. Skipping...")
        return

    logging.info(f"Files to process for node {node_alias}, GPU {gpu_id}: {files_to_process}")

    # Generate space-separated command strings (avoiding JSON)
    sifting_commands = ";;".join([
        command_template.format(
            first_stage_sifting_path=first_stage_sifting_path,  
            file=file,
            node_alias=node_alias,
            gpu_id=gpu_id,
            data_id = data_id
        )
        for file in files_to_process
    ])

    print("Commands to execute:", sifting_commands)

    # SSH command to execute sifting remotely
    ssh_command = (
        f'ssh -X {node_alias} "source {environ_init_script} && '
        f'python3 {multi_node_sifting_script_path} \'{sifting_commands}\' {workers} '
        f'> {log_dir}/sifting_cpu_log_{node_alias}_gpu_{gpu_id}.log 2>&1 &"'
    )

    logging.info(f"Executing SSH command in background: {ssh_command}")

    # Run SSH command in background
    subprocess.run(ssh_command, shell=True)


def multi_node_sifting(node_alias, data_id, input_file_dir, log_dir, gpu_0_start_delay, gpu_1_start_delay, command_template):
    """
    Process files for both GPUs of the node concurrently with separate delays.
    """
    processes = []

    # Start GPU 0 process with its specific delay
    time.sleep(gpu_0_start_delay)
    p_gpu_0 = Process(
        target=parallel_sifting,
        args=(node_alias, 0, data_id, input_file_dir, log_dir, command_template)
    )
    processes.append(p_gpu_0)
    p_gpu_0.start()
    logging.info(f"Started processing for GPU 0 on {node_alias}.")

    # Start GPU 1 process with its specific delay
    time.sleep(gpu_1_start_delay)
    p_gpu_1 = Process(
        target=parallel_sifting,
        args=(node_alias, 1, data_id, input_file_dir, log_dir, command_template)
    )
    processes.append(p_gpu_1)
    p_gpu_1.start()
    logging.info(f"Started processing for GPU 1 on {node_alias}.")

    # Wait for all processes to complete
    for p in processes:
        p.join()

    logging.info(f"Processing completed for both GPUs on {node_alias}.")

if __name__ == "__main__":
    
    # Read command string and worker count from command-line arguments
    command_string = sys.argv[1]  # Space-separated commands
    commands = command_string.split(";;")  # Split commands
    workers = int(sys.argv[2])

    execute_siftings(commands, workers)