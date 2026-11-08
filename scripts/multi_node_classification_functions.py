import os
import sys
import time
import logging
import subprocess

# ----------------------------------------------------------
# Configure logging
# ----------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ----------------------------------------------------------
# Load base directory
# ----------------------------------------------------------
base_dir = "/lustre_data/spotlight/data/pulsar_fft_pipe_ver1"


# ----------------------------------------------------------
# Add required paths
# ----------------------------------------------------------
required_paths = [
    "input_file_dir_init/scripts",
]

for relative_path in required_paths:
    full_path = os.path.join(base_dir, relative_path)

    if os.path.exists(full_path):
        sys.path.insert(0, full_path)
        logging.info("Added to sys.path: %s", full_path)
    else:
        logging.warning("Path does not exist: %s", full_path)

# ----------------------------------------------------------
# Import required modules
# ----------------------------------------------------------
try:
    from read_input_file_dir import load_parameters
    logging.info("Modules imported successfully.")
except ImportError:
    logging.exception("Failed to import required modules.")
    sys.exit(1)

# ----------------------------------------------------------
# Read configuration
# ----------------------------------------------------------
config_file_path = os.path.join(
    base_dir,
    "input_file_dir_init/input_dir/input_file_directory.txt"
)

if not os.path.exists(config_file_path):
    logging.error("Configuration file not found: %s", config_file_path)
    sys.exit(1)

try:
    params = load_parameters(config_file_path)
    python2_exe = params.get("python2_exe_sh_path")
    candidate_classifier_path = params.get("candidate_classifier_path")

    logging.info("Configuration parameters loaded successfully.")

except Exception:
    logging.exception("Unable to read configuration.")
    sys.exit(1)


# ----------------------------------------------------------
# Run classification on one node
# ----------------------------------------------------------
def multi_node_classification(
    classifier_input_dir,
    classifier_output_dir,   
    node_alias,
    log_dir,
    command_template
):
    """
    Execute one classification job on one node.
    """

    if not os.path.isdir(log_dir):
        os.makedirs(log_dir)

    try:

        command = command_template.format(
            python2_exe=python2_exe,
            candidate_classifier_path =candidate_classifier_path,
            input_dir=classifier_input_dir,
            output_dir=classifier_output_dir,
            node_alias=node_alias,
            log_dir=log_dir
        )

        logging.info("Executing on node %s", node_alias)
        logging.info("Command:\n%s", command)

        result = subprocess.run(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        logging.info("Return code: %d", result.returncode)

        if result.stdout:
            logging.info("STDOUT:\n%s", result.stdout)

        if result.stderr:
            logging.error("STDERR:\n%s", result.stderr)

        result.check_returncode()

        logging.info("Processing completed on node %s", node_alias)

    except subprocess.CalledProcessError as e:
        logging.error(
            "Command failed on %s (return code %d)",
            node_alias,
            e.returncode
        )

    except Exception:
        logging.exception("Unexpected error on node %s", node_alias)
