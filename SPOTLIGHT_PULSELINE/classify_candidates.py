#!/usr/bin/env python2

import os
import sys
import argparse
import subprocess

# ------------------------------------------------------------------
# Environment sanity (IMPORTANT for psrchive)
# ------------------------------------------------------------------
os.environ["PYTHONNOUSERSITE"] = "1"
os.environ["PYTHONPATH"] = (
    "/usr/local/lib/python2.7/site-packages:"
    "/lustre_archive/apps/tdsoft/usr/src/presto_old/lib/python"
)
os.environ["LD_LIBRARY_PATH"] = (
    "/usr/local/lib:" + os.environ.get("LD_LIBRARY_PATH", "")
)

# ------------------------------------------------------------------
# Get base directory
# ------------------------------------------------------------------
base_dir = os.getenv("PULSELINE_DEV_DIR")
if not base_dir:
    print("Error: PULSELINE_DEV_DIR environment variable is not set.")
    sys.exit(1)

# ------------------------------------------------------------------
# Extend PYTHONPATH dynamically
# ------------------------------------------------------------------
relative_paths = [
    "input_file_dir_init/scripts",
    "SPOTLIGHT_PULSELINE/scripts",
    "scripts"
]

for rel in relative_paths:
    sys.path.insert(0, os.path.join(base_dir, rel))

# ------------------------------------------------------------------
# Imports
# ------------------------------------------------------------------
try:
    from GHVFDT_classifier_functions import *
    from remove_files import *
    from read_input_file_dir import load_parameters
except ImportError as e:
    print("Error importing required modules: %s" % str(e))
    sys.exit(1)

# ------------------------------------------------------------------
# Constants
# ------------------------------------------------------------------
config_file_path = os.path.join(
    base_dir,
    "input_file_dir_init/input_dir/input_file_directory.txt"
)

folded_outputs = "folded_outputs"

# ------------------------------------------------------------------
# Main logic
# ------------------------------------------------------------------
def run_classifier(data_id):

    if not os.path.exists(config_file_path):
        print("Error: Configuration file not found: %s" % config_file_path)
        sys.exit(1)

    try:
        params = load_parameters(config_file_path)
    except Exception as e:
        print("Error loading configuration: %s" % str(e))
        sys.exit(1)

    use_GHVFDT = int(params.get("use_GHVFDT", 0))
    use_PICS   = int(params.get("use_PICS", 0))

    if use_GHVFDT + use_PICS != 1:
        print("Error: Select exactly one classifier (GHVFDT or PICS).")
        sys.exit(1)

    PICS_function_path = params.get("PICS_function_path")
    python2_exe_sh_path = params.get("python2_exe_sh_path")
    cand_prob_threshold = params.get("cand_prob_threshold", "0.5")
    machine_learning_files_path = params.get("machine_learning_files_path")
    python2_env_path = params.get("python2_env_path")
    pulseline_output_dir = params.get("pulseline_output_dir")
    classifier_output_dir = params.get("classifier_output_dir")

    classifier_input_dir = os.path.join(pulseline_output_dir, data_id)

    if not all([
        machine_learning_files_path,
        python2_env_path,
        classifier_input_dir,
        classifier_output_dir
    ]):
        print("Error: Missing required parameters in config file.")
        sys.exit(1)

    classifier_output_path = os.path.join(classifier_output_dir, data_id)

    clean_directory_parallel(
        classifier_output_path,
        params.get("workers_per_node")
    )

    classifier_files_dir = os.path.join(
        classifier_input_dir,
        folded_outputs
    )

    # -------------------- PICS --------------------
    if use_PICS == 1:
        try:
            cmd = [
                python2_exe_sh_path,
                PICS_function_path,
                classifier_files_dir,
                classifier_output_path,
                str(cand_prob_threshold)
            ]
            print("Running PICS: %s" % " ".join(cmd))
            subprocess.check_call(cmd)
        except Exception as e:
            print("Error running PICS classifier: %s" % str(e))
            sys.exit(1)

    # -------------------- GHVFDT --------------------
    elif use_GHVFDT == 1:
        try:
            GHVFDT_classifier_cmds(
                classifier_files_dir,
                classifier_output_path,
                python2_env_path,
                machine_learning_files_path
            )
        except Exception as e:
            print("Error running GHVFDT classifier: %s" % str(e))
            sys.exit(1)

# ------------------------------------------------------------------
# Entry point
# ------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run classifier for a given data_id"
    )
    parser.add_argument("data_id", help="Data ID")

    args = parser.parse_args()
    run_classifier(args.data_id)