import os
import re
import sys
import time
import logging
from pathlib import Path
import subprocess
import numpy as np
from multiprocessing import Process

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]: %(message)s")

# ===== Redirect print() to logging =====
class PrintToLogger:
    def write(self, message):
        message = message.strip()
        if message:
            logging.info(message)
    def flush(self):
        pass

sys.stdout = PrintToLogger()
sys.stderr = PrintToLogger()

# Get the base directory from the environment variable
base_dir = os.getenv("PULSELINE_VER1_DIR")
if not base_dir:
    logging.error("Error: PULSELINE_VER1_DIR environment variable is not set.")
    sys.exit(1)

# Add required paths to sys.path
required_paths = [
    "input_file_dir_init/scripts",
    "SPOTLIGHT_PULSELINE/scripts",
    "input_file_dir_init",
    "SPOTLIGHT_PULSELINE",
    "scripts"
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
    from process_multi_node_aa_functions import *
    from multi_node_sifting_functions import *
    from multi_node_folding_functions import *
    from multi_node_rfi_mitigation import *
    from multi_node_classification_functions import *
    from access_informations import *
    from remove_files import *
    logging.info("Modules imported successfully.")
except ImportError as e:
    logging.error("Error importing required modules.", exc_info=True)
    sys.exit(1)

# Define the obs details and configuration file
obs_file_path = os.path.join(base_dir, "GTAC_obs_details/obs_details.txt")
config_file_path = os.path.join(base_dir, "input_file_dir_init/input_dir/input_file_directory.txt")
input_file_generator_dir = os.path.join(base_dir, "input_file_dir_init")

# Check if configuration file exists
if not os.path.exists(config_file_path):
    logging.error(f"Configuration file not found: {config_file_path}")
    sys.exit(1)


def main():
    """
    Main function to execute astro-accelerate jobs on available GPU nodes.
    """

    # Step 0: Load basic configuration
    if not os.path.exists(config_file_path):
        print(f"Configuration file not found: {config_file_path}")
        sys.exit(1)

    try:
        params = load_parameters(config_file_path)
    except Exception as e:
        print(f"Error loading parameters from configuration file: {e}")
        sys.exit(1)

    # Step 1: Extract all parameters and paths from the configuration file
    # File remove, and data status, file, and input file generator path
    file_remove_module_path = params.get('file_remove_module_path')
    data_status_check_path = params.get('data_status_check_path')
    generate_input_data_file = params.get('generate_input_data_file')
    data_and_input_file_generator_path = params.get('data_and_input_file_generator_path')

    # Directories and other required parameters
    aa_input_file_dir = params.get('aa_input_file_dir')
    aa_output_dir = params.get('aa_output_dir')
    pulseline_input_dir = params.get('pulseline_input_dir')
    pulseline_input_file_dir = params.get('pulseline_input_file_dir')
    pulseline_output_dir = params.get('pulseline_output_dir')
    avail_gpus_file_dir = params.get('avail_gpus_file_dir')
    environ_init_script = params.get('environ_init_script')
    log_base_dir = params.get('aa_pulseline_log_dir')

    gpu_0_start_delay = int(params.get('gpu_0_start_delay', 5))  # Default: 5 seconds
    gpu_1_start_delay = int(params.get('gpu_1_start_delay', 10))  # Default: 10 seconds
    file_processing_delay = int(params.get('file_processing_delay', 5))  # Default: 5 seconds

    # RFI mitigation flags, script and othe necessary paths
    fil_flag = params.get('fil_flag')
    flag_rfi = params.get('flag_rfi')
    raw_input_base_dir = params.get('raw_input_base_dir')

    # Final_stage_candidate_sifting module runner paths and flags
    beam_level_sifting = params.get('beam_level_sifting')
    final_stage_sifting_path = params.get('final_stage_sifting_path')

    # Classifier module runner paths and flags
    do_classify = params.get('do_classify')
    cand_prob_threshold = params.get('cand_prob_threshold')
    PICS_output_script_path = params.get('PICS_output_script_path')
    classifier_output_dir = params.get('classifier_output_dir')

    # Final outputs code path and fildata flag
    delete_fildata = params.get('delete_fildata')
    final_outputs_script_path = params.get("final_outputs_script_path")

    # Get the common GPU node and workers per node
    common_node_id = params.get('common_node_id')
    workers_per_node = params.get('workers_per_node')

    #Step 2: Generate required input data infos for further processing
    command = (
        f'ssh -X {common_node_id} "'
        f'echo Step 1: Connecting to node {common_node_id} && '
        f'source {environ_init_script} && '
        f'echo Step 2: Environment sourced && '
        f'python3 {data_status_check_path} && '
        f'python3 {generate_input_data_file}'
        f'"'
    )

    # Print to verify correctness
    print("Running command:")
    print(command)

    # Execute the command
    subprocess.run(command, shell=True, check=True)

    # Step 3: Load the observation details
    GTAC_scan, Target, Total_beams, uGMRT_band = extract_gtac_obs_informations(obs_file_path)

    # Ensure base_dir is defined
    report_path = os.path.join(base_dir, "analysis_report.log")
    global_report_path = os.path.join(base_dir, "outfiles/global_analysis_report.log")
    status_path = os.path.join(base_dir, "analysis_status.log")
    executed_targets = []

    # Set analysis_status = ON at the start in both place
    with open(status_path, "w") as status_file:
        status_file.write("analysis_status = ON\n")

    # Initialise the report file
        with open(report_path, "w") as log_file:
            log_file.write(f"Sequential data execution for each target started successfully.\n")


    for i in range(0, int(len(GTAC_scan))):

        
        #Extract the SCAN ID, data ID, band ID etc (observation informations)
        scan_id = str(GTAC_scan[i])
        data_id = str(Target[i])
        total_beams = Total_beams[i]
        band_id = str(uGMRT_band[i])

        print(f"[{i}] Processing Scan ID: {scan_id}, Data ID: {data_id}, Band: {band_id}")


        # Define the AA output_path depending on the data_id, and if not present create
        aa_output_path = os.path.join(aa_output_dir, data_id)
        os.makedirs(aa_output_path, exist_ok=True)

        # Define and the log_directory to store the logs, and if not present create
        log_output_path = os.path.join(log_base_dir, data_id)
        os.makedirs(log_output_path, exist_ok=True)

       # Step 4: Clean the directories for making the pipeline able to rerun on same data without any errors.
       # Loop for multiple directory
        for directory in [
            aa_output_path,
            log_output_path,
            os.path.join(pulseline_input_dir, data_id),
            os.path.join(pulseline_output_dir, data_id),
            os.path.join(classifier_output_dir, data_id)           
            ]:
            print(f"Starting cleaning of directory: {directory} on node: {common_node_id}")

            command = (
                f'ssh -X {common_node_id} "'
                f'echo Cleaning {directory} on {common_node_id} && '
                f'source {environ_init_script} && echo Environment sourced && '
                f'if [ -d {directory} ]; then '
                f'  echo Directory exists. Running cleaning script. && '
                f'  python3 {file_remove_module_path} {directory} {workers_per_node}; '
                f'else '
                f'  echo Directory {directory} does not exist. Skipping.; '
                f'fi && '
                f'echo Finished cleaning {directory}"'
            )

            print(f"Running command:\n{command}\n")

            try:
                subprocess.run(command, shell=True, check=True)
                print(f"Successfully handled directory: {directory}\n")
            except subprocess.CalledProcessError as e:
                print(f"Error occurred while cleaning or creating directory: {directory}")
                print(f"Error details: {e}\n")

        # Step 5: Generate input files to run the pipeline
        command = (
            f'ssh -X {common_node_id} "'
            f'echo Step 1: Connecting to node {common_node_id} && '
            f'source {environ_init_script} && echo Step 2: Environment sourced && '
            f'python3 {data_and_input_file_generator_path} {scan_id} {data_id} {input_file_generator_dir} {band_id} && '
            f'>> {log_output_path}/cpu_log_common_node_{common_node_id}.log 2>&1"'
        )

        # Print to verify correctness
        print(f"common_node_id = {common_node_id}")
        print(f"Running command: {command}")

        # Execute the command
        subprocess.run(command, shell=True, check=True)

        print(f"Generating input files done for all the filterbank files.")

        os.system("sleep 2")  # Sleeps for 2 seconds

        # Step 6: Load available GPU nodes
        try:
            avail_gpu_nodes = np.atleast_1d(np.loadtxt(
                os.path.join(avail_gpus_file_dir, 'avail_gpu_nodes.txt'),
                dtype=str
            )).tolist()
            
        except Exception as e:
            print(f"Error reading available GPU nodes: {e}")
            sys.exit(1)

        if not isinstance(avail_gpu_nodes, (list, np.ndarray)) or len(avail_gpu_nodes) == 0:
            print("No available GPU nodes found in the file.")
            sys.exit(1)

        # Step 7: Construct the SSH commands for RFI mitigation, GPU processing and same node CPU processing for search, shift, fold, etc.

        command_template_0 = (
            'ssh -X {node_alias} "source {environ_init_script} && '
            'python3 {rfi_clean_module_path} {fil_files} {data_id} {scan_id} {band_id} '
            '> {log_dir}/rfi_mitigation_cpu_log_{node_alias}_gpu_{gpu_id}.log 2>&1"'
        )


        command_template_1 = (
            'ssh -X {node_alias} "source {environ_init_script} && '
            'source {aa_executable_file_dir}/environment.sh && '
            'bash {aa_executable_file_dir}/astro-accelerate.sh {input_file_path} {output_dir} '
            '> {log_dir}/aa_gpu_log_{node_alias}_gpu_{gpu_id}.log 2>&1"'
        )

        command_template_2 = (
            'python3 {first_stage_sifting_path} {file} {node_alias} {gpu_id} {data_id}'
        )

        command_template_3 = (
            'ssh -X {node_alias} "source {environ_init_script} && '
            'python3 {beam_level_folding_path} {node_alias} {gpu_id} {data_id} {scan_id}'
            '> {log_dir}/folding_cpu_log_{node_alias}_gpu_{gpu_id}.log 2>&1"'
        )

        command_template_4 = (
            'ssh -X {node_alias} '
            '"{python2_exe} {candidate_classifier_path} '
            '{input_dir} {output_dir} {node_alias} '
            '> {log_dir}/classification_cpu_log_{node_alias}.log 2>&1"'
        )

        
        # Step 8: Process each node concurrently for the command template 0 for runnign FILTOOL based RFI mitigation
        status_log = os.path.join(
            raw_input_base_dir,
            scan_id,
            fil_flag,
            data_id,
            "RFI_mitigation_status.log"
        )

        # Flag check + log-file existence check
        if flag_rfi == 1:

            if os.path.exists(status_log):
                print(f"[INFO] RFI mitigation already done. Log found at:")
                print(f"       {status_log}")
                print("[INFO] Skipping RFI cleaning step.")

            else:
                print("[INFO] RFI log not found. Starting RFI mitigation...")

                processes = []

                # Launch RFI mitigation on each GPU node
                for node_alias in avail_gpu_nodes:
                    p = Process(
                        target=multi_node_rfi_mitigation,
                        args=(
                            node_alias,
                            data_id,
                            scan_id,
                            band_id,
                            pulseline_input_file_dir,
                            log_output_path,
                            gpu_0_start_delay,
                            gpu_1_start_delay,
                            file_processing_delay,
                            command_template_0
                        )
                    )
                    processes.append(p)
                    p.start()

                # Wait for all nodes to finish
                for p in processes:
                    p.join()

                # Write completion status
                os.makedirs(os.path.dirname(status_log), exist_ok=True)
                with open(status_log, "a") as status_file:
                    status_file.write("RFI mitigation completed for all beam files.\n")

                print("[INFO] RFI mitigation completed for all beam files.")

                time.sleep(2)  # small cooldown

        else:
            print("[INFO] flag_rfi is disabled. Skipping RFI mitigation.")

        # Step 9: Process each node concurrently for the command template 1 for running AA pulsar search
        processes = []
        for node_alias in avail_gpu_nodes:
            p = Process(
                target=process_node,
                args=(
                    node_alias,
                    data_id,
                    aa_input_file_dir,
                    aa_output_path,
                    log_output_path,
                    gpu_0_start_delay,
                    gpu_1_start_delay,
                    file_processing_delay,
                    command_template_1
                )
            )
            processes.append(p)
            p.start()   # fixed indentation

        for p in processes:
            p.join()

        os.system("sleep 2")  # Sleeps for 2 seconds
        

        # Step 10: Process each node concurrently for the command template 2 for running first stage sifting in a parallel manner
        processes = []
        for node_alias in avail_gpu_nodes:
            p = Process(
                target=multi_node_sifting,
                args=(
                    node_alias,
                    data_id,
                    aa_input_file_dir,
                    log_output_path,
                    gpu_0_start_delay,
                    gpu_1_start_delay,
                    command_template_2
                )
            )
            processes.append(p)
            p.start()

        for p in processes:
            p.join()

        os.system("sleep 2")  # Sleeps for 2 seconds


        # Step 11: Process all the search level sifted candidates output at once in common GPU node to do a beam level candidate filtration
        if beam_level_sifting == 1:
            command = (
                f'ssh -X {common_node_id} "'
                f'echo Step 1: Connecting to {common_node_id} && '
                f'source {environ_init_script} && echo Step 2: Environment Loaded && '
                f'which python3 && echo Step 3: Python Located && '
                f'ls -l {final_stage_sifting_path} && echo Step 4: Checking Script Permissions && '
                f'python3 {final_stage_sifting_path} {scan_id} {data_id} && echo Step 5: Python Script Executed '
                f'>> {log_output_path}/beam_sifting_cpu_log_common_node_{common_node_id}.log 2>&1"'
            )

            # Print the command for manual testing
            print(f"\nRunning SSH Command:\n{command}\n")

            # Execute the SSH command
            try:
                subprocess.run(command, shell=True, check=True)
            except subprocess.CalledProcessError as e:
                print(f"Error: SSH command failed with return code {e.returncode}")
                print(f"Check log file: {log_output_path}/beam_sifting_cpu_log_common_node_{common_node_id}.log")


        print(f"Beam level sifting completed for all the initial sorted candidates.")

        os.system("sleep 2")  # Sleeps for 2 seconds

     
        # Step 12: Process each node concurrently for the command template 3 for multinode CPU folding
        processes = []
        for node_alias in avail_gpu_nodes:
            p = Process(
                target=multi_node_folding,
                args=(
                    node_alias, data_id, scan_id, log_output_path, gpu_0_start_delay, gpu_1_start_delay, command_template_3)
            )
            processes.append(p)
            p.start()

        for p in processes:
            p.join()

        print(f"Folding completed for all the filtered candidates.")

        os.system("sleep 2")  # Sleeps for 2 seconds
       

        # Step 13: Process all the folded candidate output AR files for multi-node classification
        if do_classify == 1:

            classifier_input_file_path = os.path.join(
                pulseline_output_dir,
                data_id,
                "folded_outputs"
            )

            #Create output dir if don't exist
            classifier_output_file_path = os.path.join(
                classifier_output_dir,
                data_id
            )
            if not os.path.isdir(classifier_output_file_path):
                os.makedirs(classifier_output_file_path)

            processes = []

            for node_alias in avail_gpu_nodes:

                p = Process(
                    target=multi_node_classification,
                    args=(
                        classifier_input_file_path, classifier_output_file_path, node_alias, log_output_path, command_template_4)
                )

                processes.append(p)
                p.start()

            for p in processes:
                p.join()

            print("Classification completed for all the filtered candidates.")

            command = (
                f'ssh -X {common_node_id} "'
                f'echo Step 1: Connecting to {common_node_id} && '
                f'source {environ_init_script} && '
                f'echo Step 2: Environment Loaded && '
                f'echo Step 3: Python Located && '
                f'ls -l {PICS_output_script_path} && '
                f'echo Step 4: PICS Output Script Found && '
                f'python3 {PICS_output_script_path} '
                f'{classifier_input_file_path} '
                f'{classifier_output_file_path} '
                f'{cand_prob_threshold}'
                f'" >> {log_output_path}/classification_summary_log_common_node_{common_node_id}.log 2>&1'
            )

            # Print the command for debugging
            print(f"\nRunning SSH Command:\n{command}\n")

            # Execute the command
            try:
                subprocess.run(command, shell=True, check=True)
                print("Classifier outputs processed successfully.")

            except subprocess.CalledProcessError as e:
                print(f"Error: SSH command failed with return code {e.returncode}")
                print(f"Check log file: {log_output_path}/classification_summary_log_common_node_{common_node_id}.log")
            os.system("sleep 2")
  
        # Step 14: Generate the final outputs for sorted candidates
        if not final_outputs_script_path:
            print("Error: Missing required path for final outputs generation.")
            sys.exit(1)

        # Construct SSH command with additional debug checks
        command = (
            f'ssh -X {common_node_id} "'
            f'echo Step 1: Connecting to {common_node_id} && '
            f'source {environ_init_script} && echo Step 2: Environment Loaded && '
            f'which python3 && echo Step 3: Python Located && '
            f'ls -l {final_outputs_script_path} && echo Step 4a: Final Outputs Script Found && '
            f'python3 {final_outputs_script_path} {scan_id} {data_id} {band_id} && echo Step 4b: Final Outputs Generated '
            f'>> {log_output_path}/final_output_cpu_log_common_node_{common_node_id}.log 2>&1"'
        )

        # Print the command for debugging
        print(f"\nRunning SSH Command:\n{command}\n")

        # Execute the command
        try:
            subprocess.run(command, shell=True, check=True)
            print("Final outputs generated successfully.")
        except subprocess.CalledProcessError as e:
            print(f"Error: SSH command failed with return code {e.returncode}")
            print(f"Check log file: {log_output_path}/final_output_cpu_log_common_node_{common_node_id}.log")


        #Track the executed targets
        executed_targets.append(data_id)

        # Update report with latest executed scan info
        with open(report_path, "a") as log_file:
            log_file.write(f"Executed scan: {data_id}\n")
        
        #Update in global file as well
        with open(global_report_path, "a") as global_log_file:
            global_log_file.write(
                "Observation: {} | Executed scan: {}\n".format(scan_id, data_id)
            )
        

        # Step 15: Clean the directories again to avoid unnecessary storage consumption.
        # ---- NEW: Filterbank files directory ----
        fil_data_dir = os.path.join(raw_input_base_dir, scan_id, "FilData", data_id)

        # Only clean the FIL files in the FilData directory if selected
        if delete_fildata == 1:
            if os.path.exists(fil_data_dir):
                for filename in os.listdir(fil_data_dir):
                    if filename.endswith(".fil"):
                        file_path = os.path.join(fil_data_dir, filename)
                        os.remove(file_path)
                        print(f"Removed: {file_path}")
            else:
                print(f"Directory not found: {fil_data_dir}")

        elif delete_fildata == 0:
            print("You chose not to delete the filterbank files in the FilData directory.")
        
        else:
            print("Invalid value for delete_fildata flag. Please set it to 1 (to delete) or 0 (to keep). Skipping filterbank file deletion.")

        # Loop for multiple directory cleaning (AA output, pulseline input, pulseline output, classifier output)
        for directory in [
            aa_output_path,
            os.path.join(pulseline_input_dir, data_id),
            os.path.join(pulseline_output_dir, data_id),
            os.path.join(classifier_output_dir, data_id)
        ]:
            print(f"Starting cleaning of directory: {directory} on node: {common_node_id}")

            command = (
                f'ssh -X {common_node_id} "'
                f'echo Cleaning {directory} on {common_node_id} && '
                f'source {environ_init_script} && echo Environment sourced && '
                f'if [ -d {directory} ]; then '
                f'  echo Directory exists. Running cleaning script. && '
                f'  python3 {file_remove_module_path} {directory} {workers_per_node}; '
                f'else '
                f'  echo Directory {directory} does not exist. Skipping.; '
                f'fi && '
                f'echo Finished cleaning {directory}"'
            )

            print(f"Running command:\n{command}\n")

            try:
                subprocess.run(command, shell=True, check=True)
                print(f"Successfully handled directory: {directory}\n")
            except subprocess.CalledProcessError as e:
                print(f"Error occurred while cleaning or creating directory: {directory}")
                print(f"Error details: {e}\n")
    
    #Step 16: Generate final status files
    # Final status summary (overwrite again)
    with open(report_path, "w") as log_file:
        if len(executed_targets) == 0:
            log_file.write("No target got executed, maybe the data recording is going on.\n")
        elif len(executed_targets) == len(GTAC_scan):
            log_file.write("All targets got executed successfully for the current run.\n")
        else:
            log_file.write("Partial execution: executed scans - " + ", ".join(executed_targets) + "\n")                                                   
    
    # Write "analysis_status = OFF" at the end
    with open(status_path, "w") as status_file:
        status_file.write("analysis_status = OFF\n")

if __name__ == "__main__":
    main()
