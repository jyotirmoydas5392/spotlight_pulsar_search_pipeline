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
#logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]: %(message)s")

# ===== Configure logging to write to file =====
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s]: %(message)s",
    handlers=[
        logging.FileHandler("my_script.log", mode="w"),
        logging.StreamHandler(sys.stdout)  # Uncomment if you still want log output on terminal
    ]
)

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

    # Step 0: Extract all parameters and paths

    # File remove, and data plus input file generator path
    file_remove_module_path = params.get('file_remove_module_path')
    data_and_input_file_generator_path = params.get('data_and_input_file_generator_path')

    # Directories and other required parameters
    aa_executable_file_dir = params.get('aa_executable_file_dir')
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
    rfi_clean_module_path = params.get('rfi_clean_module_path')

    # First_stage_candidate_sifting module runner path
    first_stage_sifting_path = params.get('first_stage_sifting_path')

    # Final_stage_candidate_sifting module runner paths and flags
    beam_level_sifting = params.get('beam_level_sifting')
    final_stage_sifting_path = params.get('final_stage_sifting_path')

    # Beam_level_candidate_folding module runner path
    pulsarx_folding_flag = params.get('pulsarx_folding_flag')
    presto_folding_flag = params.get('presto_folding_flag')
    beam_level_folding_path = params.get('beam_level_folding_path')

    # Classifier module runner paths and flags
    do_classify = params.get('do_classify')
    python2_exe_sh_path = params.get('python2_exe_sh_path')
    candidate_classifier_path = params.get('candidate_classifier_path')
    classifier_output_dir = params.get('classifier_output_dir')

    # Final outputs and summary plots genearting code path
    final_outputs_script_path = params.get("final_outputs_script_path")
    summary_plot_code_path = params.get('summary_plot_code_path')

    # Get the common GPU node and workers per node
    common_node_id = params.get('common_node_id')
    workers_per_node = params.get('workers_per_node')

    # Get the path for writing pipeline status
    pulsar_search_status_path = params.get("pulsar_search_status_path")


    # Step 1: Load the observation details
    GTAC_scan, Target, Total_beams, uGMRT_band, Input_file = extract_gtac_obs_informations(obs_file_path)

    # Ensure base_dir is defined
    report_path = os.path.join(base_dir, "analysis_report.log")
    status_path = os.path.join(base_dir, "analysis_status.log")
    executed_targets = []

    # Set analysis_status = ON at the start in both place
    with open(status_path, "w") as status_file:
        status_file.write("analysis_status = ON\n")

    # Initialise the report file
        with open(report_path, "w") as log_file:
            log_file.write(f"Sequential data execution for each target started successfully.\n")


    for i in range(0, int(len(GTAC_scan))):

        start_scan = time.time()
        
        #Extract the SCAN ID, data ID, band ID etc (observation informations)
        scan_id = str(GTAC_scan[i])
        data_id = str(Target[i])
        total_beams = Total_beams[i]
        band_id = str(uGMRT_band[i])
        data_type = Input_file[i].strip()

        print(f"[{i}] Processing Scan ID: {scan_id}, Data ID: {data_id}, Band: {band_id}, Type: {data_type}")


        # Define the AA output_path depending on the data_id, and if not present create
        aa_output_path = os.path.join(aa_output_dir, data_id)
        os.makedirs(os.path.join(aa_output_path, data_id), exist_ok=True)

        # Define and the log_directory to store the logs, and if not present create
        log_output_path = os.path.join(log_base_dir, data_id)
        os.makedirs(os.path.join(log_output_path, data_id), exist_ok=True)

       # Step 3: Clean the directories for making the pipeline able to rerun on same data without any errors.
       # Loop for multiple directory
        for directory in [
            aa_output_path,
            log_output_path,
            os.path.join(pulseline_input_dir, data_id),
            os.path.join(pulseline_output_dir, data_id)
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

        # Step 4: Generate input data if needed, and input files to run the pipeline
        command = (
            f'ssh -X {common_node_id} "'
            f'echo Step 1: Connecting to node {common_node_id} && '
            f'source {environ_init_script} && echo Step 2: Environment sourced && '
            f'python3 {data_and_input_file_generator_path} {config_file_path} {scan_id} {data_id} {total_beams} {data_type} {input_file_generator_dir} {band_id} && '
            f'>> {log_output_path}/cpu_log_common_node_{common_node_id}.log 2>&1"'
        )

        # Print to verify correctness
        print(f"common_node_id = {common_node_id}")
        print(f"Running command: {command}")

        # Execute the command
        subprocess.run(command, shell=True, check=True)

        print(f"Generating data and input files done for all the filterbank files.")

        os.system("sleep 2")  # Sleeps for 2 seconds


        # Step 5: Load available GPU nodes
        try:
            avail_gpu_nodes = np.loadtxt(
                os.path.join(avail_gpus_file_dir, 'avail_gpu_nodes.txt'), dtype=str
            )
        except Exception as e:
            print(f"Error reading available GPU nodes: {e}")
            sys.exit(1)

        if not isinstance(avail_gpu_nodes, (list, np.ndarray)) or len(avail_gpu_nodes) == 0:
            print("No available GPU nodes found in the file.")
            sys.exit(1)

        # Step 6: Construct the SSH commands for RFI mitigation, GPU processing and same node CPU processing for search, shift, fold, etc.

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

        
        RFI_start = time.time() 
        # Step 7: Process each node concurrently for the command template 0 for runnign FILTOOL based RFI mitigation
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

        RFI_stop = time.time()
 
        AA_start = time.time()
        # Step 8: Process each node concurrently for the command template 1 for running AA pulsar search
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
        AA_stop = time.time()

        Stage1_start = time.time()
        # Step 9: Process each node concurrently for the command template 2 for running first stage sifting in a parallel manner
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
        Stage1_stop = time.time()

        Stage2_start = time.time()
        # Step 10: Process all the search level sifted candidates output at once in common GPU node to do a beam level candidate filtration
        if beam_level_sifting == 1:
            command = (
                f'ssh -X {common_node_id} "'
                f'echo Step 1: Connecting to {common_node_id} && '
                f'source {environ_init_script} && echo Step 2: Environment Loaded && '
                f'which python3 && echo Step 3: Python Located && '
                f'ls -l {final_stage_sifting_path} && echo Step 4: Checking Script Permissions && '
                f'python3 {final_stage_sifting_path} {scan_id} {data_id} {data_type} && echo Step 5: Python Script Executed '
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
        Stage2_stop = time.time()

        Folding_start = time.time()
        # Step 11: Process each node concurrently for the command template 3 for multinode CPU folding
        processes = []
        for node_alias in avail_gpu_nodes:
            p = Process(
                target=multi_node_folding,
                args=(
                    node_alias, data_id, scan_id, log_output_path, gpu_0_start_delay, gpu_1_start_delay, file_processing_delay, command_template_3)
            )
            processes.append(p)
            p.start()

        for p in processes:
            p.join()

        print(f"Folding completed for all the filtered candidates.")

        os.system("sleep 2")  # Sleeps for 2 seconds
        Folding_stop = time.time()

        Classification_start = time.time()
        # Step 12: Process all the folded candidate output AR files for classification
        if do_classify == 1:
            if not candidate_classifier_path:
                print("Error: Missing required path for candidate classification.")
                sys.exit(1)

            command = (
                '{python2_exe} {script} {data_id} >> {logfile} 2>&1'
            ).format(
                python2_exe=python2_exe_sh_path,
                script=candidate_classifier_path,
                data_id=data_id,
                logfile=log_output_path + "/classification_local_test.log"
            )


            # Print the command for debugging
            print(f"\nRunning SSH Command:\n{command}\n")

            # Execute the command
            try:
                subprocess.run(command, shell=True, check=True)
                print("Classification completed for all the folded candidates.")
            except subprocess.CalledProcessError as e:
                print(f"Error: SSH command failed with return code {e.returncode}")
                print(f"Check log file: {log_output_path}/classification_cpu_log_common_node_{common_node_id}.log")

        else:
            print("You chose not to run the classifier to classify the candidates.")

        os.system("sleep 2")  # Sleeps for 2 seconds
        Classification_stop = time.time()

        Summary_start = time.time()  
        # Step 13: Generate the final outputs and summary plots for sorted candidates
        if not final_outputs_script_path:
            print("Error: Missing required path for final outputs generation.")
            sys.exit(1)

        if not summary_plot_code_path:
            print("Error: Missing required path for summary plot generation.")
            sys.exit(1)

        # Construct SSH command with additional debug checks
        command = (
            f'ssh -X {common_node_id} "'
            f'echo Step 1: Connecting to {common_node_id} && '
            f'source {environ_init_script} && echo Step 2: Environment Loaded && '
            f'which python3 && echo Step 3: Python Located && '
            f'ls -l {final_outputs_script_path} && echo Step 4a: Final Outputs Script Found && '
            f'python3 {final_outputs_script_path} {scan_id} {data_id} {band_id} && echo Step 4b: Final Outputs Generated && '
            f'ls -l {summary_plot_code_path} && echo Step 5a: Summary Plot Script Found && '
            f'python3 {summary_plot_code_path} {data_id} && echo Step 5b: Summary Plot Generated '
            f'>> {log_output_path}/final_output_cpu_log_common_node_{common_node_id}.log 2>&1"'
        )

        # Print the command for debugging
        print(f"\nRunning SSH Command:\n{command}\n")

        # Execute the command
        try:
            subprocess.run(command, shell=True, check=True)
            print("Final outputs and summary plots generated successfully.")
        except subprocess.CalledProcessError as e:
            print(f"Error: SSH command failed with return code {e.returncode}")
            print(f"Check log file: {log_output_path}/final_output_cpu_log_common_node_{common_node_id}.log")


        #Track the executed targets
        executed_targets.append(data_id)

        # Update report with latest executed scan info
        with open(report_path, "a") as log_file:
            log_file.write(f"Executed scan: {data_id}\n")
        
        Summary_end = time.time()
        end_scan = time.time()

        Summary = Summary_end - Summary_start
        Folding = Folding_stop - Folding_start
        Classification = Classification_stop - Classification_start
        Stage1 = Stage1_stop - Stage1_start
        Stage2 = Stage2_stop - Stage2_start
        AA = AA_stop - AA_start
        total_scan = end_scan - start_scan

        with open("timing_log.txt", "a") as f:
            f.write(f"Ver 1 Timing {total_scan:.4f} for {data_id}\n")
            f.write(f"Ver 1 AA Timing {AA:.4f}\n")
            f.write(f"Ver 1 Stage 1 Sifting Timing {Stage1:.4f}\n")
            f.write(f"Ver 1 Stage 2 Sifting Timing {Stage2:.4f}\n")
            f.write(f"Ver 1 Folding Timing {Folding:.4f}\n")
            f.write(f"Ver 1 Classification Timing {Classification:.4f}\n")
            f.write(f"Ver 1 Summary Plots Generation Timing {Summary:.4f}\n")
        

        # Step 14: Clean the directories again to avoid unnecessary storage consumption.
        # ---- NEW: Filterbank files directory ----
        fil_data_dir = os.path.join(raw_input_base_dir, scan_id, "FilData", data_id)

        # Only clean the FIL files in the FilData directory
        if os.path.exists(fil_data_dir):
            for filename in os.listdir(fil_data_dir):
                if filename.endswith(".fil"):
                    file_path = os.path.join(fil_data_dir, filename)
                    os.remove(file_path)
                    print(f"Removed: {file_path}")
        else:
            print(f"Directory not found: {fil_data_dir}")

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
