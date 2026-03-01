import os
import sys
import logging
import numpy as np

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]: %(message)s")

# Get the base directory from the environment variable
base_dir = os.getenv("PULSELINE_DEV_DIR")
if not base_dir:
    logging.error("PULSELINE_DEV_DIR environment variable is not set.")
    sys.exit(1)

# List of relative paths to add dynamically
relative_paths = [
    "input_file_dir_init/scripts"
]

# Loop through and add each path to sys.path
for relative_path in relative_paths:
    full_path = os.path.join(base_dir, relative_path)
    sys.path.insert(0, full_path)

# Import necessary functions
try:
    from read_input_file_dir import load_parameters
except ImportError as e:
    logging.error("Error importing required modules. Ensure the scripts exist in the specified paths.")
    logging.error(e)
    sys.exit(1)


def candidates(input_dir, file_name, harmonic_opt_flag, beam_sort_flag):
    """
    Load candidates file based on flags. Return reversed candidate array and a status flag.
    """
    if harmonic_opt_flag == 0.0 and beam_sort_flag == 0.0:
        candidate_file = f"{file_name}_all_sifted_candidates.txt"
    elif harmonic_opt_flag == 0.0 and beam_sort_flag == 1.0:
        candidate_file = f"{file_name}_all_sifted_beam_sorted_candidates.txt"
    elif harmonic_opt_flag == 1.0 and beam_sort_flag == 0.0:
        candidate_file = f"{file_name}_all_sifted_harmonic_removed_candidates.txt"
    elif harmonic_opt_flag == 1.0 and beam_sort_flag == 1.0:
        candidate_file = f"{file_name}_all_sifted_harmonic_removed_beam_sorted_candidates.txt"
    else:
        print("Invalid combination of harmonic_opt_flag and beam_sort_flag.")
        return None, False

    file_path = os.path.join(input_dir, candidate_file)

    # Check if file exists and is not empty
    if not os.path.exists(file_path):
        print(f"Error: File '{file_path}' does not exist.")
        return None, False

    if os.stat(file_path).st_size == 0:
        print(f"No candidates to process: {file_path}")
        return None, False

    with open(file_path, "r") as file:
        first_line = file.readline().strip()

    if first_line == "No valid data found to process.":
        print(f"Error message in file: {file_path}")
        return None, False

    try:
        data = np.loadtxt(file_path, dtype=float, skiprows=1)
        if data.size == 0:
            print(f"No valid candidate rows in file: {file_path}")
            return None, False

        if data.ndim == 1:
            data = data.reshape(1, -1)

        return data[::-1], True
    except Exception as e:
        print(f"Error loading candidate file {candidate_file}: {e}")
        return None, False

def generate_pulsarx_folding_candidates_per_beam(
    input_dir, output_dir, aa_input_file_dir, pulseline_input_file_dir,
    node_alias, gpu_id):
    """
    Process files for specific files for each node and GPU. Aggregate all valid candidates and
    write to one output file with metadata (fil_file_path as last column) for PULSARX folding.
    """
    files_to_process = [
        f for f in os.listdir(aa_input_file_dir)
        if f.startswith("AA") and f.endswith(f"node_{node_alias}_gpu_id_{gpu_id}.txt")
    ]

    if not files_to_process:
        logging.info(f"No files for node {node_alias}, GPU {gpu_id}. Skipping...")
        return

    logging.info(f"Files to process for node {node_alias}, GPU {gpu_id}: {files_to_process}")
    os.makedirs(output_dir, exist_ok=True)

    PULSELINE = "PULSELINE"

    for file in sorted(files_to_process):
        pulseline_file_name = f"{PULSELINE}_{file.replace('AA_', '').strip()}"
        pulseline_file_path = os.path.join(pulseline_input_file_dir, pulseline_file_name)
        logging.info(f"Pulseline file path: {pulseline_file_path}")

        if not os.path.exists(pulseline_file_path):
            logging.error(f"Pulseline file not found: {pulseline_file_path}")
            continue

        try:
            params = load_parameters(pulseline_file_path)
        except Exception as e:
            logging.error(f"Error loading parameters from pulseline file: {e}")
            continue

        fil_file_path = params.get('fil_file')
        if not fil_file_path:
            logging.error("Missing 'fil_file' in configuration.")
            continue

        fil_file = os.path.basename(fil_file_path)
        file_name = fil_file.replace(".fil", "")
        logging.info(f"Using fil base name: {file_name}")

        # Prepare the final candidate file path
        candidate_output_file = os.path.join(
            output_dir,
            f"{file_name}_node_{node_alias}_gpu_{gpu_id}_pulsarx_folding_all_candidates.txt"
        )

        # Initialize output file with proper header
        with open(candidate_output_file, "w") as f_out:
            f_out.write("#id     DM        acc        F0               F1            S/N\n")

        
        candidate_array, valid_data = candidates(input_dir, file_name, params.get("harmonic_opt_flag"), params.get("beam_sort_flag"))
        if not valid_data:
            logging.warning("No valid candidates for this input.")
            continue

        try:
            with open(candidate_output_file, "a") as f_out:
                i = 0
                for row in candidate_array:
                    i += 1

                    P = row[0]       # seconds
                    Pdot = row[1]    # s/s
                    DM = row[2]

                    F0 = 1.0 / P
                    F1 = -Pdot / (P * P)

                    acc = 0.0        # assuming zero acceleration
                    snr = 100.0      # placeholder

                    f_out.write(
                        f"{i:5d}  {DM:10.4f}  {acc:8.3f}  {F0:20.12e}  {F1:20.12e}  {snr:8.2f}\n"
                    )

            logging.info(f"Appended {candidate_array.shape[0]} candidates to {candidate_output_file}")
        except Exception as e:
            logging.error(f"Error writing candidates to {candidate_output_file}: {e}")


def generate_presto_folding_candidates_per_node(
    input_dir, output_dir, aa_input_file_dir, pulseline_input_file_dir,
    node_alias, gpu_id):
    """
    Process files for a specific node and GPU. Aggregate all valid candidates and
    write to one output file with metadata (fil_file_path as last column) for PRESTO folding.
    """
    files_to_process = [
        f for f in os.listdir(aa_input_file_dir)
        if f.startswith("AA") and f.endswith(f"node_{node_alias}_gpu_id_{gpu_id}.txt")
    ]

    if not files_to_process:
        logging.info(f"No files for node {node_alias}, GPU {gpu_id}. Skipping...")
        return

    logging.info(f"Files to process for node {node_alias}, GPU {gpu_id}: {files_to_process}")
    os.makedirs(output_dir, exist_ok=True)

    PULSELINE = "PULSELINE"

    # Prepare the final candidate file path
    candidate_output_file = os.path.join(
        output_dir,
        f"node_{node_alias}_gpu_{gpu_id}_presto_folding_all_candidates.txt"
    )

    # Initialize output file with proper header
    with open(candidate_output_file, "w") as f_out:
        f_out.write("Period(sec)   Pdot(s/s)   DM(pc/cc)   Fil_File_Path\n")

    for file in sorted(files_to_process):
        pulseline_file_name = f"{PULSELINE}_{file.replace('AA_', '').strip()}"
        pulseline_file_path = os.path.join(pulseline_input_file_dir, pulseline_file_name)
        logging.info(f"Pulseline file path: {pulseline_file_path}")

        if not os.path.exists(pulseline_file_path):
            logging.error(f"Pulseline file not found: {pulseline_file_path}")
            continue

        try:
            params = load_parameters(pulseline_file_path)
        except Exception as e:
            logging.error(f"Error loading parameters from pulseline file: {e}")
            continue

        fil_file_path = params.get('fil_file')
        if not fil_file_path:
            logging.error("Missing 'fil_file' in configuration.")
            continue

        fil_file = os.path.basename(fil_file_path)
        file_name = fil_file.replace(".fil", "")
        logging.info(f"Using fil base name: {file_name}")

        candidate_array, valid_data = candidates(input_dir, file_name, params.get("harmonic_opt_flag"), params.get("beam_sort_flag"))
        if not valid_data:
            logging.warning("No valid candidates for this input.")
            continue

        try:
            with open(candidate_output_file, "a") as f_out:
                for row in candidate_array:
                    f_out.write(f"{row[0]} {row[1]} {row[2]} {fil_file_path}\n")
            logging.info(f"Appended {candidate_array.shape[0]} candidates to {candidate_output_file}")
        except Exception as e:
            logging.error(f"Error writing candidates to {candidate_output_file}: {e}")
