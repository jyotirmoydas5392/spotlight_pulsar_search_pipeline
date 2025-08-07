import os
import sys
import numpy as np
from multiprocessing import Pool

def folding(command):
    """Execute a folding system call."""
    try:
        os.system(command)
    except Exception as e:
        print(f"Error executing folding command: {e}")

def read_candidate_file(candidate_file):
    """
    Reads a node-GPU-specific candidate file.
    Returns: (list of tuples (period, pdot, dm, fil_file_path), bool)
    The bool is True if there is at least one valid candidate, False otherwise.
    """
    if not os.path.exists(candidate_file):
        print(f"Candidate file not found: {candidate_file}")
        return [], False

    try:
        with open(candidate_file, "r") as f:
            lines = f.readlines()

        data_lines = [line.strip() for line in lines[1:] if line.strip()]

        if not data_lines:
            print(f"No candidate data found in file: {candidate_file}")
            return [], False

        candidates = []
        for line in data_lines:
            try:
                parts = line.split()
                if len(parts) < 4:
                    raise ValueError("Incomplete candidate line")
                period = float(parts[0])
                pdot = float(parts[1])
                dm = float(parts[2])
                fil_file_path = parts[3]
                candidates.append((period, pdot, dm, fil_file_path))
            except Exception as e:
                print(f"Skipping line due to parse error: {line} ({e})")

        if not candidates:
            return [], False
        return candidates, True

    except Exception as e:
        print(f"Error reading candidate file {candidate_file}: {e}")
        return [], False


def folding_from_file(node_alias, gpu_id, input_dir, output_dir, workers):
    """
    Reads candidate data and performs folding (only using .fil files) in parallel.
    - node_alias: Node identifier.
    - gpu_id: GPU identifier.
    - input_dir: Directory where candidate files and .fil files are stored.
    - output_dir: Directory to store the folding outputs.
    - workers: Number of parallel workers for folding.
    """
    candidate_file = os.path.join(
        input_dir,
        f"node_{node_alias}_gpu_{gpu_id}_shifted_final_folding_all_candidates.txt"
    )

    print(f"\nProcessing candidate file: {candidate_file}")
    candidate_data, valid_data = read_candidate_file(candidate_file)
    if not valid_data:
        print("No valid candidates found.")
        return

    os.makedirs(output_dir, exist_ok=True)
    fil_commands = []

    for idx, (period, pdot, dm, fil_file_path) in enumerate(candidate_data):
        DM_value = f"{dm:.2f}"
        period_value = f"{period:.10f}"
        pdot_value = f"{pdot:.6f}"

        fil_base_name = os.path.basename(fil_file_path).replace(".fil", "")
        file_base_prefix = f"{fil_base_name}_node_{node_alias}_gpu{gpu_id}"
        base_out_name = f"{file_base_prefix}_DM{DM_value}_Serial_no_{idx}"

        fil_command = (
            f"prepfold -p {period_value} -pd {pdot_value} -dm {DM_value} "
            f"-noxwin -topo -nodmsearch -nopdsearch "
            f"-o {base_out_name}_FIL {fil_file_path}"
        )
        fil_commands.append(fil_command)

    def execute(commands):
        original_dir = os.getcwd()
        try:
            os.chdir(output_dir)
            print(f"Folding started in: {output_dir}")
            with Pool(workers) as pool:
                pool.map(folding, commands)
        finally:
            os.chdir(original_dir)
            print(f"Returned to original directory: {original_dir}")

    print(f"Executing {len(fil_commands)} FIL folding commands.")
    execute(fil_commands)
    print("Folding operation completed.")
