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

def validate_pulsarx_candidate_file(candidate_file):

    if not os.path.exists(candidate_file):
        print(f"[WARNING] Candidate file not found: {candidate_file}")
        return False

    try:
        with open(candidate_file, "r") as f:
            header = f.readline()
            if not header:
                print(f"[WARNING] Candidate file is empty: {candidate_file}")
                return False

            data_lines = [line.strip() for line in f if line.strip()]

        if not data_lines:
            print(f"[WARNING] No candidate data found in file: {candidate_file}")
            return False

        data = np.loadtxt(candidate_file, dtype=float, skiprows=1)

        if data.size == 0:
            print(f"[WARNING] Candidate file contains no numeric data: {candidate_file}")
            return False

        print(f"[INFO] Loaded {data.shape[0] if data.ndim > 1 else 1} candidates from {candidate_file}")
        return True

    except ValueError as e:
        print(f"[ERROR] Numeric parsing failed for {candidate_file}: {e}")
        return False

    except Exception as e:
        print(f"[ERROR] Unexpected error reading candidate file {candidate_file}: {e}")
        return False


def read_presto_candidate_file(candidate_file):
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

def pulsarx_folding_from_file(node_alias, gpu_id, input_file_dir, template_file_path, input_dir, output_dir, workers):
    """
    Reads candidate data and performs folding (only using .fil files) in parallel.
    - node_alias: Node identifier.
    - gpu_id: GPU identifier.
    - input_file_dir: Directory where candidate files and .fil files are stored.
    - output_dir: Directory to store the folding outputs.
    - workers: Number of parallel workers for folding.
    """

    files_to_process = [
        f for f in os.listdir(input_file_dir)
        if f.endswith(f"node_{node_alias}_gpu_{gpu_id}_pulsarx_folding_all_candidates.txt")
    ]

    valid_files_to_process = []

    for file_name in files_to_process:
        candidate_file = os.path.join(input_file_dir, file_name)
        print(f"\nProcessing candidate file: {candidate_file}")
        valid_data = validate_pulsarx_candidate_file(candidate_file)
        if valid_data:
            valid_files_to_process.append(candidate_file)
        else:
            print(f"No valid candidates found in file: {candidate_file}")

    if not valid_files_to_process:
        print("No valid candidate files to process.")
        return

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    fil_commands = []

    base_dir = "/lustre_data/spotlight/data"

    for file in valid_files_to_process:

        # --------------------------------------------------
        # Derive base name
        # --------------------------------------------------
        fil_base_name = os.path.basename(file).replace(
            f"_node_{node_alias}_gpu_{gpu_id}_pulsarx_folding_all_candidates.txt", ""
        )

        fil_file_path = os.path.join(input_dir, f"{fil_base_name}.fil")

        # --------------------------------------------------
        # Docker paths (absolute inside container)
        # --------------------------------------------------
        docker_template = f"/data/{os.path.relpath(template_file_path, base_dir)}"
        docker_candfile = f"/data/{os.path.relpath(file, base_dir)}"
        docker_filfile  = f"/data/{os.path.relpath(fil_file_path, base_dir)}"

        # Docker working directory = output_dir
        docker_workdir = f"/data/{os.path.relpath(output_dir, base_dir)}"

        # Output name ONLY base name
        docker_outfile = fil_base_name

        # --------------------------------------------------
        # Debug prints
        # --------------------------------------------------
        print("[DEBUG] Docker paths:")
        print(" template :", docker_template)
        print(" candfile :", docker_candfile)
        print(" filfile  :", docker_filfile)
        print(" workdir  :", docker_workdir)
        print(" outfile  :", docker_outfile)

        # --------------------------------------------------
        # Docker command
        # --------------------------------------------------
        docker_cmd = (
            "docker run --rm "
            f"-u {os.getuid()}:{os.getgid()} "
            "-v /lustre_archive/spotlight/raghav/PulsarX:/pulsarx "
            "-v /lustre_data/spotlight/data:/data "
            f"-w {docker_workdir} "
            "ypmen/pulsarx "
            "sh -c "
            "\"psrfold_fil -v "
            f"--template {docker_template} "
            f"--candfile {docker_candfile} "
            "--plotx -n 64 -b 64 --clfd 2 "
            f"-f {docker_filfile} "
            f"-o {docker_outfile}\""
        )

        fil_commands.append(docker_cmd)


    # --------------------------------------------------
    # Execute folding commands (NO os.chdir needed)
    # --------------------------------------------------
    def execute(commands):
        print(f"Folding started in Docker workdir: {output_dir}")
        with Pool(workers) as pool:
            pool.map(folding, commands)


    print(f"Executing {len(fil_commands)} FIL folding commands.")
    execute(fil_commands)
    print("Folding operation completed.")


def presto_folding_from_file(node_alias, gpu_id, input_file_dir, output_dir, workers):
    """
    Reads candidate data and performs folding (only using .fil files) in parallel.
    - node_alias: Node identifier.
    - gpu_id: GPU identifier.
    - input_file_dir: Directory where candidate files and .fil files are stored.
    - output_dir: Directory to store the folding outputs.
    - workers: Number of parallel workers for folding.
    """

    candidate_file = os.path.join(
        input_file_dir,
        f"node_{node_alias}_gpu_{gpu_id}_presto_folding_all_candidates.txt"
    )

    print(f"\nProcessing candidate file: {candidate_file}")
    candidate_data, valid_data = read_presto_candidate_file(candidate_file)
    if not valid_data:
        print("No valid candidates found.")
        return

    # Create output directory if it doesn't exist
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
            f"-noxwin -topo -nosearch "
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
