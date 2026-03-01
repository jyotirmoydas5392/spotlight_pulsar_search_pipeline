#!/usr/bin/env python3

import os
import numpy as np
from multiprocessing import Pool

# ==================================================
# HARD-CODED CONFIGURATION (as provided)
# ==================================================

NODE_ALIAS = 'rggpu42'
GPU_ID = 1
WORKERS = 20

CANDIDATE_DIR = "/lustre_data/spotlight/data/PULSELINE_OUTPUT_DATA/J0738-4042_20251212_043812"
FIL_DIR = "/lustre_data/spotlight/data/TST3176_20251212_041455_NEVER_DELETE/FilData/J0738-4042_20251212_043812"
TEMPLATE_FILE = "/lustre_data/spotlight/data/pulsar_search_dev_run_files/pulsarx_run_files/input_files/meerkat_fold.template"
OUTPUT_DIR = "/lustre_data/spotlight/data/PULSELINE_OUTPUT_DATA/J0738-4042_20251212_043812/folded_outputs"
BASE_DIR = "/lustre_data/spotlight/data"

# ==================================================
# Utility functions
# ==================================================

def run_command(cmd):
    """Execute a shell command."""
    try:
        os.system(cmd)
    except Exception as e:
        print(f"[ERROR] Command failed: {e}")


def validate_pulsarx_candidate_file(candidate_file):
    """Check if PulsarX candidate file has valid numeric content."""
    if not os.path.exists(candidate_file):
        print(f"[WARNING] Candidate file not found: {candidate_file}")
        return False

    try:
        with open(candidate_file, "r") as f:
            header = f.readline()
            if not header:
                print(f"[WARNING] Empty candidate file: {candidate_file}")
                return False

            data_lines = [l for l in f if l.strip()]

        if not data_lines:
            print(f"[WARNING] No candidate entries in: {candidate_file}")
            return False

        data = np.atleast_2d(np.loadtxt(candidate_file, skiprows=1))
        if data.shape[0] == 0:
            print(f"[WARNING] No numeric data in: {candidate_file}")
            return False

        print(f"[INFO] Valid candidate file with {data.shape[0]} candidates")
        return True

    except Exception as e:
        print(f"[ERROR] Failed to read {candidate_file}: {e}")
        return False


# ==================================================
# Main PulsarX folding logic
# ==================================================

def pulsarx_folding():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    cand_suffix = f"node_{NODE_ALIAS}_gpu_{GPU_ID}_pulsarx_folding_all_candidates.txt"

    candidate_files = [
        os.path.join(CANDIDATE_DIR, f)
        for f in os.listdir(CANDIDATE_DIR)
        if f.endswith(cand_suffix)
    ]

    if not candidate_files:
        print("[ERROR] No PulsarX candidate files found.")
        return

    commands = []

    for cand_file in candidate_files:
        print(f"\n[INFO] Checking candidate file: {cand_file}")

        if not validate_pulsarx_candidate_file(cand_file):
            print("[INFO] Skipping invalid candidate file.")
            continue

        base_name = os.path.basename(cand_file).replace(
            f"_node_{NODE_ALIAS}_gpu_{GPU_ID}_pulsarx_folding_all_candidates.txt", ""
        )

        fil_file = os.path.join(FIL_DIR, f"{base_name}.fil")
        out_prefix = os.path.join(OUTPUT_DIR, base_name)

        if not os.path.exists(fil_file):
            print(f"[WARNING] FIL file not found: {fil_file}")
            continue

        docker_template = f"/data/{os.path.relpath(TEMPLATE_FILE, BASE_DIR)}"
        docker_candfile = f"/data/{os.path.relpath(cand_file, BASE_DIR)}"
        docker_filfile  = f"/data/{os.path.relpath(fil_file, BASE_DIR)}"
        docker_outfile  = f"/data/{os.path.relpath(out_prefix, BASE_DIR)}"

        docker_cmd = (
            "docker run --rm "
            f"-u {os.getuid()}:{os.getgid()} "
            "-v /lustre_archive/spotlight/raghav/PulsarX:/pulsarx "
            "-v /lustre_data/spotlight/data:/data "
            "ypmen/pulsarx "
            "sh -c "
            f"\"psrfold_fil -v "
            f"--template {docker_template} "
            f"--candfile {docker_candfile} "
            f"--plotx -n 64 -b 64 --clfd 2 "
            f"-f {docker_filfile} "
            f"-o {docker_outfile}\""
        )

        print("[DEBUG] Docker command:")
        print(docker_cmd)

        commands.append(docker_cmd)

    if not commands:
        print("[ERROR] No valid folding commands generated.")
        return

    print(f"\n[INFO] Executing {len(commands)} PulsarX folding jobs with {WORKERS} workers")

    with Pool(WORKERS) as pool:
        pool.map(run_command, commands)

    print("\n[INFO] PulsarX folding completed successfully.")


# ==================================================
# Entry point
# ==================================================

if __name__ == "__main__":
    pulsarx_folding()