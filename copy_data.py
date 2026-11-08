import os
import re
import subprocess

# -----------------------------
# INPUT FILE
# -----------------------------
input_file = "/lustre_data/spotlight/data/pulsar_fft_pipe_ver1/infield_source_all_copied_files.txt"

# -----------------------------
# REMOTE SETTINGS
# -----------------------------
remote_user = "guest"
remote_host = "achilles.ncra.tifr.res.in"
remote_base = "newdata/LPT/sps"

# Base path to preserve structure from
local_base = "/lustre_data/spotlight/data"

# Toggle this first → set to False when ready
DRY_RUN = False 

# -----------------------------
# FUNCTION: identify pulsars
# -----------------------------
def is_pulsar(name):
    return re.match(r'^[JB]', name.strip()) is not None

# -----------------------------
# MAIN LOOP
# -----------------------------
with open(input_file, "r") as f:
    for line in f:
        parts = line.strip().split("|")

        if len(parts) < 2:
            continue

        # Extract .fil path (remove index number safely)
        fil_path = parts[0].strip().split()[-1]
        source_name = parts[1].strip()

        # -----------------------------
        # FILTER ONLY PULSARS
        # -----------------------------
        if not is_pulsar(source_name):
            continue

        # -----------------------------
        # BUILD candies_output PATH
        # -----------------------------
        base_dir = os.path.dirname(fil_path)
        candies_path = os.path.join(base_dir, "raw", "candies_output")

        if not os.path.exists(candies_path):
            print(f"[SKIP] Not found: {candies_path}")
            continue

        # -----------------------------
        # PRESERVE DIRECTORY STRUCTURE
        # -----------------------------
        relative_path = os.path.relpath(candies_path, local_base)
        remote_path = os.path.join(remote_base, relative_path)

        print(f"[INFO] {source_name}")
        print(f"       {candies_path} → {remote_path}")

        if DRY_RUN:
            continue

        # -----------------------------
        # CREATE REMOTE DIRECTORY
        # -----------------------------
        mkdir_cmd = f"ssh {remote_user}@{remote_host} 'mkdir -p {remote_path}'"
        subprocess.run(mkdir_cmd, shell=True)

        # -----------------------------
        # COPY USING RSYNC
        # -----------------------------
        rsync_cmd = (
            f"rsync -avz --progress "
            f"{candies_path}/ "
            f"{remote_user}@{remote_host}:{remote_path}/"
        )

        subprocess.run(rsync_cmd, shell=True)

print("\nDone.")
