import os
import logging
import shutil
import sys
import re

# ================= USER CONTROL =================
DRY_RUN = False    # Set to False to actually delete files
# ================================================

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]: %(message)s")

# Load base development directory
base_dir = os.getenv("PULSELINE_VER1_DIR")
if not base_dir:
    logging.error("PULSELINE_VER1_DIR not set")
    sys.exit(1)

# Load configuration to get raw_input_base_dir
sys.path.insert(0, os.path.join(base_dir, "input_file_dir_init/scripts"))
from read_input_file_dir import load_parameters

config_file_path = os.path.join(base_dir, "input_file_dir_init/input_dir/input_file_directory.txt")

params = load_parameters(config_file_path)

raw_input_base_dir = params.get("raw_input_base_dir")

if not raw_input_base_dir:
    logging.error("raw_input_base_dir not found in configuration file")
    sys.exit(1)

# Input and output files
status_file = os.path.join(base_dir, "obs_per_scan_data_status.txt")
output_file = os.path.join(base_dir, "GTAC_obs_details", "obs_details.txt")

os.makedirs(os.path.dirname(output_file), exist_ok=True)


def delete_fil_files(scan_dir):

    if not os.path.exists(scan_dir):
        return

    deleted_any = False

    for f in os.listdir(scan_dir):

        if f.endswith(".fil"):

            file_path = os.path.join(scan_dir, f)

            if DRY_RUN:
                logging.info(f"[DRY RUN] Would delete FIL file: {file_path}")
            else:
                try:
                    os.remove(file_path)
                    logging.info(f"Deleted FIL file: {file_path}")
                except Exception as e:
                    logging.error(f"Failed to delete {file_path}: {e}")

            deleted_any = True

    if not deleted_any:
        logging.info(f"No .fil files found in {scan_dir}")


def delete_raw_files(beam_dir, scan_name):

    pattern = re.compile(rf"^{re.escape(scan_name)}\.raw\.\d+$")

    if not os.path.exists(beam_dir):
        return

    for f in os.listdir(beam_dir):
        if pattern.match(f):

            file_path = os.path.join(beam_dir, f)

            if DRY_RUN:
                logging.info(f"[DRY RUN] Would delete RAW file: {file_path}")
            else:
                try:
                    os.remove(file_path)
                    logging.info(f"Deleted RAW file: {file_path}")
                except Exception as e:
                    logging.error(f"Failed to delete {file_path}: {e}")


def get_first_ahdr_file(beam_dir, scan_name):

    if not os.path.exists(beam_dir):
        return None

    pattern = re.compile(rf"^{re.escape(scan_name)}\.raw\.\d+\.ahdr$")

    for f in os.listdir(beam_dir):
        if pattern.match(f):
            return os.path.join(beam_dir, f)

    return None


def parse_beams_per_host(ahdr_file):

    try:
        with open(ahdr_file, "r") as f:
            for line in f:
                if "Total No. of Beams/host" in line:
                    return int(line.split("=")[-1].strip())
    except:
        pass

    return None


def parse_band_from_ahdr(ahdr_file):

    freq_hz = None

    try:
        with open(ahdr_file, "r") as f:
            for line in f:
                if "Frequency Ch. 0" in line:
                    freq_hz = float(line.split("=")[-1].strip())
                    break
    except:
        return None

    if freq_hz is None:
        return None

    freq_mhz = freq_hz / 1e6

    if 290 <= freq_mhz <= 510:
        return "BAND3"
    elif 540 <= freq_mhz <= 960:
        return "BAND4"
    elif 1000 <= freq_mhz <= 1500:
        return "BAND5"
    else:
        return "UNKNOWN"


def main():

    if not os.path.exists(status_file):
        logging.error(f"Status file not found: {status_file}")
        sys.exit(1)

    with open(status_file, "r") as f:
        lines = f.readlines()

    header = lines[0].strip().split(",")

    required_cols = [
        "ObsID",
        "ActualDirectory",
        "ScanName",
        "AHDR_Count",
        "FilData",
        "BeamData_Before",
        "Pulsar_search_status"
    ]

    for col in required_cols:
        if col not in header:
            logging.error(f"Required column {col} missing in status file")
            sys.exit(1)

    idx_obs = header.index("ObsID")
    idx_dir = header.index("ActualDirectory")
    idx_scan = header.index("ScanName")
    idx_ahdr = header.index("AHDR_Count")
    idx_fil = header.index("FilData")
    idx_raw = header.index("BeamData_Before")
    idx_psr = header.index("Pulsar_search_status")

    out = open(output_file, "w")

    out.write("# GTAC_scan    Target    Total_beams    uGMRT_band    Input_file (0 for RAW, 1 for FIL)\n")

    for line in lines[1:]:

        parts = line.strip().split(",")

        obs = parts[idx_obs]
        actual_dir = parts[idx_dir]
        scan = parts[idx_scan]

        ahdr = parts[idx_ahdr]
        fil = parts[idx_fil]
        raw = parts[idx_raw]
        psr_status = parts[idx_psr]

        # ===== FINAL DIRECTORY STRUCTURE =====
        beam_dir = os.path.join(raw_input_base_dir, actual_dir, "BeamData")
        fildata_scan_dir = os.path.join(raw_input_base_dir, actual_dir, "FilData", scan)

        if not os.path.exists(beam_dir):
            logging.info(f"{obs} | {scan} : BeamData directory missing -> deleting any FIL")
            delete_fil_files(fildata_scan_dir)
            continue

        if psr_status == "True":
            logging.info(f"{obs} | {scan} : Pulsar search completed -> deleting RAW and FIL")
            delete_fil_files(fildata_scan_dir)
            delete_raw_files(beam_dir, scan)
            continue

        if ahdr == "False":
            logging.info(f"{obs} | {scan} : AHDR missing -> deleting any RAW or FIL")
            delete_fil_files(fildata_scan_dir)
            delete_raw_files(beam_dir, scan)
            continue

        if raw == "False" and fil == "False":
            logging.info(f"{obs} | {scan} : Only AHDR present -> nothing to process")
            continue

        first_ahdr = get_first_ahdr_file(beam_dir, scan)

        if not first_ahdr:
            logging.warning(f"{obs} | {scan} : AHDR marked true but file not found")
            continue

        beams_per_host = parse_beams_per_host(first_ahdr)
        band = parse_band_from_ahdr(first_ahdr)

        if beams_per_host is None or band is None:
            logging.warning(f"{obs} | {scan} : Could not parse AHDR information")
            continue

        ahdr_count = int(ahdr)
        real_total_beams = beams_per_host * ahdr_count

        if raw != "False" and fil == "False":
            config_line = f"{actual_dir}    {scan}    {real_total_beams}    {band}    0\n"
            out.write(config_line)
            continue

        if raw != "False" and fil != "False":
            logging.info(f"{obs} | {scan} : FIL already exists -> deleting RAW")
            delete_raw_files(beam_dir, scan)

            config_line = f"{actual_dir}    {scan}    {real_total_beams}    {band}    1\n"
            out.write(config_line)
            continue

        if raw == "False" and fil != "False":
            config_line = f"{actual_dir}    {scan}    {real_total_beams}    {band}    1\n"
            out.write(config_line)

    out.close()

    logging.info(f"Processing configuration written to: {output_file}")


if __name__ == "__main__":
    main()