import os
import sys
import logging
import re

# =================== USER CONTROL =====================
DRY_RUN = False     # Set to False when you really want deletion
# ======================================================

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]: %(message)s")

# Get base directory
base_dir = os.getenv("PULSELINE_DEV_DIR")
if not base_dir:
    logging.error("Error: PULSELINE_DEV_DIR environment variable is not set.")
    sys.exit(1)

# Import load_parameters function
sys.path.insert(0, os.path.join(base_dir, "input_file_dir_init/scripts"))
try:
    from read_input_file_dir import load_parameters
except ImportError:
    logging.error("Error importing required modules.")
    sys.exit(1)

# Configuration file path
config_file_path = os.path.join(base_dir, "input_file_dir_init/input_dir/input_file_directory.txt")


def read_obs_ids(obs_file):
    """Read observation IDs from obs_ids.txt"""
    if not os.path.exists(obs_file):
        logging.error(f"Observation ID file not found: {obs_file}")
        sys.exit(1)

    with open(obs_file, "r") as f:
        obs_ids = [line.strip() for line in f if line.strip()]

    return obs_ids


def get_scans_from_beamdata(beam_dir):
    """
    Extract scan IDs from filenames like:
    J0542+498_20260109_221356.raw.0
    """

    if not os.path.exists(beam_dir):
        return []

    pattern = re.compile(r"^(.*?)\.raw\.\d+$")

    scans = set()

    for f in os.listdir(beam_dir):
        match = pattern.match(f)
        if match:
            scans.add(match.group(1))

    return list(scans)


def count_raw_files_for_scan(beam_dir, scan_name):
    """Count files exactly like <scan>.raw.<number>"""
    if not os.path.exists(beam_dir):
        return 0

    pattern = re.compile(rf"^{re.escape(scan_name)}\.raw\.\d+$")

    return len([f for f in os.listdir(beam_dir) if pattern.match(f)])


def count_ahdr_files_for_scan(beam_dir, scan_name):
    """Count files exactly like <scan>.raw.<number>.ahdr"""
    if not os.path.exists(beam_dir):
        return 0

    pattern = re.compile(rf"^{re.escape(scan_name)}\.raw\.\d+\.ahdr$")

    return len([f for f in os.listdir(beam_dir) if pattern.match(f)])


def count_fil_files(scan_dir):
    """Count .fil files in a FilData scan directory"""
    if not os.path.exists(scan_dir):
        return 0

    return len([f for f in os.listdir(scan_dir) if f.endswith(".fil")])


def get_scan_names_from_fildata(fil_base):
    """Get scan names from FilData subdirectories"""
    if not os.path.exists(fil_base):
        return []

    return [
        d for d in os.listdir(fil_base)
        if os.path.isdir(os.path.join(fil_base, d))
    ]


def check_pulsar_pipe(pipe_base, scan_name):
    """Check if PulsarPipeData contains subdirectory for this scan"""
    scan_dir = os.path.join(pipe_base, scan_name)
    return os.path.exists(scan_dir) and os.path.isdir(scan_dir)


def delete_beam_raw_files(beam_dir, scan_name):
    """
    Delete ONLY files of exact form:
        <scan>.raw.<number>
    """

    if not os.path.exists(beam_dir):
        return

    pattern = re.compile(rf"^{re.escape(scan_name)}\.raw\.\d+$")

    for fname in os.listdir(beam_dir):

        if not pattern.match(fname):
            continue

        file_path = os.path.join(beam_dir, fname)

        if DRY_RUN:
            logging.info(f"[DRY RUN] Would delete: {file_path}")
        else:
            try:
                os.remove(file_path)
                logging.info(f"Deleted: {file_path}")
            except Exception as e:
                logging.error(f"Failed to delete {file_path}: {e}")


def main():
    try:
        params = load_parameters(config_file_path)
        logging.info("Loaded parameters from configuration file.")
    except Exception as e:
        logging.error(f"Error loading parameters: {e}")
        sys.exit(1)

    raw_input_base_dir = params.get("raw_input_base_dir")

    if not raw_input_base_dir:
        logging.error("raw_input_base_dir not found in config file.")
        sys.exit(1)

    obs_file = os.path.join(base_dir, "obs_ids.txt")
    obs_ids = read_obs_ids(obs_file)

    logging.info(f"Found {len(obs_ids)} observation IDs")

    status_file = os.path.join(base_dir, "obs_per_scan_data_status.txt")

    with open(status_file, "w") as out:

        header = "ObsID,ActualDirectory,ScanName,BeamData_Before,BeamData_After,AHDR_Count,FilData,Pulsar_search_status\n"
        out.write(header)

        available_dirs = [
            d for d in os.listdir(raw_input_base_dir)
            if os.path.isdir(os.path.join(raw_input_base_dir, d))
        ]

        for obs in obs_ids:

            matching_dirs = [
                d for d in available_dirs if d.startswith(obs)
            ]

            if not matching_dirs:
                logging.warning(f"No matching directories found for obs id: {obs}")
                out.write(f"{obs},NA,NA,False,False,False,False,False\n")
                continue

            for actual_dir in matching_dirs:

                obs_dir = os.path.join(raw_input_base_dir, actual_dir)

                beam_base = os.path.join(obs_dir, "BeamData")
                fil_base = os.path.join(obs_dir, "FilData")
                pipe_base = os.path.join(obs_dir, "PulsarPipeData")

                scans_from_beam = get_scans_from_beamdata(beam_base)
                scans_from_fil = get_scan_names_from_fildata(fil_base)

                all_scans = sorted(set(scans_from_beam + scans_from_fil))

                if not all_scans:
                    logging.warning(f"No scans found for {actual_dir}")
                    out.write(f"{obs},{actual_dir},NO_SCANS,False,False,False,False,False\n")
                    continue

                for scan in all_scans:

                    raw_before = count_raw_files_for_scan(beam_base, scan)
                    fil_count = count_fil_files(os.path.join(fil_base, scan))
                    ahdr_count = count_ahdr_files_for_scan(beam_base, scan)

                    pulsar_status = check_pulsar_pipe(pipe_base, scan)

                    if raw_before > 0 and fil_count > 0:
                        logging.info(
                            f"{obs} | {actual_dir} | {scan} : "
                            "Both Beam and Fil present -> removing beam raw files"
                        )
                        delete_beam_raw_files(beam_base, scan)

                    raw_after = count_raw_files_for_scan(beam_base, scan)

                    # Apply ZERO -> False rule
                    beam_before_status = raw_before if raw_before > 0 else "False"
                    beam_after_status = raw_after if raw_after > 0 else "False"

                    ahdr_status = ahdr_count if ahdr_count > 0 else "False"
                    fil_status = fil_count if fil_count > 0 else "False"

                    line = f"{obs},{actual_dir},{scan},{beam_before_status},{beam_after_status},{ahdr_status},{fil_status},{pulsar_status}\n"
                    out.write(line)

    logging.info(f"Per-scan status file generated: {status_file}")


if __name__ == "__main__":
    main()
