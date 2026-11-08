import os
import sys
import glob
import shutil
import logging
import numpy as np
import re
from astropy.coordinates import SkyCoord, FK5
from astropy.time import Time
import astropy.units as u

# --------------------------------------------------
# Logging
# --------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]: %(message)s")

# --------------------------------------------------
# Base directory
# --------------------------------------------------
base_dir = os.getenv("PULSELINE_VER1_DIR")
if not base_dir:
    logging.error("PULSELINE_VER1_DIR not set")
    sys.exit(1)

# --------------------------------------------------
# Add module paths
# --------------------------------------------------
for rel in ["input_file_dir_init/scripts", "scripts"]:
    path = os.path.join(base_dir, rel)
    if os.path.exists(path):
        sys.path.insert(0, path)

from read_input_file_dir import load_parameters

# --------------------------------------------------
# Config
# --------------------------------------------------
params = load_parameters(os.path.join(base_dir, "input_file_dir_init/input_dir/input_file_directory.txt"))
max_beam_sep = float(params.get("max_beam_sep", 1.0))
logging.info(f"Using max_beam_sep = {max_beam_sep} arcmin")

# --------------------------------------------------
# Logs
# --------------------------------------------------
global_log_file = os.path.join(base_dir, "outfiles/infield_source_all_copied_files.txt")
global_status_file = os.path.join(base_dir, "outfiles/infield_source_run_status.txt")

# --------------------------------------------------
# Log cleanup
# --------------------------------------------------
def remove_old_status_entries(file_path, scan_id, data_id):
    if not os.path.exists(file_path):
        return
    with open(file_path) as f:
        lines = f.readlines()
    with open(file_path, "w") as f:
        for l in lines:
            if not l.startswith(f"{scan_id} {data_id} "):
                f.write(l)

def remove_old_file_entries(file_path, scan_id, data_id):
    if not os.path.exists(file_path):
        return
    with open(file_path) as f:
        lines = f.readlines()
    with open(file_path, "w") as f:
        for l in lines:
            if not (scan_id in l and data_id in l):
                f.write(l)

# --------------------------------------------------
# NEW: Ensure newline before append
# --------------------------------------------------
def ensure_newline(file_path):
    if os.path.exists(file_path):
        with open(file_path, "rb+") as f:
            f.seek(0, os.SEEK_END)
            if f.tell() > 0:
                f.seek(-1, os.SEEK_END)
                if f.read(1) != b"\n":
                    f.write(b"\n")

# --------------------------------------------------
# Safe datetime parsing
# --------------------------------------------------
def extract_datetime_from_dataid(data_id):
    parts = data_id.split('_')
    if len(parts) < 3:
        return None, None
    if not (parts[-2].isdigit() and parts[-1].isdigit()):
        return None, None
    return parts[-2], int(parts[-1])

# --------------------------------------------------
# Scan helpers
# --------------------------------------------------
def extract_datetime_from_scan(line):
    parts = line.split()
    first = parts[0].split('_')
    return first[-2], int(first[-1])

def find_all_scans_same_date(match_file, scan_id, data_id):
    target_date, _ = extract_datetime_from_dataid(data_id)
    if target_date is None:
        return []

    scan_lines = []

    with open(match_file) as f:
        for line in f:
            line = line.strip()
            if not line or "----" in line:
                continue
            if not line.startswith(scan_id):
                continue

            try:
                d, _ = extract_datetime_from_scan(line)
                if d == target_date:
                    scan_lines.append(line)
            except:
                continue

    return scan_lines

# --------------------------------------------------
# Source extraction
# --------------------------------------------------
def get_sources_for_scan(match_file, scan_line):

    sources = []

    with open(match_file) as f:
        lines = f.readlines()

    for i in range(len(lines)):
        if lines[i].strip() == scan_line.strip():

            i += 1
            while i < len(lines) and not lines[i].startswith("-"):
                i += 1

            i += 1

            while i < len(lines) and not lines[i].startswith("-"):
                line = lines[i].strip()

                if line and line != "NULL":
                    parts = [p.strip() for p in line.split(',')]
                    if len(parts) >= 3:
                        sources.append((parts[0], parts[1], parts[2]))

                i += 1
            break

    return list(dict.fromkeys(sources))

def get_all_sources(match_file, scan_lines):
    all_sources = []
    for sl in scan_lines:
        all_sources.extend(get_sources_for_scan(match_file, sl))
    return list(dict.fromkeys(all_sources))

# --------------------------------------------------
# Coordinate conversion
# --------------------------------------------------
def get_obs_time(data_id):
    date = data_id.split('_')[-2]
    date_iso = f"{date[:4]}-{date[4:6]}-{date[6:8]}"
    return Time(f"{date_iso} 00:00:00", format='iso', scale='utc')

def source_to_precessed_radian(ra, dec, obs_time):
    c = SkyCoord(ra, dec, unit=(u.hourangle, u.deg), frame='icrs')
    c = c.transform_to(FK5(equinox=obs_time))
    return c.ra.rad, c.dec.rad

# --------------------------------------------------
# Beam reading
# --------------------------------------------------
def read_ahdr_for_dataid(ahdr_dir, data_id):
    ahdr_files = glob.glob(os.path.join(ahdr_dir, "*.ahdr"))

    beams = []
    for fpath in ahdr_files:
        if not os.path.basename(fpath).startswith(data_id):
            continue

        with open(fpath) as f:
            reading = False
            for line in f:
                if "RA" in line and "DEC" in line:
                    reading = True
                    continue
                if reading:
                    parts = line.split()
                    if len(parts) >= 3:
                        try:
                            beams.append((int(parts[2]), float(parts[0]), float(parts[1])))
                        except:
                            continue

    return beams

# --------------------------------------------------
# Pipeline
# --------------------------------------------------
def run_pipeline(match_file, scan_id, data_id, ahdr_dir, fil_dir):

    date, _ = extract_datetime_from_dataid(data_id)
    if date is None:
        logging.warning(f"Skipping invalid data_id: {data_id}")
        return

    scan_lines = find_all_scans_same_date(match_file, scan_id, data_id)
    sources = get_all_sources(match_file, scan_lines)
    beams = read_ahdr_for_dataid(ahdr_dir, data_id)

    if not sources:
        status = "NO_SOURCE"
    elif not beams:
        status = "NO_AHDR"
    else:
        obs_time = get_obs_time(data_id)
        copied_files = []
        rejected = 0

        for src_name, ra, dec in sources:

            ra_rad, dec_rad = source_to_precessed_radian(ra, dec, obs_time)

            best_sep = 1e9
            best_beam = None

            for beam_id, bra, bdec in beams:
                cos_sep = (
                    np.sin(dec_rad)*np.sin(bdec) +
                    np.cos(dec_rad)*np.cos(bdec)*np.cos(ra_rad - bra)
                )
                cos_sep = np.clip(cos_sep, -1, 1)
                sep = np.arccos(cos_sep)

                if sep < best_sep:
                    best_sep = sep
                    best_beam = (beam_id, bra, bdec)

            if best_beam is None:
                continue

            beam_id, _, _ = best_beam
            sep_arcmin = best_sep * (180/np.pi) * 60

            if sep_arcmin > max_beam_sep:
                rejected += 1
                logging.warning(f"{data_id} | {src_name} rejected ({sep_arcmin:.2f})")
                continue

            logging.info(f"{data_id} | {src_name} → BM{beam_id} | {sep_arcmin:.2f} arcmin")

            src_file = os.path.join(fil_dir, f"BM{beam_id}.fil")
            if not os.path.exists(src_file):
                continue

            out_dir = os.path.join(fil_dir, "targeted_source_data")
            os.makedirs(out_dir, exist_ok=True)

            safe_name = re.sub(r'[^A-Za-z0-9_+-]', '_', src_name)

            for f in os.listdir(out_dir):
                if f.startswith(safe_name + "_"):
                    os.remove(os.path.join(out_dir, f))

            dst_file = os.path.join(out_dir, f"{safe_name}_{scan_id}_BM{beam_id}.fil")
            shutil.copy(src_file, dst_file)

            copied_files.append((dst_file, src_name, beam_id, sep_arcmin))

        status = "COPIED" if copied_files else "NO_FILE"
        logging.info(f"{data_id} → rejected {rejected}")

    # Clean logs
    remove_old_status_entries(global_status_file, scan_id, data_id)

    # Write status
    ensure_newline(global_status_file)
    with open(global_status_file, "a") as f:
        if status == "COPIED":
            f.write(f"{scan_id} {data_id} {status} {len(copied_files)}\n")
        else:
            f.write(f"{scan_id} {data_id} {status}\n")

    # Write file log
    if status == "COPIED":
        remove_old_file_entries(global_log_file, scan_id, data_id)
        ensure_newline(global_log_file)
        with open(global_log_file, "a") as f:
            for file, src_name, beam_id, sep in copied_files:
                f.write(f"{file} | {src_name} | BM{beam_id} | sep={sep:.2f} arcmin | max={max_beam_sep}\n")

# --------------------------------------------------
# ENTRY
# --------------------------------------------------
if __name__ == "__main__":

    if len(sys.argv) != 6:
        print("Usage: python script.py <match_file> <scan_id> <data_id> <ahdr_dir> <fil_dir>")
        sys.exit(1)

    match_file, scan_id, data_id, ahdr_dir, fil_dir = sys.argv[1:]

    run_pipeline(match_file, scan_id, data_id, ahdr_dir, fil_dir)

    logging.info("Done.")