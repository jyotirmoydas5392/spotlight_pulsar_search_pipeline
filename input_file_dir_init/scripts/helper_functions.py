import os
import sys
import math
import re
import subprocess
from astropy.coordinates import SkyCoord
import astropy.units as u
import logging
import argparse
import numpy as np

def clear_directory(directory, file_prefix):
    """Deletes all files in the directory starting with the given file_prefix."""
    if not os.path.exists(directory):
        print(f"Directory {directory} does not exist.")
        return
    for filename in os.listdir(directory):
        if filename.startswith(file_prefix):
            file_path = os.path.join(directory, filename)
            try:
                os.remove(file_path)
                print(f"Deleted pre-generated file: {file_path}")
            except Exception as e:
                print(f"Error deleting file {file_path}: {e}")


def get_source_ra_dec_from_ahdr(ahdr_dir, data_id):
    """
    Pick first .ahdr file (lowest index) matching data_id
    and return Source RA/DEC in degrees.
    
    If anything fails → returns (0.0, 0.0)
    """

    def rad_to_deg(rad):
        return rad * 180.0 / math.pi

    def extract_index(fname):
        m = re.search(r'\.raw\.(\d+)\.ahdr$', fname)
        return int(m.group(1)) if m else float('inf')

    try:
        # ---- Find matching files ----
        ahdr_files = [
            f for f in os.listdir(ahdr_dir)
            if f.endswith(".ahdr") and data_id in f
        ]

        if not ahdr_files:
            return 0.0, 0.0

        # ---- Sort numerically ----
        ahdr_files.sort(key=extract_index)

        ahdr_path = os.path.join(ahdr_dir, ahdr_files[0])

        source_ra = None
        source_dec = None

        # ---- Parse file ----
        with open(ahdr_path, "r") as f:
            for line in f:
                line = line.strip()

                if "Source RA (Rad)" in line:
                    source_ra = float(line.split("=")[-1].strip())

                elif "Source DEC (Rad)" in line:
                    source_dec = float(line.split("=")[-1].strip())

        if source_ra is None or source_dec is None:
            return 0.0, 0.0

        return rad_to_deg(source_ra), rad_to_deg(source_dec)

    except Exception:
        return 0.0, 0.0


def get_frequency_mhz_from_ahdr(ahdr_dir, data_id):
    """
    Read 'Frequency Ch. 0 (Hz)' from first matching .ahdr file
    and return value in MHz.

    If anything fails → returns 1350.0 MHz
    """

    def extract_index(fname):
        m = re.search(r'\.raw\.(\d+)\.ahdr$', fname)
        return int(m.group(1)) if m else float('inf')

    try:
        # ---- Find matching files ----
        ahdr_files = [
            f for f in os.listdir(ahdr_dir)
            if f.endswith(".ahdr") and data_id in f
        ]

        if not ahdr_files:
            return 1350.0

        # ---- Sort numerically ----
        ahdr_files.sort(key=extract_index)

        ahdr_path = os.path.join(ahdr_dir, ahdr_files[0])

        # ---- Parse file ----
        with open(ahdr_path, "r") as f:
            for line in f:
                line = line.strip()

                if "Frequency Ch. 0" in line:
                    freq_hz = float(line.split("=")[-1].strip())
                    return freq_hz / 1e6  # Hz → MHz

        return 1350.0

    except Exception:
        return 1350.0

def get_ne2025_dm_limit(NE2025_model_exe_path, ahdr_dir, data_id):
    """
    Estimate LOS DM using NE2025 and round up to next multiple of 10.
    If NE2025 fails → fallback to DM = 2000.
    """

    # ---- Helper: round up to next multiple of 10 ----
    def round_up_to_next_10(x):
        return int(math.ceil(x / 10.0) * 10)

    # ---- Ensure absolute path ----
    NE2025_model_exe_path = os.path.abspath(NE2025_model_exe_path)

    # ---- Get RA/DEC ----
    ra_deg, dec_deg = get_source_ra_dec_from_ahdr(ahdr_dir, data_id)

    # ---- Convert to Galactic ----
    c = SkyCoord(ra=ra_deg * u.deg, dec=dec_deg * u.deg, frame='icrs')
    l_deg = c.galactic.l.deg
    b_deg = c.galactic.b.deg

    # ---- NE2025 command ----
    cmd = [
        NE2025_model_exe_path,
        str(l_deg),
        str(b_deg),
        "50",
        "-1"
    ]

    ne_dir = os.path.dirname(NE2025_model_exe_path)

    dm = None

    try:
        result = subprocess.check_output(
            cmd,
            universal_newlines=True,
            cwd=ne_dir
        )

        # ---- Parse DM safely ----
        capture = False

        for line in result.splitlines():
            if "#NE2025 output" in line:
                capture = True
                continue

            if capture:
                match = re.search(r'^\s*([\d.E+-]+)\s+DM\s', line)
                if match:
                    dm = float(match.group(1))
                    break

        if dm is None:
            raise ValueError("DM not found in NE2025 output")

    except Exception as e:
        print("WARNING: NE2025 failed, using fallback DM = 2000")
        print("Reason:", str(e))
        dm = 2000.0

    # ---- Round up ----
    dm_rounded = round_up_to_next_10(dm)

    return dm_rounded


def get_search_dm_limit(ne2025_model_exe_path, aa_input_file_dir, ahdr_dir, data_id):

    # ---- NE2025 DM ----
    dm_model = get_ne2025_dm_limit(ne2025_model_exe_path, ahdr_dir, data_id)

    # ---- Apply safety factor ----
    dm_model_safe = 2.0 * dm_model

    # ---- Frequency ----
    obs_freq = get_frequency_mhz_from_ahdr(ahdr_dir, data_id)

    # ---- Band selection ----
    if obs_freq <= 520:
        band = "BAND3"
    elif 530 <= obs_freq < 970:
        band = "BAND4"
    else:
        band = "BAND5"

    # ---- Read band DM limits ----
    band_dm_limits_file = os.path.join(aa_input_file_dir, "band_dm_limits.txt")

    band_dm_limit = None

    if os.path.exists(band_dm_limits_file):
        with open(band_dm_limits_file, "r") as f:
            for line in f:
                line = line.strip()

                if not line or line.startswith("#"):
                    continue

                parts = line.split()

                if len(parts) < 2:
                    continue

                band_name = parts[0]
                dm_value = float(parts[1])

                if band_name == band:
                    band_dm_limit = dm_value
                    break

    # ---- Final DM logic ----
    if band_dm_limit is not None:
        final_dm_limit = min(dm_model_safe, band_dm_limit)
    else:
        print("WARNING: Band DM limit not found for %s, using model DM" % band)
        final_dm_limit = dm_model_safe

    # ---- Enforce minimum ----
    final_dm_limit = max(final_dm_limit, 50)

    return int(final_dm_limit)