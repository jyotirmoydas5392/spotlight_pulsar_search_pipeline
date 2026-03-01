from pathlib import Path
from datetime import datetime
from PIL import Image
import numpy as np
import shutil
import math
import os


def radians_to_hms(rad):
    """Convert RA in radians to HH:MM:SS format."""
    total_hours = math.degrees(rad) / 15.0
    hours = int(total_hours)
    minutes = int((total_hours - hours) * 60)
    seconds = (total_hours - hours - minutes / 60) * 3600
    return f"{hours:02d}:{minutes:02d}:{seconds:05.2f}"

def radians_to_dms(rad):
    """Convert Dec in radians to DD:MM:SS format."""
    total_degrees = math.degrees(rad)
    sign = "-" if total_degrees < 0 else ""
    total_degrees = abs(total_degrees)
    degrees = int(total_degrees)
    minutes = int((total_degrees - degrees) * 60)
    seconds = (total_degrees - degrees - minutes / 60) * 3600
    return f"{sign}{degrees:02d}:{minutes:02d}:{seconds:05.2f}"

def extract_observation_info_from_header(header_dir):
    """
    Extracts RA, Dec in sexagesimal, Bandwidth (MHz), and Observed Frequency (MHz)
    from 'Observation_information_header_file.hdr' in the given directory.

    Args:
        header_dir (str): Path to the directory containing the copied header file.

    Returns:
        tuple: (RA_HMS, Dec_DMS, Bandwidth_MHz, Observed_Frequency_MHz)
    """
    header_path = os.path.join(header_dir, "Observation_information_header_file.hdr")

    ra_rad = None
    dec_rad = None
    bandwidth = None
    obs_freq_hz = None

    print(f"Reading header file from: {header_path}")

    try:
        with open(header_path, 'r') as file:
            for line in file:
                line = line.strip()
                if "Source RA (Rad)" in line:
                    ra_rad = float(line.split('=')[-1].strip())
                    print(f"Parsed RA (rad): {ra_rad}")
                elif "Source DEC (Rad)" in line:
                    dec_rad = float(line.split('=')[-1].strip())
                    print(f"Parsed Dec (rad): {dec_rad}")
                elif "Bandwidth (MHz)" in line:
                    bandwidth = float(line.split('=')[-1].strip())
                    print(f"Parsed Bandwidth (MHz): {bandwidth}")
                elif "Frequency Ch. 0  (Hz)" in line:
                    obs_freq_hz = float(line.split('=')[-1].strip())
                    print(f"Parsed Observed Frequency (Hz): {obs_freq_hz}")

        if ra_rad is None:
            print("Warning: RA not found in header.")
        if dec_rad is None:
            print("Warning: Dec not found in header.")
        if bandwidth is None:
            print("Warning: Bandwidth not found in header.")
        if obs_freq_hz is None:
            print("Warning: Observed Frequency not found in header.")

        ra_hms = radians_to_hms(ra_rad) if ra_rad is not None else None
        dec_dms = radians_to_dms(dec_rad) if dec_rad is not None else None
        obs_freq_mhz = obs_freq_hz / 1e6 if obs_freq_hz is not None else None

        print(f"RA (HMS): {ra_hms}, Dec (DMS): {dec_dms}, Bandwidth: {bandwidth}, Frequency (MHz): {obs_freq_mhz}")
        return ra_hms, dec_dms, bandwidth, obs_freq_mhz

    except Exception as e:
        print(f"Error reading or parsing header file: {e}")
        return None, None, None, None


def pngs_to_pdf(input_dir, output_dir, data_id, keyword, dpi=72):
    """
    Convert PNG images in input_dir to a multi-page PDF in output_dir.
    Reduces PDF size by lowering image DPI.
    
    Args:
        input_dir (str): Directory containing .png/.PNG files.
        output_dir (str): Directory to save the final PDF.
        data_id (str): Identifier for naming the PDF.
        keyword (str): Descriptive keyword for PDF naming.
        dpi (int): DPI to use when saving PDF (default: 100).
    """
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    target_file = os.path.join(output_path, f"{data_id}_final_folded_{keyword}_candidates.pdf")

    # Collect all .png and .PNG files
    png_files = sorted(input_path.glob("*.png")) + sorted(input_path.glob("*.PNG"))
    if not png_files:
        print(f"No PNG files found in '{input_dir}'")
        return

    # Convert PNGs to RGB images
    images = [Image.open(p).convert("RGB") for p in png_files]

    # Save all images to one PDF with reduced DPI
    images[0].save(
        target_file,
        save_all=True,
        append_images=images[1:],
        resolution=dpi
    )
    print(f"PDF saved with reduced DPI ({dpi}) to: {target_file}")


def copy_one_header_file(input_dir, output_dir):
    """
    Copies one .ahdr file from the input directory to the output directory,
    renaming it to 'Observation_information_header_file.hdr'.

    Args:
        input_dir (str): Path to the directory containing .ahdr files.
        output_dir (str): Path to the destination directory.
    """
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Find all .ahdr files in the input directory
    ahdr_files = [f for f in os.listdir(input_dir) if f.endswith(".ahdr")]

    if ahdr_files:
        # Select the first .ahdr file
        selected_file = ahdr_files[0]
        src_path = os.path.join(input_dir, selected_file)
        dest_path = os.path.join(output_dir, "Observation_information_header_file.hdr")
        
        # Copy and rename
        shutil.copy(src_path, dest_path)
        print(f"Copied '{selected_file}' to '{dest_path}'")
    else:
        print("No .ahdr files found in the input directory.")


def write_outputs_info(output_dir, data_id, band_id):
    """
    Write the Outputs_info.txt file containing summary information.
    """

    import os
    from datetime import datetime

    # Extract target name safely
    parts = data_id.split('_')
    target_name = '_'.join(parts[:-2]) if len(parts) > 2 else data_id

    # Extract header information safely
    try:
        ra, dec, bandwidth, obs_freq = extract_observation_info_from_header(output_dir)
    except Exception as e:
        print(f"Warning: Could not extract header info: {e}")
        ra = dec = bandwidth = obs_freq = "Unavailable"

    # File path
    info_file_path = os.path.join(output_dir, f"{data_id}_outputs_info.txt")

    # Timestamp
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Write file
    with open(info_file_path, "w") as info_file:
        info_file.write("Final Output Generation Summary\n")
        info_file.write("================================\n")
        info_file.write(f"Generation Time:             {timestamp}\n")
        info_file.write(f"Processed Target ID:         {data_id}\n")
        info_file.write(f"Target Name:                 {target_name}\n")
        info_file.write(f"Processed RA (HH:MM:SS):     {ra}\n")
        info_file.write(f"Processed Dec (DD:MM:SS):    {dec}\n")
        info_file.write(f"Observed Band:               {band_id}\n")
        info_file.write(f"Observed Frequency (MHz):    {obs_freq}\n")
        info_file.write(f"Bandwidth (MHz):             {bandwidth}\n")

    print(f"Info file saved at: {info_file_path}")


def copy_contents_only(input_dir, output_dir):
    """
    Copies all contents (files and subdirectories) from input_dir to output_dir
    without copying the input_dir itself.

    Parameters:
    - input_dir: str, path to the source directory.
    - output_dir: str, path to the destination directory.
    """
    if not os.path.exists(input_dir):
        raise FileNotFoundError(f"Input directory '{input_dir}' does not exist.")
    
    if not os.path.isdir(input_dir):
        raise NotADirectoryError(f"'{input_dir}' is not a directory.")
    
    os.makedirs(output_dir, exist_ok=True)

    for item in os.listdir(input_dir):
        src_path = os.path.join(input_dir, item)
        dest_path = os.path.join(output_dir, item)

        if os.path.isdir(src_path):
            shutil.copytree(src_path, dest_path, dirs_exist_ok=True)
        else:
            shutil.copy2(src_path, dest_path)

