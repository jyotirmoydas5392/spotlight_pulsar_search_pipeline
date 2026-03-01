import os
import re
import shutil
import sys
import time
import argparse
import logging
import numpy as np
import multiprocessing
import matplotlib.pyplot as plt

# Get the base directory from the environment variable
base_dir = os.getenv("PULSELINE_DEV_DIR")
if not base_dir:
    print("Error: PULSELINE_DEV_DIR environment variable is not set.")
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
    print("Error importing 'read_input_file_dir'. Ensure the script exists in the specified path.")
    print(e)
    sys.exit(1)
    

def read_candidates(input_dir, file_name, sorting_stage):
    """
    Loads the candidates file based on the sorting stage and ensures the output is always 2D.

    :param input_dir: The directory where the input files are located.
    :param file_name: The base name for the candidates file.
    :param sorting_stage: The sorting stage identifier.
    :return: 2D NumPy array (even if the file has a single row) or None if an error occurs.
    """
    # Define candidate file names based on sorting stage
    candidate_files = {
        0.0: f"{file_name}_all_sifted_candidates.txt",
        1.0: f"{file_name}_all_sifted_harmonic_removed_candidates.txt",
        2.0: f"{file_name}_all_sifted_harmonic_removed_beam_sorted_candidates.txt",
    }

    candidate_file = candidate_files.get(sorting_stage)
    if candidate_file is None:
        print("Invalid sorting_stage value.")
        return None

    # Load the candidate file safely
    try:
        file_path = os.path.join(input_dir, candidate_file)

        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            return None

        data = np.loadtxt(file_path, dtype=str, skiprows=1)  # Load data, skipping header

        # Handle empty file case
        if data.size == 0:
            print(f"Empty file: {file_path}")
            return None

        # Convert to float array
        candidate_array = np.array(data, dtype=float)

        # If 1D, reshape to 2D (1 row, N columns)
        if candidate_array.ndim == 1:
            candidate_array = candidate_array.reshape(1, -1)

        return candidate_array

    except Exception as e:
        print(f"Error loading candidate file {candidate_file}: {e}")
        return None



def plot_and_save_figures(periods, dms, snrs, sorting_stage, output_dir):
    """
    Generates and saves three scatter plots:
    - Period vs DM
    - Period vs SNR
    - DM vs SNR
    Saves individual plots and a combined plot inside 'summary_plots' directory.

    Parameters:
    - periods: Array for Period (sec).
    - dms: Array for DM (pc/cc).
    - snrs: Array for SNR.
    - sorting_stage: Numeric value indicating sorting stage (mapped to a keyword).
    - output_dir: Parent directory where 'summary_plots' will be created.
    """

    # Ensure all inputs are 1D arrays
    periods, dms, snrs = map(np.ravel, [periods, dms, snrs])

    # Define keyword mapping for sorting stages
    stage_keywords = {
        0: "Initial_sorting",
        1: "Harmonic_optimized",
        2: "Beam_sorted",
        3: "Yet_to_decide",
    }

    # Get keyword for current sorting_stage
    sorting_keyword = stage_keywords.get(sorting_stage, "Unknown_Stage")

    # Define and set the final plots directory
    plots_directory = os.path.join(output_dir, "summary_plots")
    os.makedirs(plots_directory, exist_ok=True)

    # Define file paths
    plot_files = {
        "Period_vs_DM": os.path.join(plots_directory, f"{sorting_keyword}_Period_vs_DM.png"),
        "Period_vs_SNR": os.path.join(plots_directory, f"{sorting_keyword}_Period_vs_SNR.png"),
        "DM_vs_SNR": os.path.join(plots_directory, f"{sorting_keyword}_DM_vs_SNR.png"),
        "Combined": os.path.join(plots_directory, f"{sorting_keyword}_Combined.png"),
    }

    # Define plot parameters
    plot_data = [
        (periods, dms, "Period (sec)", "DM (pc/cc)", f"{sorting_keyword}: Period vs DM", plot_files["Period_vs_DM"]),
        (periods, snrs, "Period (sec)", "SNR", f"{sorting_keyword}: Period vs SNR", plot_files["Period_vs_SNR"]),
        (dms, snrs, "DM (pc/cc)", "SNR", f"{sorting_keyword}: DM vs SNR", plot_files["DM_vs_SNR"]),
    ]

    # Generate and save **separate** plots
    for x, y, xlabel, ylabel, title, file_name in plot_data:
        plt.figure(figsize=(6, 5))
        plt.scatter(x, y, alpha=0.7)
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.title(title)
        plt.text(0.70, 0.95, f"N: {len(x)}", transform=plt.gca().transAxes, fontsize=12, fontweight='bold', verticalalignment='top')
        plt.savefig(file_name, bbox_inches='tight')
        plt.close()  # Close to free memory

    # Generate and save **combined** plot with subplots
    fig, axs = plt.subplots(1, 3, figsize=(15, 5))

    for ax, (x, y, xlabel, ylabel, title, _) in zip(axs, plot_data):
        ax.scatter(x, y, alpha=0.7)
        ax.set_xlabel(xlabel)
        ax.set_ylabel(ylabel)
        ax.set_title(title)
        ax.text(0.70, 0.95, f"N: {len(x)}", transform=ax.transAxes, fontsize=12, fontweight='bold', verticalalignment='top')

    fig.savefig(plot_files["Combined"], bbox_inches='tight')
    plt.close(fig)  # Close to free memory

    # Print saved file paths
    print(f"Plots saved in '{plots_directory}':")
    for name, path in plot_files.items():
        print(f"- {name}: {path}")


def summary_plots(input_dir, output_dir, input_file_dir, total_sorting_stage):

     # Process files to get the file name for processing the sorted candidate files for genearting summary plots.
    files_to_process = [
        f for f in os.listdir(input_file_dir)
        if f.startswith("PULSELINE") and "node" in f and "gpu_id" in f and f.endswith(".txt")
    ]

    if not files_to_process:
        logging.info(f"No files found in {input_file_dir} matching the criteria. Skipping...")
        return

    os.makedirs(output_dir, exist_ok=True)

    all_files = []
    # Save the filterbank files for accessig the canddiate files for geneartig the sumamry plots
    for i, file in enumerate(sorted(files_to_process)):
        # Reload parameters from pulseline input file
        try:
            params = load_parameters(os.path.join(input_file_dir, file))
        except Exception as e:
            print(f"Error loading parameters from configuration file: {e}")
            sys.exit(1)
        
        # Extract and print 'fil_file' from params
        fil_file_path = params.get('fil_file')
        if fil_file_path:
            fil_file = os.path.basename(fil_file_path)
            print(f"Extracted fil file name: {fil_file}")
        else:
            print("fil_file parameter not found in the loaded parameters.")
        file_name = fil_file.replace(".fil", "")
        all_files.append(file_name)


    # Load the flags for plotting the different summary plots
    params = load_parameters(os.path.join(input_file_dir, "pulseline_master.txt"))
    harmonic_opt_flag = params.get('harmonic_opt_flag')
    beam_sort_flag = params.get('beam_sort_flag')

    # Generate the data to plot for different stages of filtering
    for i in range(total_sorting_stage):

        # Skip i = 1 if harmonic_opt_flag is not set
        if i == 1 and harmonic_opt_flag != 1:
            continue

        # Skip i = 2 if beam_sort_flag is not set
        if i == 2 and beam_sort_flag != 1:
            continue

        Period_array = []
        DM_array = []
        SNR_array = []

        for file_name in all_files:
            # Load candidates based on harmonic flag
            candidate_array = read_candidates(input_dir, file_name, sorting_stage=float(i))
            if candidate_array is None:
                print("No candidates to process. Skipping...")
                continue

            Period_array.extend(np.array(candidate_array[:, 0], dtype=float).flatten())
            DM_array.extend(np.array(candidate_array[:, 2], dtype=float).flatten())
            SNR_array.extend(np.array(candidate_array[:, 3], dtype=float).flatten())

        # Plot the figures for the current sorting stage
        plot_and_save_figures(Period_array, DM_array, SNR_array, sorting_stage=float(i), output_dir=output_dir)

