import numpy as np
import os 
import logging 
import sys 

# Initialize globals
def initialize_globals():
    global leader_frequency
    global leader_DMs
    global leader_SNRs
    global leader_pdots
    global leader_sr_nos

    try:
        leader_frequency
    except NameError:
        leader_frequency = []

    try:
        leader_DMs
    except NameError:
        leader_DMs = []

    try: 
        leader_SNRs
    except NameError:
        leader_SNRs = []

    try: 
        leader_pdots
    except NameError:
        leader_pdots = []

    try: 
        leader_sr_nos
    except NameError:
        leader_sr_nos = []

def remove_duplicate_indices(input_path, output_path):
    rows = []

    # Read file
    with open(input_path, 'r') as f:
        header = f.readline().strip().split()  # first line is header
        for line in f:
            parts = line.strip().split()
            if len(parts) != 5:
                continue  # skip malformed lines
            period, pdot, dm, snr, idx = parts
            rows.append([
                float(period),
                float(pdot),
                float(dm),
                float(snr),
                int(idx)
            ])

    # Keep best (highest SNR) row for each index
    best_by_index = {}
    for row in rows:
        idx = row[-1]
        snr = row[3]
        if idx not in best_by_index or snr > best_by_index[idx][3]:
            best_by_index[idx] = row

    # Sort by index
    unique_rows = [best_by_index[k] for k in sorted(best_by_index.keys())]

    # Write cleaned file WITHOUT index column
    with open(output_path, 'w') as f_out:
        # drop index from header
        if header[-1].lower() == "index":
            header = header[:-1]
        f_out.write(" ".join(header) + "\n")

        for r in unique_rows:
            f_out.write(f"{r[0]:.10f} {r[1]:.6e} {r[2]:.2f} {r[3]:.2f}\n")

    print(f"Cleaned file (no index column) written to {output_path}")

def filter_candidates(infile, outfile, DM_slope, DM_intercept):
    """
    Removes duplicates from candidate file:
    - Candidates are duplicates if they share the same period
      and their DMs differ by <= dm_tol.
    - Keeps only the candidate with the highest SNR per cluster.

    Args:
        infile (str): Path to input text file.
        outfile (str): Path to save cleaned file.
        dm_tol (float): DM tolerance for grouping (default=1.0).
    """

    tol_period = 0.050  # fixed period of 50 ms
    dm_tol = DM_intercept + DM_slope * tol_period
    with open(infile) as f:
        header = f.readline()
        rows = [list(map(float, line.split())) for line in f if line.strip()]

    # sort by Period, then by descending SNR
    rows.sort(key=lambda x: (x[0], -x[3]))

    kept = []
    for p, pdot, dm, snr in rows:
        # keep only if no stronger candidate with same period & nearby DM exists
        if not any(abs(dm - d) <= dm_tol and p == pp for pp, _, d, _ in kept):
            kept.append([p, pdot, dm, snr])

    with open(outfile, "w") as f:
        f.write(header)
        for p, pdot, dm, snr in kept:
            f.write(f"{p:.10f} {pdot:.6e} {dm:.2f} {snr:.2f}\n")

def is_harmonically_related(p1, p2, dm1, dm2, max_harm, period_tol, DM_slope, DM_intercept):
    """
    Check if p1 and p2 are harmonically related up to max_harm and have similar DM.
    Uses dynamic DM tolerance calculated from fixed period (10 ms) for consistency.
    """
    tol_period = 0.05  # fixed period of 50 ms
    dm_tolerance = DM_intercept + DM_slope * tol_period
    period_ratio = p1 / p2

    for n in range(1, max_harm + 1):
        for m in range(1, max_harm + 1):
            if abs(period_ratio - (n / m)) < period_tol:
                if abs(dm1 - dm2) <= dm_tolerance:
                    return True
    return False

def harmonic_elimination(input_dir, output_dir, input_file_dir):
    
    '''
    Creates the harmonic groups and replaces all the harmonics with the one having the maximum SNR
    '''

    try:
        from read_input_file_dir import load_parameters
    except ImportError as e:
        print("Error importing 'read_input_file_dir'. Ensure the script exists in the specified path.")
        print(e)
        sys.exit(1)
    
    initialize_globals()

    # Process files to get the file name for processing the candidate files for sorting.
    files = [
        f for f in os.listdir(input_file_dir)
        if f.startswith("PULSELINE") and "node" in f and "gpu_id" in f and f.endswith(".txt")
    ]

    try:
        params = load_parameters(os.path.join(input_file_dir, files[0]))
    except Exception as e:
        print(f"Error loading parameters from configuration file: {e}")
        sys.exit(1)

    period_tol_harm = params.get('period_tol_harm')
    max_harm = params.get('max_harm')
    DM_filtering_cut_1000 = params.get('DM_filtering_cut_1000')
    DM_filtering_cut_10 = params.get('DM_filtering_cut_10')

    if not files:
        logging.info(f"No files found in {input_file_dir} matching the criteria. Skipping...")
        return

    # Create directory, if don't exist
    os.makedirs(output_dir, exist_ok=True)
    
    full_path = None
    files_to_process  = [
        f for f in os.listdir(input_dir)
        if f.startswith("BM") and f.endswith("all_sifted_candidates.txt")
    ]

    for file_name in files_to_process:
        new_name = file_name.replace("_candidates.txt", "_harmonic_removed_candidates.txt", 1)
        full_path = os.path.join(output_dir, new_name)
    
        with open(full_path, "w"):
            pass  # creates empty file in target_dir

    if full_path is not None:
        print(f"Created: {full_path}")
    else:
        print("No files were processed.")

    #print(os.getcwd())
    #print(files_to_process)   
    
    for i, file in enumerate(sorted(files_to_process)):
     
        harmonic_groups = []
        harmonic_DMs = []
        harmonic_pdots = []
        harmonic_SNRs = []
        harmonic_sr_nos = []
       
        file_name  = file.split("_")[0]

        input_path = os.path.join(input_dir, f"{file_name}_all_sifted_candidates.txt")
        output_path = os.path.join(output_dir, f"{file_name}_all_sifted_harmonic_removed_candidates.txt")

        # Read input file
        try:
            with open(input_path, 'r') as f:
                lines = f.readlines()
        except FileNotFoundError:
            print(f"Input file {input_path} not found.")
            return
    
        if len(lines) == 1 and "No valid data found to process." in lines[0]:
            with open(output_path, 'w') as f_out:
                f_out.write("No valid data found to process.\n")
            print(f"No valid data found. Exiting. Response written to {output_path}")
            return
    
        try:
            data = np.loadtxt(input_path, dtype=float, skiprows=1)
            if data.size == 0:
                raise ValueError("No valid candidate data found in the file.")
        except Exception as e:
            with open(output_path, 'w') as f_out:
                f_out.write("No valid data found to process.\n")
            print(f"Error reading input file: {e}. Response written to {output_path}")
            return
 
        # Loading the params from the file into an array
        periods = data[:, 0]
        p_dots = data[:, 1]
        DMs = data[:, 2]
        SNRs = data[:, 3]
        n_candidates = len(periods)
        sr_nos = np.arange(0, n_candidates)

        print("Step 2 done")

        # Dynamic DM tolerance parameters
        DM_slope = (DM_filtering_cut_1000 - DM_filtering_cut_10) / (1.000 - 0.010)
        DM_intercept = DM_filtering_cut_10 - DM_slope * 0.010

        # Highest SNR first and lowest last and indexes correspond are saved in "index"
        index = np.argsort(-SNRs)

        # Soting the arrays according to the decreasing SNRs
        periods = periods[index]
        p_dots = p_dots[index]
        DMs = DMs[index]
        SNRs = SNRs[index]
        sr_nos = sr_nos[index]

        print("Step 3 done")
    
        for i in range(n_candidates):
            appending_group = [periods[i]]
            appending_DMs = [DMs[i]]
            appending_pdots = [p_dots[i]]
            appending_SNRs = [SNRs[i]]
            appending_sr_nos = [sr_nos[i]]

            for j in range(n_candidates):
                higher_period = max(periods[i], periods[j])
                period_tol = higher_period * period_tol_harm
       
                if j > i:

                    if is_harmonically_related(periods[i], periods[j], DMs[i], DMs[j], max_harm, period_tol, DM_slope, DM_intercept):
                        appending_group.append(periods[j])
                        appending_DMs.append(DMs[j])
                        appending_pdots.append(p_dots[j])
                        appending_SNRs.append(SNRs[j])
                        appending_sr_nos.append(sr_nos[j])
                        continue

            harmonic_groups.append(appending_group)
            harmonic_DMs.append(appending_DMs)
            harmonic_SNRs.append(appending_SNRs)
            harmonic_pdots.append(appending_pdots)
            harmonic_sr_nos.append(appending_sr_nos)

        print("Step 4 done")

        # Replaces the entire array with the highest SNR frequency and the average DM
        # for groups in harmonic_groups:
        #     groups[:] = [groups[0]]
        
        # for DMs in harmonic_DMs:
        #     DMs[:] = [(max(DMs) + min(DMs))/2]

        new_harmonic_groups = []

        for groups, snrs in zip(harmonic_groups, harmonic_SNRs):
            if not groups:
                new_harmonic_groups.append([])
                continue

            # guard if lengths differ; choose min so indexing stays valid
            L = min(len(groups), len(snrs))
            max_idx = max(range(L), key=lambda j: snrs[j])   # index of max SNR (first max if tie)
            chosen_freq = groups[max_idx]

            # fill the whole subgroup with that chosen frequency
            new_harmonic_groups.append([chosen_freq] * len(groups))

        harmonic_groups = new_harmonic_groups

        print("Step 5 done")

        # This part is to ensure that the same frequency is constant across all the beams

        for i in range(len(harmonic_groups)):
            new_freq = harmonic_groups[i][0]
            new_dm = harmonic_DMs[i][0]
            new_SNR = harmonic_SNRs[i][0]
            new_sr_nos = harmonic_sr_nos[i][0]
            new_pdots = harmonic_pdots[i][0]

            is_related = False

            for existing_freq, existing_dm, existing_SNR in zip(leader_frequency, leader_DMs, leader_SNRs):
                if is_harmonically_related(new_freq, existing_freq, new_dm, existing_dm, max_harm, period_tol, DM_slope, DM_intercept):
                    is_related = True
                    break 

                # if is_related:
                #     #print(harmonic_groups)
                #     if existing_SNR > new_SNR: 
                #         harmonic_groups[i][:] = [existing_freq]
                #     else:
                #         harmonic_groups[i][:] = [new_freq]

            if is_related:
                if new_SNR > existing_SNR:
                    for j, freq in enumerate(leader_frequency):
                        if freq == existing_freq:
                            leader_frequency[j] = new_freq
                            leader_SNRs[j] = new_SNR
                            break

            if not is_related:
                leader_frequency.append(new_freq)
                leader_DMs.append(new_dm)
                leader_SNRs.append(new_SNR)
                leader_pdots.append(new_pdots)
                leader_sr_nos.append(new_sr_nos)

        print("Step 6 done")

        # print("Leader Frequency: ", leader_frequency)
        # print("Leader DMs:", leader_DMs)
        # print("Leader SNRs:", leader_SNRs)


def harmonic_replacement(input_dir, output_dir, input_file_dir):
    
    try:
        from read_input_file_dir import load_parameters
    except ImportError as e:
        print("Error importing 'read_input_file_dir'. Ensure the script exists in the specified path.")
        print(e)
        sys.exit(1)

    # Process files to get the file name for processing the candidate files for sorting.
    files = [
        f for f in os.listdir(input_file_dir)
        if f.startswith("PULSELINE") and "node" in f and "gpu_id" in f and f.endswith(".txt")
    ]

    try:
        params = load_parameters(os.path.join(input_file_dir, files[0]))
    except Exception as e:
        print(f"Error loading parameters from configuration file: {e}")
        sys.exit(1)

    period_tol_harm = params.get('period_tol_harm')
    max_harm = params.get('max_harm')
    DM_filtering_cut_1000 = params.get('DM_filtering_cut_1000')
    DM_filtering_cut_10 = params.get('DM_filtering_cut_10')

    if not files:
        logging.info(f"No files found in {input_file_dir} matching the criteria. Skipping...")
        return

    # Create directory, if don't exist
    os.makedirs(output_dir, exist_ok=True)
    
    full_path = None
    files_to_process  = [
        f for f in os.listdir(input_dir)
        if f.startswith("BM") and f.endswith("all_sifted_candidates.txt")
    ]

    for file_name in files_to_process:
        new_name = file_name.replace("_candidates.txt", "_harmonic_removed_candidates.txt", 1)
        full_path = os.path.join(output_dir, new_name)
    
        with open(full_path, "w"):
            pass  # creates empty file in target_dir

    if full_path is not None:
        print(f"Created: {full_path}")
    else:
        print("No files were processed.")

    #print(os.getcwd())
    print(files_to_process)  

    for i, file in enumerate(sorted(files_to_process)):
     
        harmonic_groups = []
        harmonic_DMs = []
        harmonic_pdots = []
        harmonic_SNRs = []
        harmonic_sr_nos = []
       
        file_name  = file.split("_")[0]

        input_path = os.path.join(input_dir, f"{file_name}_all_sifted_candidates.txt")
        output_path = os.path.join(output_dir, f"{file_name}_all_sifted_harmonic_removed_candidates.txt")

        print("Output_path: ", output_path)

        # Read input file
        try:
            with open(input_path, 'r') as f:
                lines = f.readlines()
        except FileNotFoundError:
            print(f"Input file {input_path} not found.")
            return
    
        if len(lines) == 1 and "No valid data found to process." in lines[0]:
            with open(output_path, 'w') as f_out:
                f_out.write("No valid data found to process.\n")
            print(f"No valid data found. Exiting. Response written to {output_path}")
            return
    
        try:
            data = np.loadtxt(input_path, dtype=float, skiprows=1)
            if data.size == 0:
                raise ValueError("No valid candidate data found in the file.")
        except Exception as e:
            with open(output_path, 'w') as f_out:
                f_out.write("No valid data found to process.\n")
            print(f"Error reading input file: {e}. Response written to {output_path}")
            return

        # Loading the params from the file into an array
        periods = data[:, 0]
        p_dots = data[:, 1]
        DMs = data[:, 2]
        SNRs = data[:, 3]
        n_candidates = len(periods)
        sr_nos = np.arange(0, n_candidates)

        print("Step 2 done")

        # Dynamic DM tolerance parameters
        DM_slope = (DM_filtering_cut_1000 - DM_filtering_cut_10) / (1.000 - 0.010)
        DM_intercept = DM_filtering_cut_10 - DM_slope * 0.010

        # Highest SNR first and lowest last and indexes correspond are saved in "index"
        index = np.argsort(-SNRs)

        # Soting the arrays according to the decreasing SNRs
        periods = periods[index]
        p_dots = p_dots[index]
        DMs = DMs[index]
        SNRs = SNRs[index]
        sr_nos = sr_nos[index]

        print("Step 3 done")
    
        for i in range(n_candidates):
            appending_group = [periods[i]]
            appending_DMs = [DMs[i]]
            appending_pdots = [p_dots[i]]
            appending_SNRs = [SNRs[i]]
            appending_sr_nos = [sr_nos[i]]

            for j in range(n_candidates):
                higher_period = max(periods[i], periods[j])
                period_tol = higher_period * period_tol_harm
       
                if j > i:

                    if is_harmonically_related(periods[i], periods[j], DMs[i], DMs[j], max_harm, period_tol, DM_slope, DM_intercept):
                        appending_group.append(periods[j])
                        appending_DMs.append(DMs[j])
                        appending_pdots.append(p_dots[j])
                        appending_SNRs.append(SNRs[j])
                        appending_sr_nos.append(sr_nos[j])
                        continue

            harmonic_groups.append(appending_group)
            harmonic_DMs.append(appending_DMs)
            harmonic_SNRs.append(appending_SNRs)
            harmonic_pdots.append(appending_pdots)
            harmonic_sr_nos.append(appending_sr_nos)

        new_harmonic_groups = []

        for groups, snrs in zip(harmonic_groups, harmonic_SNRs):
            if not groups:
                new_harmonic_groups.append([])
                continue

            # guard if lengths differ; choose min so indexing stays valid
            L = min(len(groups), len(snrs))
            max_idx = max(range(L), key=lambda j: snrs[j])   # index of max SNR (first max if tie)
            chosen_freq = groups[max_idx]

            # fill the whole subgroup with that chosen frequency
            new_harmonic_groups.append([chosen_freq] * len(groups))

        harmonic_groups = new_harmonic_groups

        # print("HARMONIC GROUPS:", harmonic_groups)
        # print("HARMONIC DMS:", harmonic_DMs)
        # print("HARMONIC SNR:", harmonic_SNRs)


        # This part is to ensure that the same frequency is constant across all the beams

        for i in range(len(harmonic_groups)):
            new_freq = harmonic_groups[i][0]
            new_dm = harmonic_DMs[i][0]
            new_SNR = harmonic_SNRs[i][0]
            new_sr_nos = harmonic_sr_nos[i][0]
            new_pdots = harmonic_pdots[i][0]

            is_related = False

            for existing_freq, existing_dm, existing_SNR in zip(leader_frequency, leader_DMs, leader_SNRs):
                if is_harmonically_related(
                    new_freq, existing_freq, new_dm, existing_dm,
                    max_harm, period_tol, DM_slope, DM_intercept
                ):
                    is_related = True
                    break

            if is_related:
                if existing_SNR > new_SNR:
                    harmonic_groups[i] = [existing_freq] * len(harmonic_groups[i])

        # Write output
        with open(output_path, 'w') as f_out:
            f_out.write("Period(sec) Pdot(s/s) DM(pc/cc) SNR Index\n")
            for row_a, row_b, row_c, row_d, row_e in zip(
                harmonic_groups, harmonic_pdots, harmonic_DMs, harmonic_SNRs, harmonic_sr_nos
            ):
                for val_a, val_b, val_c, val_d, val_e in zip(row_a, row_b, row_c, row_d, row_e):
                    f_out.write(f"{val_a:.10f} {val_b:.6e} {val_c:.2f} {val_d:.2f} {val_e}\n")

        # filter_candidates(output_path, output_path, DM_slope, DM_intercept)
        remove_duplicate_indices(output_path, output_path)

        print(f"Harmonic replacement done for {file}")


# def main():

#     input_dir = "/lustre_data/spotlight/data/PULSELINE_OUTPUT_DATA/TEST"
#     output_dir = "/lustre_data/spotlight/data/PULSELINE_OUTPUT_DATA/TEST"
#     input_file_dir = "/lustre_data/spotlight/data/pulsar_search_dev_run_files/pulseline_run_files/input_files/TEST"

#     harmonic_elimination(input_dir, output_dir, input_file_dir)
#     harmonic_replacement(input_dir, output_dir, input_file_dir)

# if __name__ == "__main__":
#     main()
