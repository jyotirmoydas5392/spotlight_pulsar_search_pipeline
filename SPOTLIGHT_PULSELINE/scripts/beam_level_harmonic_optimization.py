import numpy as np
import os 
import logging 
import sys 
import time

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
                    return True, True

    return False, False

def is_harmonically_related_vectorized(p1, p2, dm1, dm2, max_harm, period_tol, DM_slope, DM_intercept):
    """
    Vectorized: returns a boolean mask for all p2 values that are harmonically related to p1.
    """
    tol_period = 0.05  # fixed period (200 ms here, check if you meant 50 ms = 0.05)
    dm_tolerance = DM_intercept + DM_slope * tol_period

    period_ratio = p1 / p2  # shape (N,)

    # build harmonic fractions n/m
    #harm = np.array([n/m for n in range(1, max_harm+1) for m in range(1, max_harm+1)])

    harm = np.array([0.1,0.11111111,0.125,0.14285714,0.16666667,0.2,0.25,0.33333333,0.5,1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0])

    # outer difference → shape (N, H)
    diff = np.abs(period_ratio[:, None] - harm[None, :])

    # True if ANY harmonic matches within tolerance
    harmonic_match = (diff < period_tol).any(axis=1)

    # DM constraint
    dm_match = np.abs(dm1 - dm2) <= dm_tolerance

    return harmonic_match & dm_match

def initialize_numpy_array(files_to_process, input_dir):
    n_candidates = 0
    
    print(input_dir)
    for files in files_to_process:

        input_path = os.path.join(input_dir, f"{files}") 

        data = np.loadtxt(input_path, skiprows=1)
        if len(data[:, 0]) > n_candidates:
            n_candidates = len(data[:, 0])

    Periods = np.full((n_candidates, len(files_to_process)), np.nan)
    DMs = np.full((n_candidates, len(files_to_process)), np.nan)
    SNRs = np.full((n_candidates, len(files_to_process)), np.nan)
    Pdots = np.full((n_candidates, len(files_to_process)), np.nan)
    Indexes = np.full((n_candidates, len(files_to_process)), np.nan)
    Harmonics = np.full((n_candidates, len(files_to_process)), np.nan)

    return Periods, DMs, SNRs, Pdots, Indexes, Harmonics, n_candidates

def harmonic_elimination(input_dir, output_dir, input_file_dir):
    
    '''
    Creates the harmonic groups and replaces all the harmonics with the one having the maximum SNR
    '''

    start_time = time.time()

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

    print(input_file_dir)

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

    #files_to_process = ["BM100_all_sifted_candidates.txt", "BM30_all_sifted_candidates.txt"]

    # Periods, DMs, SNRs, Pdots, Indexes, Harmonics, n_candidates = initialize_numpy_array(files_to_process, input_dir)

    Periods   = []
    Pdots     = []
    DMs       = []
    SNRs      = []
    Harmonics = []
    n_cands   = []
    
    print("Files to process are:",sorted(files_to_process))
    updated_files_to_process = sorted(files_to_process)
    for i, file in enumerate(sorted(files_to_process)):
       
        print("File is:", file)
        new_name = file.replace("_candidates.txt", "_harmonic_removed_candidates.txt", 1)
        full_path = os.path.join(output_dir, new_name)
        output_path = full_path

        print("Output path:", output_path)

        with open(full_path, "w"):
            pass  # creates empty file in target_dir

        if full_path is not None:
            print(f"Created: {full_path}")
        else:
            print("No files were processed.")

        #print(os.getcwd())
        #print(files_to_process)
 
        input_path = os.path.join(input_dir, f"{file}")
        print("Input_path:", input_path)

        # Read input file
        try:
            with open(input_path, 'r') as f:
                lines = f.readlines()
        except FileNotFoundError:
            print(f"Input file {input_path} not found.")
            updated_files_to_process.remove(file)
            continue
    
        if len(lines) == 1 and "No valid data found to process." in lines[0]:
            with open(output_path, 'w') as f_out:
                f_out.write("No valid data found to process.\n")
            print(f"No valid data found. Exiting. Response written to {output_path}")
            updated_files_to_process.remove(file)
            continue
    
        try:
            data = np.loadtxt(input_path, dtype=float, skiprows=1)
            if data.size == 0:
                raise ValueError("No valid candidate data found in the file.")
        except Exception as e:
            with open(output_path, 'w') as f_out:
                f_out.write("No valid data found to process.\n")
            print(f"Error reading input file: {e}. Response written to {output_path}")
            updated_files_to_process.remove(file)
            continue
 
        # Periods[:len(data), i] = data[:, 0]
        # Pdots[:len(data), i] = data[:, 1]
        # DMs[:len(data), i] = data[:, 2]
        # SNRs[:len(data), i] = data[:, 3]        
        # Harmonics[:len(data), i] = data[:, 4]      

        n_cands.append(len(data[:, 0]))
        Periods.append(data[:, 0])
        Pdots.append(data[:, 1])
        DMs.append(data[:, 2])
        SNRs.append(data[:, 3])
        Harmonics.append(data[:, 4])  

        print("Step 2 done", i)
    
    Periods   = np.concatenate(Periods)
    Pdots     = np.concatenate(Pdots)
    DMs       = np.concatenate(DMs)
    SNRs      = np.concatenate(SNRs)
    Harmonics = np.concatenate(Harmonics)
    n_cands   = np.array(n_cands)

    # Dynamic DM tolerance parameters
    DM_slope = (DM_filtering_cut_1000 - DM_filtering_cut_10) / (1.000 - 0.010)
    DM_intercept = DM_filtering_cut_10 - DM_slope * 0.010

    total_cands = np.sum(n_cands)
    Indices = np.arange(0, total_cands)
    
    # Flatten
    #Indices_flatten = np.arange(1, len(files_to_process) * n_candidates + 1)
    # Indices_flatten = np.arange(0, len(files_to_process) * n_candidates)
    # Periods_flatten = Periods.flatten()
    # Pdots_flatten = Pdots.flatten()
    # DMs_flatten = DMs.flatten()
    # SNRs_flatten = SNRs.flatten()
    # Harmonics_flatten = Harmonics.flatten()

    # # Sort by SNR descending
    # index = np.argsort(-SNRs_flatten)
    # Periods_flatten = Periods_flatten[index]
    # Pdots_flatten = Pdots_flatten[index]
    # DMs_flatten = DMs_flatten[index]
    # SNRs_flatten = SNRs_flatten[index]
    # Indices_flatten = Indices_flatten[index]
    # Harmonics_flatten = Harmonics_flatten[index]

    # Sort by SNR descending
    index = np.argsort(-SNRs)
    Periods_flatten = Periods[index]
    Pdots_flatten = Pdots[index]
    DMs_flatten = DMs[index]
    SNRs_flatten = SNRs[index]
    Indices_flatten = Indices[index]
    Harmonics_flatten = Harmonics[index]

    # # Filter out NaNs
    # Nan_index = np.where(np.isnan(Periods_flatten))
    # Filtered_period_list0 = np.delete(Periods_flatten, Nan_index[0])
    # Filtered_pdots_list0 = np.delete(Pdots_flatten, Nan_index[0])
    # Filtered_DMs_list0 = np.delete(DMs_flatten, Nan_index[0])
    # Filtered_SNRs_list0 = np.delete(SNRs_flatten, Nan_index[0])
    # Filtered_Indices_list0 = np.delete(Indices_flatten, Nan_index[0])

    Filtered_period_list0 = Periods_flatten
    Filtered_pdots_list0 = Pdots_flatten
    Filtered_DMs_list0 = DMs_flatten
    Filtered_SNRs_list0 = SNRs_flatten
    Filtered_Indices_list0 = Indices_flatten

    # Initialize mask
    mask = np.ones(len(Filtered_period_list0), dtype=bool)

    # Group containers
    uniq_groups = []
    
    i = 0
    # timing inner loop separately
    loop_start = time.time()
    while mask.any():
        iter_start = time.time()
        first_idx = np.flatnonzero(mask)[0] # Returns the first index where the mask is True
        indices = np.where(
            is_harmonically_related_vectorized(
                Filtered_period_list0[first_idx], Filtered_period_list0,
                Filtered_DMs_list0[first_idx], Filtered_DMs_list0,
                max_harm, period_tol_harm, DM_slope, DM_intercept
            ) & mask
        )[0]
        i = i + 1
        iter_end = time.time() 
        print(i)
        total_time = iter_end - iter_start
        print(total_time)
        uniq_groups.append(indices)
        mask[indices] = False

    loop_end = time.time()
    print(f"Grouping loop took {loop_end - loop_start:.2f} seconds")

    # Step 1: Build replacement map (global index ->  new period)
    replacement_map = {}
    for group in uniq_groups:
        group_snrs = Filtered_SNRs_list0[group]
        max_idx = group[np.argmax(group_snrs)]
        max_period = Filtered_period_list0[max_idx]
        for idx in group:
            replacement_map[idx] = max_period

    # Step 2: Apply replacements
    updated_periods = Filtered_period_list0.copy()
    for idx, new_period in replacement_map.items():
        updated_periods[idx] = new_period # The list of all the new periods (essentially harmonics)

    # Step 3: Unsort back to original candidate order
    #unsorted_periods = np.empty_like(Periods.flatten())
    #unsorted_periods[index] = np.nan  # start with NaNs
    #unsorted_periods[index[:len(updated_periods)]] = updated_periods
    
    #print(len(Filtered_Indices_list0), n_candidates, len(updated_periods))
    #unsorted_periods = np.full(n_candidates*len(files_to_process), np.nan)

    unsorted_periods = np.full(total_cands, np.nan)
    unsorted_periods[Filtered_Indices_list0] = updated_periods

    # reshaped_periods = unsorted_periods.reshape((n_candidates, len(files_to_process)))

    # Step 4: Write per-beam updated files
    # Precompute split indices from n_cands
    split_idx = np.cumsum(n_cands)[:-1]
    period_chunks = np.split(unsorted_periods, split_idx)

    # print(files_to_process)

    for j, file in enumerate(updated_files_to_process):
        input_path = os.path.join(input_dir, f"{file}")
        new_name = file.replace("_candidates.txt", "_harmonic_removed_candidates.txt", 1)
        output_path = os.path.join(output_dir, new_name)

        # Read input file
        try:
            with open(input_path, 'r') as f:
                lines = f.readlines()
        except FileNotFoundError:
            print(f"Input file {input_path} not found.")
            continue

        if len(lines) == 1 and "No valid data found to process." in lines[0]:
            with open(output_path, 'w') as f_out:
                f_out.write("No valid data found to process.\n")
            print(f"No valid data found. Exiting. Response written to {output_path}")
            continue

        try:
            data = np.loadtxt(input_path, dtype=float, skiprows=1)
            if data.size == 0:
                raise ValueError("No valid candidate data found in the file.")
        except Exception as e:
            with open(output_path, 'w') as f_out:
                f_out.write("No valid data found to process.\n")
            print(f"Error reading input file: {e}. Response written to {output_path}")
            continue

        
        # data = np.loadtxt(input_path, skiprows=1)

        # Replace first column with the chunk belonging to this file

        print("Data obtained", period_chunks[j])
        print("Data actual", data[:, 0])

        data[:, 0] = period_chunks[j]

        # preserve header
        with open(input_path) as f:
            header = f.readline()

        print(input_path)

        np.savetxt(output_path, data, fmt="%.10f", header=header.strip(), comments='')
        print(f"Written {output_path}")

    end_time = time.time()
    print(f"Harmonic_elimination completed in {end_time - start_time:.2f} seconds") 
    
# def main():

#     input_dir = "/lustre_data/spotlight/data/PULSELINE_OUTPUT_DATA/TEST"
#     output_dir = "/lustre_data/spotlight/data/PULSELINE_OUTPUT_DATA/TEST"
#     input_file_dir = "/lustre_data/spotlight/data/pulsar_search_dev_run_files/pulseline_run_files/TEST"

#     harmonic_elimination(input_dir, output_dir, input_file_dir)

# if __name__ == "__main__":
#     main()

