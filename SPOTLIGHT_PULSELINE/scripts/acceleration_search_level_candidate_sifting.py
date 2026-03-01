import numpy as np
import shutil
import math
import os

def consecutive(data, stepsize=1):
    """Identify consecutive groups in an array."""
    return np.split(data, np.where(np.diff(data) != stepsize)[0] + 1)

def prepare_dm_array(start_DM, end_DM, dm_step):
    """Prepare DM array based on start, end, and step."""
    N0 = int(math.floor((end_DM - start_DM) / dm_step))
    dm1 = ["{:.2f}".format(start_DM + i * dm_step) for i in range(N0)]
    DM_array = np.array(dm1, dtype=str)
    return DM_array

def remove_duplicate_candidates(file_path):
    """
    Reads a candidate file, removes duplicates based on the 'Period' column, 
    keeps the candidate with the highest SNR, and sorts all entries in descending order of SNR.

    :param file_path: Absolute path to the candidate file.
    """
    try:
        with open(file_path, 'r') as file:
            lines = file.readlines()

        if not lines:
            print(f"File {file_path} is empty. Skipping...")
            return
        
        # The first line is always the header
        header = lines[0]
        data_lines = lines[1:]  # Remaining lines contain data
        
        unique_candidates = {}
        
        for line in data_lines:
            columns = line.split()
            if len(columns) < 4:
                continue  # Skip malformed lines
            
            period, pdot, dm, snr = columns[:4]  # Extract relevant fields
            snr = float(snr)  # Convert SNR to float for comparison
            
            # If period already exists, keep the entry with the highest SNR
            if period in unique_candidates:
                if snr > unique_candidates[period][-1]:  # Last column is SNR
                    unique_candidates[period] = (period, pdot, dm, snr)
            else:
                unique_candidates[period] = (period, pdot, dm, snr)

        # Convert dictionary to sorted list based on highest SNR
        sorted_candidates = sorted(unique_candidates.values(), key=lambda x: -x[-1])  # Sort by SNR descending
        
        # Write back to file
        with open(file_path, 'w') as file:
            file.write(header)  # Write the header first
            for candidate in sorted_candidates:
                file.write(f"{candidate[0]} {candidate[1]} {candidate[2]} {candidate[3]:.2f}\n")  # Write formatted data
        
        print(f"Processed file saved: {file_path}")

    except Exception as e:
        print(f"Error while processing {file_path}: {e}")


def acceleartion_search_level_sift_candidates(input_dir, output_dir, file_name, start_DM, end_DM, low_period, high_period, 
            dm_step, DM_filtering_cut_10, DM_filtering_cut_1000, SNR_cut, period_tol_init_sort, min_harmonic_no):
    """
    Process the candidate data and filter based on the configuration parameters.
    
    Args:
        input_dir (str): The directory where input files are stored.
        output_dir (str): The directory where output files will be saved.
        file_name (str): The base name of the .fil file to process (without extension).
        start_DM (float): The starting DM value.
        end_DM (float): The ending DM value.
        low_period (float): The minimum period in milliseconds.
        high_period (float): The maximum period in milliseconds.
        dm_step (float): The step size for DM.
        DM_filtering_cut (int): The cutoff for DM filtering.
        SNR_cut (float): The SNR threshold for candidates.
        period_tol (float): The tolerance percentage for period filtering.
    """
    # Ensure output path exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate the DM array
    DM_array = prepare_dm_array(start_DM, end_DM, dm_step)
    print(f"Generated DM array: {DM_array}")

    cand_len = []

    for dm in DM_array:
        file_dm_path0 = os.path.join(input_dir, f"{file_name}_DM{float(dm) - dm_step:.2f}.dat")
        file_dm_path1 = os.path.join(input_dir, f"{file_name}_DM{dm}.dat")

        if os.path.exists(file_dm_path1):
            with open(file_dm_path1) as f:
                lines = f.readlines()
            cand_len.append(len(lines))
            print(f"Loaded {len(lines)} candidates for DM={dm}")
        else:
            print(f"File not found for DM={dm}, attempting to copy from previous DM.")
            if os.path.exists(file_dm_path0):
                shutil.copy(file_dm_path0, file_dm_path1)
                try:
                    with open(file_dm_path1) as f:
                        lines = f.readlines()
                    cand_len.append(len(lines))
                    print(f"Loaded {len(lines)} candidates for DM={dm} after copying the previous DM file.")
                except Exception as e:
                    print(f"Failed to read the copied file for DM={dm}: {e}")
            else:
                print(f"Previous DM file {file_dm_path0} also not found. Skipping DM={dm}.")

    max_cand_len = max(cand_len)
    if max_cand_len == 0:
        print("No candidates found in any DM trial. Exiting.")
        return

    print(f"Maximum number of candidates in any DM trial: {max_cand_len}")

    # Initialize arrays for candidate parameters
    Period_dot_array = np.full((len(DM_array), max_cand_len), np.nan)
    Period_array = np.full((len(DM_array), max_cand_len), np.nan)
    SNR_array = np.full((len(DM_array), max_cand_len), np.nan)
    Power_array = np.full((len(DM_array), max_cand_len), np.nan)

    for i in range(0, len(DM_array)):
        data = np.loadtxt(os.path.join(input_dir, f"{file_name}_DM{DM_array[i]}.dat"), dtype=float)
        A0 = np.array(data)

        #  A0[:,0] is z_bin array
        #  A0[:,1] is acceleration array
        #  A0[:,2] is r_bin array
        #  A0[:,3] is frequency array
        #  A0[:,4] is Power array
        #  A0[:,5] is SNR array

        # Deletes all the candidates having zero-frequency, zero SNR and very very high SNR
        A0 = A0[A0[:, 3] != 0]
        A0 = A0[A0[:, 5] > 0]
        A0 = A0[A0[:, 5] < 10**8]
    
        c = 3*10**8
        cand_array_len = len(A0[:,0])
        
        for j in range(0, int(cand_array_len)):
            if low_period <= np.divide(1.0, A0[j][3]) <= high_period:
            
                Period_dot_array[i][j] = np.divide(np.multiply(A0[j][1], np.divide(1.0, A0[j][3])), c)
                Period_array[i][j] = np.divide(np.divide(1.0, A0[j][3])) # In seconds
                Power_array[i][j] = A0[j][4]
                SNR_array[i][j] = A0[j][5]

    print(f"Finished processing candidate data for {len(DM_array)} DM trials.")

    # Unique Period filtering
    Period_flatten = Period_array.flatten()
    filtered_period_list0 = [x for x in np.unique(Period_flatten) if str(x) != 'nan']
    print(filtered_period_list0)
    tot_cand = len(filtered_period_list0)
    uniq_period_list0 = []
    period_tol_array = []

    print(f"Filtered unique Period list length: {len(filtered_period_list0)}")

    while len(filtered_period_list0) > 0:
        indices = np.where(filtered_period_list0 <= filtered_period_list0[0] + period_tol_init_sort / 100.0)
        period_tol_array.append(filtered_period_list0[0] * (period_tol_init_sort / 100.0))
        uniq_period_list0.append(filtered_period_list0[0])
        filtered_period_list0 = np.delete(filtered_period_list0, indices[0])

    # Final filtering and output
    output_file = os.path.join(output_dir, f"{file_name}_all_sifted_candidates.txt")
    with open(output_file, "w") as file:
        file.write("Period(sec)   Pdot(s/s)  DM(pc/cc)   SNR\n")

    print(f"Writing output to {output_file}")

    # Now the sorting begins
    for i, uniq_period in enumerate(uniq_period_list0):
        if i == 0:
            index = np.where(Period_array <= uniq_period + period_tol_array[i] / 2)
        else:
            index = np.where((Period_array > uniq_period_list0[i - 1] + period_tol_array[i - 1] / 2) &
                            (Period_array <= uniq_period + period_tol_array[i] / 2))

        DM_index, cand_index = index[0], index[1]
        DM_groups = consecutive(np.unique(DM_index))

        DM_slope = np.divide(np.subtract(DM_filtering_cut_1000, DM_filtering_cut_10), 0.990)
        DM_intercept = np.subtract(DM_filtering_cut_10, np.multiply(DM_slope, 0.010))

        DM_filtering_cut = int(np.divide(np.add(DM_intercept, np.multiply(DM_slope, Period_array[index[0][0]][index[1][0]])), dm_step))
        
        #print(f"DM slope is {DM_slope} pc/cc, DM intercept is {DM_intercept} pc/cc, and DM filtering cut is {DM_filtering_cut}")
        print(f"For Period {Period_array[index[0][0]][index[1][0]]} sec, DM tolerance is {np.multiply(DM_filtering_cut, dm_step)} pc/cc")

        for group in DM_groups:
            if len(group) >= DM_filtering_cut:
                Filtered_SNR_array = np.full(SNR_array.shape, np.nan)
                for dm_idx in group:
                    DM_cand_indices = np.where(DM_index == dm_idx)
                    Filtered_SNR_array[dm_idx, cand_index[DM_cand_indices]] = SNR_array[dm_idx, cand_index[DM_cand_indices]]

                if np.all(np.isnan(Filtered_SNR_array)):
                    continue

                maxima_index = np.unravel_index(np.nanargmax(Filtered_SNR_array), Filtered_SNR_array.shape)
                if SNR_array[maxima_index] >= SNR_cut:
                    with open(output_file, "a") as file:
                        file.write(f"{Period_array[maxima_index]:.10f}     "
                                   f"{Period_dot_array[maxima_index]:.6e}     "
                                   f"{DM_array[maxima_index[0]]}     "
                                   f"{SNR_array[maxima_index]:.2f}\n")
                    print(f"Candidate: Period={Period_array[maxima_index]:.6f}, "
                          f"Pdot={Period_dot_array[maxima_index]:.6e}, "
                          f"SNR={SNR_array[maxima_index]:.2f}")
                    
    print("Finished processing all candidates and writing the output.")

    # Remove duplicate candidates from the output file
    remove_duplicate_candidates(output_file)

    print("Finished removing the duplicate candidates from the output file.")
