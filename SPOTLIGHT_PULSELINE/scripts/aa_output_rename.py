import os
import glob
import struct
import shutil
import numpy as np

def aa_output_rename(input_dir, output_dir, file_name, search_type):
    """
    Renames or copies files in the input directory that match the pattern based on harmonic_sum_flag 
    and saves them in the output directory with a new name based on the DM value.

    :param input_path: Path to the directory containing the original files.
    :param output_path: Path to the directory where renamed/copied files will be saved.
    :param file_name: Base name for the renamed files.
    :param search_type: Type of search (0 for periodicity, 1 for acceleration search).
    :param harmonic_sum_flag: Flag to determine which file pattern to use in acceleration search.
    """

    if search_type == 0:
        file = os.path.join(input_dir, "global_periods.dat")
        if not os.path.exists(file):
            info_file = os.path.join(output_dir, "information.txt")
            with open(info_file, "w") as f:
                f.write("No output files found in the input directory.")
            print("No output files found. Information note saved.")
            return
        
        if os.path.getsize(file) == 0:
            info_file = os.path.join(output_dir, "information.txt")
            with open(info_file, "w") as f:
                f.write("No valid data found in the input directory.")
            print("No valid data found. Information note saved.")
            return

        # Define format: Assuming each row has 4 float values (DM, Period, Pdot, SNR)
        data_format = "4f"  # 4 floats per row

        with open(file, "rb") as f:
            binary = f.read()
            all_data = np.array(list(struct.iter_unpack(data_format, binary)), dtype=float)
        
        # Extract the DM array
        DM_values = np.unique(np.round(all_data[:, 0], 2)) 

        # Reshape the array for value extraction
        all_reshaped_data = all_data.reshape(-1, 4)

        for dm in DM_values:
            # Filter rows where DM matches
            mask = np.isclose(all_reshaped_data[:, 0], dm)  # Find matching DM values
            filtered_all_reshaped_data = all_reshaped_data[mask, 1:]  # Extract last three columns

            # Construct the new file name
            new_file_name = f"{file_name.replace('.fil', '')}_DM{dm:.2f}.dat"
            new_file_path = os.path.join(output_dir, new_file_name)

            # Save to text file without header
            np.savetxt(new_file_path, filtered_all_reshaped_data, fmt="%.15f")

            print(f"Saved {filtered_all_reshaped_data.shape[0]} rows to {new_file_path}")

        print(f"Renaming and saving all the periodicity output for further processing for file {file_name} complete....")

    elif search_type == 1:
        all_files = glob.glob(os.path.join(input_dir, "acc_list*dat"))
        
        if not all_files:
            info_file = os.path.join(output_dir, "information.txt")
            with open(info_file, "w") as f:
                f.write("No output files found in the input directory.")
            print("No output files found. Information note saved.")
            return

        valid_files = [f for f in all_files if os.path.getsize(f) > 0]
        
        if not valid_files:
            info_file = os.path.join(output_dir, "information.txt")
            with open(info_file, "w") as f:
                f.write("Matching files not found in the input directory.")
            print("Matching files not found. Information note saved.")
            return

        for file in valid_files:
            try:
                DM_value = "{:.2f}".format(float(file.split("acc_list_")[1].split(".dat")[0].replace("harm_", "")))

                new_file_name = f"{file_name.replace('.fil', '')}_DM{DM_value}.dat"
                new_file_path = os.path.join(output_dir, new_file_name)

                shutil.move(file, new_file_path)
                print(f"Moved {file} to {new_file_path}")

            except (IndexError, ValueError) as e:
                print(f"Error processing file {file}: {e}")

        print(f"Renaming and saving all the acceleration output for further processing for file {file_name} complete....")
    
    else:
        print("Select appropriate search type flag.")