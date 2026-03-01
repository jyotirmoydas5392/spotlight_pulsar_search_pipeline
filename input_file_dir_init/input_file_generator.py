import os
import sys
import logging
import argparse
import numpy as np

# ========================== SETUP SECTION ==========================

# Get the base directory from the environment variable
base_dir = os.getenv("PULSELINE_DEV_DIR")
if not base_dir:
    logging.error("Error: PULSELINE_DEV_DIR environment variable is not set.")
    sys.exit(1)

# Add required paths to sys.path
required_paths = [
    "input_file_dir_init/scripts",
]
for relative_path in required_paths:
    full_path = os.path.join(base_dir, relative_path)
    if os.path.exists(full_path):
        sys.path.insert(0, full_path)
        logging.info(f"Added to sys.path: {full_path}")
    else:
        logging.warning(f"Path does not exist: {full_path}")

# Import custom module
try:
    from read_input_file_dir import load_parameters
except ImportError as e:
    print("Error importing 'read_input_file_dir'. Ensure the script exists in the specified path.")
    print(e)
    sys.exit(1)

# Check config file
config_file_path = os.path.join(base_dir, "input_file_dir_init/input_dir/input_file_directory.txt")
if not os.path.exists(config_file_path):
    print(f"Configuration file not found at: {config_file_path}")
    sys.exit(1)

# ========================== HELPER FUNCTIONS ==========================

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

def generate_input_files(input_dir, input_file_dir, master_input_file, file_prefix, file_suffix, placeholder, replacement, is_aa, avail_gpu_nodes):
    """Generate input files based on templates."""
    fil_files = sorted(f for f in os.listdir(input_dir) if f.endswith('.fil'))
    num_nodes = len(avail_gpu_nodes)
    toggle = 0
    node_index = 0

    for fil_file in fil_files:
        gpu_id = toggle
        gpu_node = avail_gpu_nodes[node_index]
        abs_filterbank_path = os.path.join(input_dir, fil_file)

        new_file_name = f"{file_prefix}_{fil_file.split('.fil')[0]}_node_{gpu_node}_gpu_id_{gpu_id}{file_suffix}"
        new_file_path = os.path.join(input_file_dir, new_file_name)

        try:
            with open(os.path.join(input_file_dir, master_input_file), 'r') as template_file:
                content = template_file.read()

            updated_content = content.replace(placeholder, replacement.format(abs_filterbank_path))
            if is_aa:
                updated_content = updated_content.replace('selected_card_id 0', f'selected_card_id {gpu_id}')

            with open(new_file_path, 'w') as new_file:
                new_file.write(updated_content)

            print(f"Generated input file at: {new_file_path}")

        except Exception as e:
            print(f"Error processing {fil_file}: {e}")

        toggle = 1 - toggle
        if toggle == 0:
            node_index = (node_index + 1) % num_nodes

# ========================== MAIN FUNCTION ==========================

def main():
    parser = argparse.ArgumentParser(description="Generate AA and Pulseline input files.")

    parser.add_argument("scan_id", type=str, help="Scan ID to use (e.g., TEST)")
    parser.add_argument("data_id", type=str, help="Data ID to use (e.g., 2024A_XXX123)")
    parser.add_argument("band_id", type=str, choices=["BAND3", "BAND4", "BAND5"], help="Band type (BAND3/BAND4/BAND5)")

    args = parser.parse_args()

    scan_id = args.scan_id
    data_id = args.data_id
    data_band_type = args.band_id

    # Load parameters from the configuration file
    try:
        params = load_parameters(config_file_path)
    except Exception as e:
        print("Error loading parameters from the configuration file.")
        print(e)
        sys.exit(1)

    # Get the search type parameter
    search_type = params.get("search_type")
    
    if search_type not in [0, 1]:
        print(f"Invalid search_type '{search_type}' in configuration file. Should be 0 (periodicity) or 1 (acceleration).")
        sys.exit(1)

    # Get the directories initialised
    aa_input_file_dir = params.get('aa_input_file_dir')
    pulseline_input_file_dir = params.get('pulseline_input_file_dir')
    avail_gpus_file_dir = params.get('avail_gpus_file_dir')
    input_filterbank_dir = os.path.join(params.get("raw_input_base_dir"), scan_id, params.get("fil_flag"), data_id)

    avail_gpu_nodes = np.loadtxt(os.path.join(avail_gpus_file_dir, 'avail_gpu_nodes.txt'), dtype=str)

    clear_directory(aa_input_file_dir, 'AA')
    clear_directory(pulseline_input_file_dir, 'PULSELINE')

    master_file_map = {
        0: {"BAND3": "aa_periodicity_master_400_MHz.txt", "BAND4": "aa_periodicity_master_650_MHz.txt", "BAND5": "aa_periodicity_master_1260_MHz.txt"},
        1: {"BAND3": "aa_acceleration_master_400_MHz.txt", "BAND4": "aa_acceleration_master_650_MHz.txt", "BAND5": "aa_acceleration_master_1260_MHz.txt"}
    }

    try:
        master_input_file = master_file_map[search_type][data_band_type]
        generate_input_files(
            input_dir=input_filterbank_dir,
            input_file_dir=aa_input_file_dir,
            master_input_file=master_input_file,
            file_prefix='AA',
            file_suffix='.txt',
            placeholder='file /test/Test.fil',
            replacement='file {}',
            is_aa=True,
            avail_gpu_nodes=avail_gpu_nodes
        )
    except KeyError:
        print("Invalid search_type or band_type combination.")
        sys.exit(1)

    master_files = {
        "BAND3": "pulseline_master_400_MHz.txt",
        "BAND4": "pulseline_master_650_MHz.txt",
        "BAND5": "pulseline_master_1260_MHz.txt"
    }

    if data_band_type in master_files:
        generate_input_files(
            input_dir=input_filterbank_dir,
            input_file_dir=pulseline_input_file_dir,
            master_input_file=master_files[data_band_type],
            file_prefix='PULSELINE',
            file_suffix='.txt',
            placeholder='Test.fil',
            replacement='{}',
            is_aa=False,
            avail_gpu_nodes=avail_gpu_nodes
        )
    else:
        print("Invalid band_type. Choose from BAND3, BAND4, or BAND5.")

# ========================== EXECUTE MAIN ==========================

if __name__ == "__main__":
    main()