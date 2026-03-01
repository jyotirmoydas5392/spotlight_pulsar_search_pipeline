import os
import sys
import subprocess
from multiprocessing import Pool

def get_header_size(file_path):
    """Get header size in bytes using the 'header' command."""
    try:
        result = subprocess.run(
            ['header', file_path, '-headersize'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True
        )
        return int(result.stdout.strip())
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to get header size: {e.stderr.strip()}")
    except Exception as e:
        raise RuntimeError(f"Error: Could not determine header size - {e}")

def extract_binary_header(file_path, header_file, header_size):
    """Extract binary header using dd."""
    dd_cmd = [
        'dd',
        f'if={file_path}',
        f'of={header_file}',
        'bs=1',
        f'count={header_size}',
        'status=none'
    ]
    subprocess.run(dd_cmd, check=True)

def extract_raw_data(file_path, raw_file, header_size):
    """Extract raw binary data skipping the header using dd."""
    dd_cmd = [
        'dd',
        f'if={file_path}',
        f'of={raw_file}',
        'bs=1M',
        f'skip={header_size}',
        'iflag=skip_bytes,count_bytes',
        'status=none'
    ]
    subprocess.run(dd_cmd, check=True)

def process_single_file(args):
    """Process one .fil file: extract header and raw, then delete .fil if successful."""
    file_name, input_dir, output_dir = args
    file_path = os.path.join(input_dir, file_name)

    if not os.path.isfile(file_path):
        return f"Error: File '{file_path}' not found."

    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    base_name = os.path.splitext(file_name)[0]
    header_file = os.path.join(output_dir, base_name + ".hdr")
    raw_file = os.path.join(output_dir, base_name + ".raw")

    try:
        header_size = get_header_size(file_path)
        extract_binary_header(file_path, header_file, header_size)
        extract_raw_data(file_path, raw_file, header_size)

        # Only delete original .fil if both .hdr and .raw files exist
        if os.path.isfile(header_file) and os.path.isfile(raw_file):
            os.remove(file_path)
            return f"Processed {file_name} successfully and deleted original."
        else:
            missing = []
            if not os.path.isfile(header_file):
                missing.append(".hdr")
            if not os.path.isfile(raw_file):
                missing.append(".raw")
            missing_str = " and ".join(missing)
            return f"Processed {file_name} but missing {missing_str} file(s). Original .fil file NOT deleted."
    except Exception as e:
        return f"Failed {file_name}: {e}"

def process_fil_files_parallelly(fil_files, input_dir, output_dir, workers):
    args_list = [(f, input_dir, output_dir) for f in fil_files]
    with Pool(processes=workers) as pool:
        results = pool.map(process_single_file, args_list)
    for res in results:
        print(res)

if __name__ == '__main__':
    if len(sys.argv) != 5:
        print(f"Usage: python3 {sys.argv[0]} <input_dir> <output_dir> <fil1,fil2,...> <num_workers>")
        sys.exit(1)

    input_dir = sys.argv[1]
    output_dir = sys.argv[2]
    fil_files = sys.argv[3].split(",")
    workers = int(sys.argv[4])

    process_fil_files_parallelly(fil_files, input_dir, output_dir, workers)