import os
import sys
import shutil
import subprocess
from multiprocessing import Pool

def should_skip_beam(status_file_path):
    if os.path.exists(status_file_path):
        with open(status_file_path, "r") as sf:
            return sf.read().strip() == "successful"
    return False

def copy_config_file(input_file_dir, input_dir, band_id, base_name):
    config_map = {
        "BAND3": "gptool_band3.in",
        "BAND4": "gptool_band4.in",
        "BAND5": "gptool_band5.in",
    }
    if band_id in config_map:
        src = os.path.join(input_file_dir, config_map[band_id])
        dst = os.path.join(input_dir, "gptool.in")
        if os.path.exists(src):
            shutil.copy2(src, dst)
            print(f"[{base_name}] Config copied: {src} -> {dst}")
        else:
            print(f"[{base_name}] WARNING: Config file not found: {src}")
    else:
        print(f"[{base_name}] WARNING: Unknown band ID: {band_id}")

def get_observation_timeout(fil_file, base_name):
    try:
        result = subprocess.run(["header", fil_file, "-tobs"], stdout=subprocess.PIPE, check=True, text=True)
        return int(float(result.stdout.strip()))
    except Exception as e:
        print(f"[{base_name}] WARNING: Could not determine TOBS: {e}. Using default timeout.")
        return 600

def run_gptool(gptool_path, base_name, input_dir, output_dir, timeout, base_log_id):
    cmd = [
        gptool_path, "-f", base_name + ".raw",
        "-nodedisp", "-m", "32", "-t", "4", "-o", output_dir
    ]
    try:
        os.chdir(input_dir)
        subprocess.run(cmd, check=True, timeout=timeout)
        print(f"[{base_log_id}] gptool completed.")
        return "successful"
    except subprocess.TimeoutExpired:
        return "timeout"
    except subprocess.CalledProcessError:
        return "execution_failed"
    finally:
        os.chdir("..")

def create_fil_file(header_path, data_path, fil_path, base_name):
    try:
        with open(fil_path, 'wb') as fout, open(header_path, 'rb') as fhdr, open(data_path, 'rb') as fdata:
            fout.write(fhdr.read())
            fout.write(fdata.read())
        print(f"[{base_name}] .fil created: {fil_path}")
        return os.path.getsize(fil_path) >= 100
    except Exception as e:
        print(f"[{base_name}] ERROR: .fil creation failed: {e}")
        return False

def cleanup_files(base_name, files):
    for f in files:
        if os.path.exists(f):
            os.remove(f)
            print(f"[{base_name}] Deleted: {f}")

def write_status(status_file_path, status, base_name):
    try:
        with open(status_file_path, "w") as f:
            f.write(status + "\n")
        print(f"[{base_name}] Status written: {status}")
    except Exception as e:
        print(f"[{base_name}] WARNING: Failed to write status file: {e}")

def run_gptool_and_process(args):
    file_name, input_dir, input_file_dir, output_dir, gptool_path, data_id, band_id = args
    base_name = os.path.splitext(file_name)[0]

    hdr = os.path.join(input_dir, base_name + ".hdr")
    raw = os.path.join(input_dir, base_name + ".raw")
    gpt_output = os.path.join(output_dir, base_name + ".raw.gpt")
    fil = os.path.join(output_dir, base_name + ".fil")
    status_file = os.path.join(output_dir, f"status_{base_name}.txt")

    if should_skip_beam(status_file):
        print(f"[{base_name}] Already processed. Skipping.")
        return "already_done"

    copy_config_file(input_file_dir, input_dir, band_id, base_name)

    if not os.path.exists(hdr) or not os.path.exists(raw):
        print(f"[{base_name}] ERROR: Missing .hdr or .raw.")
        write_status(status_file, "missing_input", base_name)
        return "missing_input"

    timeout = get_observation_timeout(os.path.join(input_dir, file_name), base_name)
    status = run_gptool(gptool_path, base_name, input_dir, output_dir, timeout, base_name)

    data_to_use = gpt_output if status == "successful" else raw
    fil_ok = create_fil_file(hdr, data_to_use, fil, base_name)

    if fil_ok and status == "successful":
        cleanup_files(base_name, [hdr, raw, gpt_output])
    else:
        print(f"[{base_name}] Skipping cleanup due to invalid .fil or failed execution.")

    final_status = status if fil_ok else "concat_failed"
    write_status(status_file, final_status, base_name)
    return final_status

def process_gptool_parallelly(fil_files, input_dir, input_file_dir, output_dir, gptool_path, data_id, band_id, workers):
    tasks = [
        (file_name, input_dir, input_file_dir, output_dir, gptool_path, data_id, band_id)
        for file_name in fil_files
    ]
    print(f"Starting parallel processing with {workers} workers for {len(fil_files)} files.")
    with Pool(processes=workers) as pool:
        pool.map(run_gptool_and_process, tasks)
    print("Parallel processing completed.")

if __name__ == "__main__":
    if len(sys.argv) != 9:
        print(f"Usage: python3 {sys.argv[0]} <input_dir> <input_file_dir> <output_dir> <gptool_path> <data_id> <band_id> <fil1,fil2,...> <num_workers>")
        sys.exit(1)

    input_dir = sys.argv[1]
    input_file_dir = sys.argv[2]
    output_dir = sys.argv[3]
    gptool_path = sys.argv[4]
    data_id = sys.argv[5]
    band_id = sys.argv[6]
    fil_files = sys.argv[7].split(",")
    workers = int(sys.argv[8])

    print("=== GPTool Processing Script ===")
    print(f"Input directory: {input_dir}")
    print(f"Input file config directory: {input_file_dir}")
    print(f"Output directory: {output_dir}")
    print(f"GPTool path: {gptool_path}")
    print(f"Data ID: {data_id}, Band ID: {band_id}")
    print(f"Files: {fil_files}")
    print(f"Workers: {workers}")

    process_gptool_parallelly(fil_files, input_dir, input_file_dir, output_dir, gptool_path, data_id, band_id, workers)