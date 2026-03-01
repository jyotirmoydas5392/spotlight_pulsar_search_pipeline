import os
import subprocess
from concurrent.futures import ProcessPoolExecutor, as_completed

def convert_ps_to_png_with_rotation(ps_file, png_file):
    """
    Converts a PS file to a PNG file with a 270-degree rotation using Ghostscript.
    :param ps_file: Path to the input PS file.
    :param png_file: Path to the output PNG file.
    """
    try:
        # Ghostscript command to convert PS to PNG with 270-degree rotation
        command = [
            "gs",
            "-dBATCH",                        # Batch mode: exit after processing files
            "-dNOPAUSE",                      # Disable pause after each page
            "-dSAFER",                        # Restrict file operations for safety
            "-dAutoRotatePages=/None",        # Prevent auto-rotation of pages
            "-sDEVICE=png16m",                # Use the 24-bit color PNG output device
            "-r360",                          # Set resolution to 360 DPI
            f"-sOutputFile={png_file}",       # Specify the output file
            "-c", "<</Orientation 3>> setpagedevice",  # Rotate 270 degrees (90 degrees counterclockwise)
            "-f", ps_file                     # Specify the input PostScript file
        ]

        # Run the Ghostscript command
        subprocess.run(command, check=True)
        print(f"Successfully converted {ps_file} to {png_file} with a 270-degree rotation")

    except subprocess.CalledProcessError as e:
        print(f"Error during conversion: {e}")
    except FileNotFoundError:
        print("Ghostscript (gs) is not installed or not found in the system path.")
    except Exception as e:
        print(f"Unexpected error during conversion of {ps_file}: {e}")

def batch_convert_ps_to_png(input_dir, output_dir, workers, keyword):
    """
    Batch converts PS files in the input directory to PNG files in the output directory based on the keyword.
    :param input_dir: Directory containing PS files.
    :param output_dir: Directory where PNG files will be saved.
    :param workers: Number of parallel workers for processing.
    :param keyword: Keyword to search for in PS file names.
    """
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Prepare list of PS files to process based on the keyword in the file name
    ps_files = [
        (os.path.join(input_dir, filename), os.path.join(output_dir, f"{os.path.splitext(filename)[0]}.png"))
        for filename in os.listdir(input_dir)
        if filename.endswith('.ps') and keyword in filename  # Check for keyword in filename
    ]

    if not ps_files:
        print(f"No PS files found containing the keyword '{keyword}' in {input_dir}.")
        return

    # Use ProcessPoolExecutor for parallel processing
    with ProcessPoolExecutor(max_workers=workers) as executor:
        # Submit tasks to the executor
        futures = {executor.submit(convert_ps_to_png_with_rotation, ps_file, png_file): (ps_file, png_file) for ps_file, png_file in ps_files}

        # Process completed tasks
        for future in as_completed(futures):
            ps_file, png_file = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"Error converting {ps_file}: {e}")

    print("Batch conversion of PS to PNG completed.")