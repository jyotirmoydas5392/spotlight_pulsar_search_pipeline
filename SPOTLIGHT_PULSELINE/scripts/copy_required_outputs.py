import numpy as np
import shutil
import os

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