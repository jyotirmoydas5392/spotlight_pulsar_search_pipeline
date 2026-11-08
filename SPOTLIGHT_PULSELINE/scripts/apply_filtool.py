import os
import sys
import subprocess
import logging

# ----------------------------------------------------------------------
# Logging setup (inherits root logger if already configured)
# ----------------------------------------------------------------------
logger = logging.getLogger(__name__)


def run_filtool_on_fil_list(
    input_dir,
    fil_files,
    filtool_path,
    input_file_dir,
    post_filter,
    workers,
    threads
):
    """
    Run filtool-based RFI mitigation (rfi_filter) on a specific list
    of .fil files inside the given input directory.

    Parameters
    ----------
    input_dir : str
        Directory containing the .fil files
    fil_files : list
        Explicit list of .fil filenames (not full paths)
    filtool_path : str
        Absolute path to the rfi_filter Python script
    input_file_dir : str
        Directory containing the input JSON file required by filtool
    post_filter : int
        1 = overwrite original .fil files
        0 = write RFI-mitigated files separately
    workers : int
        Number of parallel workers
    threads : int
        Threads per filtool run
    """

    # Save the directory from where the pipeline was launched
    launch_dir = os.getcwd()
    logger.info(f"Launch directory: {launch_dir}")

    try:
        # Change to the directory containing the .fil files
        logger.info(f"Changing working directory to input_dir: {input_dir}")
        os.chdir(input_dir)

        # Build the rfi_filter command
        # sys.executable ensures the current conda Python is used
        cmd = [
            sys.executable,
            filtool_path,
            "-l", *fil_files,
            "-pf", str(post_filter),
            "-f", os.path.join(input_file_dir, "filplan.json"),
            "-w", str(workers),
            "-t", str(threads)
        ]

        logger.info("Running command:")
        logger.info(" ".join(cmd))

        # Execute the command
        subprocess.run(cmd, check=True)

        logger.info("RFI cleaning using filtool completed successfully.")

    except subprocess.CalledProcessError:
        logger.error("RFI cleaning using filtool failed.")
        raise

    finally:
        # Always return to the original launch directory
        os.chdir(launch_dir)
        logger.info(f"Returned to launch directory: {launch_dir}")