#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import os
import sys
import argparse
import logging

# ------------------------------------------------------------------
# Environment sanity (IMPORTANT for psrchive)
# ------------------------------------------------------------------
os.environ["PYTHONNOUSERSITE"] = "1"
os.environ["PYTHONPATH"] = (
    "/usr/local/lib/python2.7/site-packages:"
    "/lustre_archive/apps/tdsoft/usr/src/presto_old/lib/python"
)
os.environ["LD_LIBRARY_PATH"] = (
    "/usr/local/lib:" +
    os.environ.get("LD_LIBRARY_PATH", "")
)

# Get the base directory from the environment variable
base_dir = "/lustre_data/spotlight/data/pulsar_fft_pipe_ver1"

# Add required paths to sys.path
required_paths = [
    "SPOTLIGHT_PULSELINE/scripts"
]
for relative_path in required_paths:
    full_path = os.path.join(base_dir, relative_path)
    if os.path.exists(full_path):
        sys.path.insert(0, full_path)
        logging.info("Added to sys.path: %s", full_path)
    else:
        logging.warning("Path does not exist: %s", full_path)


# ------------------------------------------------------------------
# Import PICS function
# ------------------------------------------------------------------
try:
    from run_PICS import score_ar_files
except ImportError as e:
    print("Error importing run_PICS: %s" % str(e))
    sys.exit(1)


# ------------------------------------------------------------------
# Main logic
# ------------------------------------------------------------------
def run_classifier(
    input_dir,
    output_dir,
    node_alias
):

    if not os.path.isdir(input_dir):
        print("Input directory does not exist:")
        print(input_dir)
        sys.exit(1)

    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)

    print("----------------------------------------")
    print("Running PICS classifier")
    print("----------------------------------------")

    try:

        score_ar_files(
            input_dir,
            output_dir,
            node_alias
        )

    except Exception as e:

        print("Error running PICS classifier:")
        print(str(e))
        sys.exit(1)

    print("")
    print("----------------------------------------")
    print("PICS classifier completed successfully.")
    print("----------------------------------------")


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------
def main():

    parser = argparse.ArgumentParser(
        description="Run PICS classifier"
    )

    parser.add_argument(
        "input_dir",
        help="Directory containing folded .ar files"
    )

    parser.add_argument(
        "output_dir",
        help="Directory for classifier outputs"
    )

    parser.add_argument(
        "node_alias",
        help="Node alias (e.g. node001)"
    )

    args = parser.parse_args()

    run_classifier(
        args.input_dir,
        args.output_dir,
        args.node_alias
    )


# ------------------------------------------------------------------
# Entry point
# ------------------------------------------------------------------
if __name__ == "__main__":
    main()