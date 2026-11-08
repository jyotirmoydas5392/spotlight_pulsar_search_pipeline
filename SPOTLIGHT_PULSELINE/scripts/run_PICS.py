#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import os
import sys
import glob
import logging
import argparse
import cPickle

# --------------------------------------------------
# Logging
# --------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s"
)

# --------------------------------------------------
# Environment check
# --------------------------------------------------
base_dir = "/lustre_data/spotlight/data/pulsar_fft_pipe_ver1"

sys.path.insert(0, base_dir)

# --------------------------------------------------
# Imports
# --------------------------------------------------
import ubc_AI
from ubc_AI.data import pfdreader

# --------------------------------------------------
# Read configuration
# --------------------------------------------------
sys.path.insert(
    0,
    os.path.join(base_dir, "input_file_dir_init/scripts")
)

from read_input_file_dir import load_parameters

config_file_path = os.path.join(
    base_dir,
    "input_file_dir_init/input_dir/input_file_directory.txt"
)

if not os.path.exists(config_file_path):
    logging.error(
        "Configuration file not found: %s"
        % config_file_path
    )
    sys.exit(1)

params = load_parameters(config_file_path)

model_file = params.get("PICS_model_file")

if not model_file:
    logging.error(
        "PICS_model_file not specified in configuration."
    )
    sys.exit(1)

model_name = os.path.splitext(model_file)[0]


# --------------------------------------------------
# Score AR files
# --------------------------------------------------
def score_ar_files(input_dir, output_dir, node_alias):

    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)

    # Only classify files belonging to this node
    ar_files = sorted(
        glob.glob(
            os.path.join(input_dir, "*%s*.ar" % node_alias)
        )
    )

    if len(ar_files) == 0:
        logging.warning(
            "No .ar files found for node '%s' in %s"
            % (node_alias, input_dir)
        )
        return

    AI_PATH = os.path.dirname(ubc_AI.__file__)
    MODEL_DIR = os.path.join(AI_PATH, "trained_AI")

    model_path = os.path.join(
        MODEL_DIR,
        model_file
    )

    logging.info("----------------------------------------")
    logging.info("Loading classifier")
    logging.info(model_path)
    logging.info("----------------------------------------")

    import traceback

    try:
        with open(model_path, "rb") as f:
            clf = cPickle.load(f)

    except Exception:
        traceback.print_exc()
        raise

    output_file = os.path.join(
        output_dir,
        "%s_%s.txt" % (
            model_name,
            node_alias
        )
    )

    logging.info(
        "Found %d AR files for node %s"
        % (
            len(ar_files),
            node_alias
        )
    )

    fout = open(output_file, "w")

    n_success = 0
    n_failed = 0

    for i, ar_file in enumerate(ar_files):

        try:

            ar_data = pfdreader(ar_file)

            score = clf.report_score([ar_data])[0]

            fout.write(
                "%s %.6f\n"
                % (
                    os.path.basename(ar_file),
                    score
                )
            )

            n_success += 1

        except Exception as e:

            logging.error(
                "Failed: %s (%s)"
                % (
                    os.path.basename(ar_file),
                    str(e)
                )
            )

            n_failed += 1

        if (i + 1) % 100 == 0:
            logging.info(
                "Processed %d / %d"
                % (
                    i + 1,
                    len(ar_files)
                )
            )

    fout.close()

    logging.info("----------------------------------------")
    logging.info("Classification completed")
    logging.info("Node          : %s" % node_alias)
    logging.info("Model         : %s" % model_name)
    logging.info("Total files   : %d" % len(ar_files))
    logging.info("Successful    : %d" % n_success)
    logging.info("Failed        : %d" % n_failed)
    logging.info("Output file   : %s" % output_file)
    logging.info("----------------------------------------")


# --------------------------------------------------
# Main
# --------------------------------------------------
def main():

    parser = argparse.ArgumentParser(
        description="Run PICS classifier on AR files"
    )

    parser.add_argument(
        "input_dir",
        help="Directory containing .ar files"
    )

    parser.add_argument(
        "output_dir",
        help="Directory where results will be written"
    )

    parser.add_argument(
        "node_alias",
        help="Node alias used to select AR files"
    )

    args = parser.parse_args()

    score_ar_files(
        args.input_dir,
        args.output_dir,
        args.node_alias
    )


# --------------------------------------------------
# Entry
# --------------------------------------------------
if __name__ == "__main__":
    main()