#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import os
import sys
import glob
import shutil
import logging
import argparse
import cPickle

# --------------------------------------------------
# Logging
# --------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s: %(message)s'
)

# --------------------------------------------------
# Environment check
# --------------------------------------------------
base_dir = os.getenv("PULSELINE_DEV_DIR")
if not base_dir:
    logging.error("PULSELINE_DEV_DIR environment variable is not set.")
    sys.exit(1)

sys.path.insert(0, base_dir)

# --------------------------------------------------
# Imports after path setup
# --------------------------------------------------
import ubc_AI
from ubc_AI.data import pfdreader

# --------------------------------------------------
# SINGLE SOURCE OF TRUTH FOR MODELS
# --------------------------------------------------
PICS_MODELS = {
    "BD":      "clfl2_BD.pkl",
    "FL":      "clfl2_FL.pkl",
    "HTRU":    "clfl2_HTRU.pkl",
    "HTRU_0":  "clfl2_HTRU_0.pkl",
    "HTRU_1":  "clfl2_HTRU_1.pkl",
    "HTRU_2":  "clfl2_HTRU_2.pkl",
    "PALFA":   "clfl2_PALFA.pkl",
}

# --------------------------------------------------
# Read a classifier output file
# --------------------------------------------------
def read_classifier_file(filepath):
    results = {}

    if not os.path.exists(filepath):
        return results

    with open(filepath, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            parts = line.split()
            if len(parts) != 2:
                continue

            try:
                results[parts[0]] = float(parts[1])
            except ValueError:
                pass

    return results

# --------------------------------------------------
# Generate final max-probability result
# --------------------------------------------------
def generate_final_results(output_dir):
    combined = {}

    for tag in PICS_MODELS.keys():
        fname = os.path.join(output_dir, "clfresult_%s.txt" % tag)
        res = read_classifier_file(fname)

        for ar, prob in res.iteritems():
            if ar not in combined:
                combined[ar] = prob
            else:
                combined[ar] = max(combined[ar], prob)

    final_path = os.path.join(output_dir, "final_clfresult.txt")
    fout = open(final_path, "w")

    for ar in sorted(combined.keys()):
        fout.write("%s %s\n" % (ar, combined[ar]))

    fout.close()
    logging.info("Final combined result written to %s" % final_path)

    return final_path

# --------------------------------------------------
# Split candidates & copy AR + matching files
# --------------------------------------------------
def split_and_copy_candidates(final_file, ar_dir, output_dir, threshold):

    pos_dir = os.path.join(output_dir, "positive_candidates")
    neg_dir = os.path.join(output_dir, "negative_candidates")

    for d in [pos_dir, neg_dir]:
        if not os.path.isdir(d):
            os.makedirs(d)

    pos_count = 0
    neg_count = 0

    with open(final_file, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            parts = line.split()
            if len(parts) != 2:
                continue

            ar_name = parts[0]
            try:
                prob = float(parts[1])
            except ValueError:
                continue

            # full AR path
            ar_path = os.path.join(ar_dir, ar_name)
            if not os.path.exists(ar_path):
                logging.warning("Missing AR file: %s" % ar_path)
                continue

            # basename without .ar
            base = os.path.splitext(ar_name)[0]

            # match ALL files with same base name
            pattern = os.path.join(ar_dir, base + ".*")
            matching_files = glob.glob(pattern)

            if not matching_files:
                logging.warning("No matching files for base: %s" % base)
                continue

            target_dir = pos_dir if prob >= threshold else neg_dir

            for src in matching_files:
                dst = os.path.join(target_dir, os.path.basename(src))
                shutil.copy2(src, dst)

            if prob >= threshold:
                pos_count += 1
            else:
                neg_count += 1

    logging.info("Positive candidates: %d" % pos_count)
    logging.info("Negative candidates: %d" % neg_count)

# --------------------------------------------------
# Main scoring function
# --------------------------------------------------
def score_ar_files(input_dir, output_dir, threshold):
    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)

    ar_files = glob.glob(os.path.join(input_dir, "*.ar"))
    if not ar_files:
        logging.warning("No .ar files found in %s" % input_dir)
        return

    AI_PATH = os.path.dirname(ubc_AI.__file__)
    MODEL_DIR = os.path.join(AI_PATH, "trained_AI")

    for tag, model_file in PICS_MODELS.iteritems():
        model_path = os.path.join(MODEL_DIR, model_file)
        out_file = os.path.join(output_dir, "clfresult_%s.txt" % tag)

        logging.info("Running model: %s" % tag)

        try:
            clf = cPickle.load(open(model_path, "rb"))
        except Exception as e:
            logging.error("Failed to load %s: %s" % (model_file, str(e)))
            continue

        fout = open(out_file, "w")

        for ar_file in ar_files:
            try:
                ar_data = pfdreader(ar_file)
                score = clf.report_score([ar_data])[0]
                fout.write("%s %s\n" % (os.path.basename(ar_file), score))
            except Exception as e:
                logging.error("%s failed on %s: %s"
                              % (tag, os.path.basename(ar_file), str(e)))

        fout.close()

    final_file = generate_final_results(output_dir)
    split_and_copy_candidates(final_file, input_dir, output_dir, threshold)

# --------------------------------------------------
# CLI
# --------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Run PICS classifiers and split candidates (Python 2)"
    )
    parser.add_argument("input_dir", help="Directory with .ar files")
    parser.add_argument("output_dir", help="Output directory")
    parser.add_argument("threshold", type=float, help="Probability threshold for positive candidates (default: 0.5)")

    args = parser.parse_args()
    score_ar_files(args.input_dir, args.output_dir, args.threshold)

if __name__ == "__main__":
    main()