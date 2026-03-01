import os
import shutil
import subprocess

# ----------------------------------------------------------------------
# Utility functions
# ----------------------------------------------------------------------

def clean_output_dir(output_dir):
    """Removes all files in output_dir without deleting subdirectories."""
    if os.path.exists(output_dir):
        for item in os.listdir(output_dir):
            item_path = os.path.join(output_dir, item)
            if os.path.isfile(item_path):
                os.remove(item_path)


def extract_pfd_basenames(txt_file):
    """Extract base names of PFD/AR files from candidate list."""
    basenames = set()

    if not os.path.exists(txt_file):
        print("Warning: %s not found!" % txt_file)
        return basenames

    with open(txt_file, "r") as f:
        try:
            next(f)  # skip header
        except StopIteration:
            return basenames

        for line in f:
            parts = line.strip().split(",")
            if parts:
                pfd_path = parts[0].strip()
                basenames.add(os.path.basename(pfd_path))

    return basenames


def copy_matching_pfd_files(source_dir, target_dir, pfd_basenames):
    """Copy files whose names contain any pfd basename."""
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)

    copied = 0
    for fname in os.listdir(source_dir):
        for pfd in pfd_basenames:
            if pfd in fname:
                shutil.copy(
                    os.path.join(source_dir, fname),
                    os.path.join(target_dir, fname)
                )
                copied += 1
                break
    return copied


def process_pfd_candidate_files(input_dir, output_dir):
    """Process candidates.txt and candidates.txt.negative"""
    pos_dir = os.path.join(output_dir, "positive_candidates")
    neg_dir = os.path.join(output_dir, "negative_candidates")

    for d in (pos_dir, neg_dir):
        if not os.path.exists(d):
            os.makedirs(d)
        else:
            for f in os.listdir(d):
                os.remove(os.path.join(d, f))

    pos_pfds = extract_pfd_basenames(os.path.join(output_dir, "candidates.txt"))
    neg_pfds = extract_pfd_basenames(os.path.join(output_dir, "candidates.txt.negative"))

    pos_copied = copy_matching_pfd_files(input_dir, pos_dir, pos_pfds)
    neg_copied = copy_matching_pfd_files(input_dir, neg_dir, neg_pfds)

    print("Total Positive Candidates :", len(pos_pfds))
    print("Total Negative Candidates :", len(neg_pfds))
    print("Copied Positive Files     :", pos_copied)
    print("Copied Negative Files     :", neg_copied)


# ----------------------------------------------------------------------
# GHVFDT classifier wrapper (PYTHON 2 SAFE)
# ----------------------------------------------------------------------

def GHVFDT_classifier_cmds(input_dir, output_dir, python_path, ml_path):

    score_script = os.path.join(
        ml_path,
        "PulsarProcessingScripts",
        "src",
        "CandidateScoreGenerators",
        "ScoreGenerator.py"
    )

    score_output = os.path.join(output_dir, "scores.arff")

    score_command = (
        "%s %s -c %s -o %s --pfd --arff --dmprof"
        % (python_path, score_script, input_dir, score_output)
    )

    ml_jar = os.path.join(
        ml_path,
        "HTRU_CLASSIFIER_STUFF",
        "ML.jar"
    )

    model_path = os.path.join(
        ml_path,
        "HTRU_CLASSIFIER_STUFF",
        "DT_LOTAAS.model"
    )

    candidates_output = os.path.join(output_dir, "candidates.txt")

    java_command = (
        "java -jar %s -v -m%s -o%s -p%s -a1"
        % (ml_jar, model_path, candidates_output, score_output)
    )

    clean_output_dir(output_dir)

    try:
        print("Running score generation...")
        subprocess.check_call(score_command, shell=True)

        print("Running Java classifier...")
        subprocess.check_call(java_command, shell=True)

    except subprocess.CalledProcessError as e:
        print("Error executing command:", e)
        return

    process_pfd_candidate_files(input_dir, output_dir)