#!/bin/bash

INPUT_FILE="inp_obs_dirs.txt"

if [[ ! -f "$INPUT_FILE" ]]; then
    echo "Error: $INPUT_FILE not found!"
    exit 1
fi

while IFS= read -r obs_path || [[ -n "$obs_path" ]]; do
    [[ -z "$obs_path" || "$obs_path" == \#* ]] && continue

    echo "Processing: $obs_path"

    # Check if FilData exists
    if [[ -d "$obs_path/FilData" ]]; then
        echo "FilData already exists in $obs_path — skipping..."
        echo "----------------------------------"
        continue
    fi

    echo "FilData does not exist in $obs_path — creating and processing..."

    ssh rggpu40 <<EOF
    set -e  # Exit immediately if any command fails
    source /lustre_archive/apps/tdsoft/env.sh

    cd "$obs_path" || { echo "Error: Observation path not found"; exit 1; }

    mkdir -p FilData

    cd BeamData || { echo "Error: BeamData directory not found"; rm -rf ../FilData; exit 1; }

    echo "Current directory on rggpu40:"
    pwd

    for log_file in *.log; do
        [[ ! -f "\$log_file" ]] && { echo "Error: No log files found"; rm -rf ../FilData; exit 1; }

        NAME="\${log_file%.log}"
        OUTPUT_DIR="$obs_path/FilData/\$NAME"
        mkdir -p "\$OUTPUT_DIR"

        echo "Running xtract2fil for \$NAME ..."
        if ! xtract2fil "\$NAME".raw.{0..15} --no-dual --output "\$OUTPUT_DIR" --nbeams 10 --njobs 32; then
            echo "❌ Error: xtract2fil failed for \$NAME. Cleaning up..."
            rm -rf "\$OUTPUT_DIR"
            rm -rf "$obs_path/FilData"
            exit 1
        fi

        echo "Copying header files..."
        if ! cp "\$NAME"*.ahdr ../FilData/; then
            echo "⚠️Warning: No .ahdr files found for \$NAME"
        fi

        echo "✅Finished processing \$NAME"
        echo "----------------------------------"
    done
EOF

    if [[ $? -ne 0 ]]; then
        echo "❌Failed to process $obs_path. Cleaning up FilData locally (if any)..."
        ssh rggpu40 "rm -rf '$obs_path/FilData'" >/dev/null 2>&1
    else
        echo "✅Successfully processed $obs_path"
    fi

    echo "----------------------------------"

done < "$INPUT_FILE"

echo "All observations processed."

