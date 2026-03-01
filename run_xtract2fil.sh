#!/bin/bash

# Automatically locate inp_obs_dir.txt relative to this script
inp_file="inp_obs_dirs.txt"

# Check if input file exists
if [[ ! -f "$inp_file" ]]; then
    echo "Error: $inp_file not found!"
    exit 1
fi

# Loop through each observation path
while IFS= read -r obs_path; do
    # Skip empty or commented lines
    [[ -z "$obs_path" || "$obs_path" =~ ^# ]] && continue

    # Strip out anything after a space or parenthesis (like '(Marked)')
    obs_path=$(echo "$obs_path" | awk '{print $1}')

    echo "Processing: $obs_path"

    # Check if FilData exists on remote host
    if ssh rggpu42 "[ -d \"$obs_path/FilData\" ]"; then
        echo "FilData already exists in $obs_path"
    else
        echo "FilData does not exist in $obs_path — creating and processing..."

        # Run commands remotely via bash -c to allow brace expansion
        ssh -t rggpu42 bash -c "'
            set -e  # Exit on error

            # Source your environment (adjust this if 'SOURCE' is a script)
            source /lustre_archive/apps/tdsoft/env.sh

            # Go to BeamData folder
            cd \"$obs_path/BeamData\" || { echo \"BeamData not found at $obs_path\"; exit 1; }

            # Print current working directory for verification
            echo \"Current directory on rggpu42:\"
            pwd

            # Loop over all .log files
            for log in *.log; do
                name=\${log%.log}
                echo \"Running xtract2fil for \$name ...\"

                # Ensure FilData exists
                mkdir -p \"$obs_path/FilData\"

                # Run xtract2fil with proper brace expansion
                xtract2fil \${name}.raw.{0..15} --no-dual \
                    --output \"$obs_path/FilData/\$name\" \
                    --nbeams 10 --njobs 32

                # Copy .ahdr files to FilData
                cp \${name}*.ahdr ../FilData/\${name}
            done
        '"
    fi

    echo "----------------------------------"
done < "$inp_file"

