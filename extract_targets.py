#!/usr/bin/env python3

import os
import csv

BASE = "/lustre_data/spotlight/data"
STATUS_FILE = "obs_per_scan_data_status.txt"

results = []
pulsar_count = 0
unique_pulsars = set()

with open(STATUS_FILE, "r") as f:

    reader = csv.DictReader(f)

    for row in reader:

        if row["Pulsar_search_status"].strip() != "True":
            continue

        obs_id = row["ObsID"].strip()
        obs_dir = row["ActualDirectory"].strip()
        target_dir = row["ScanName"].strip()

        pipe_dir = os.path.join(
            BASE,
            obs_dir,
            "PulsarPipeData",
            target_dir
        )

        if not os.path.isdir(pipe_dir):
            print(f"Missing directory: {pipe_dir}")
            continue

        info_file = None

        for fname in os.listdir(pipe_dir):
            if fname.endswith("_outputs_info.txt"):
                info_file = os.path.join(pipe_dir, fname)
                break

        if info_file is None:
            print(f"No outputs info file found in {pipe_dir}")
            continue

        target_name = ""
        ra = ""
        dec = ""
        band = ""
        freq = ""
        bw = ""

        with open(info_file, "r") as fin:

            for line in fin:

                if line.startswith("Target Name:"):
                    target_name = line.split(":", 1)[1].strip()

                elif line.startswith("Processed RA"):
                    ra = line.split("):", 1)[1].strip()

                elif line.startswith("Processed Dec"):
                    dec = line.split("):", 1)[1].strip()

                elif line.startswith("Observed Band"):
                    band = line.split(":", 1)[1].strip()

                elif line.startswith("Observed Frequency"):
                    freq = line.split(":", 1)[1].strip()

                elif line.startswith("Bandwidth"):
                    bw = line.split(":", 1)[1].strip()

        pulsar_redetection = (
            "Yes"
            if target_name.startswith(("J", "B"))
            else "No"
        )

        if pulsar_redetection == "Yes":
            pulsar_count += 1
            unique_pulsars.add(target_name)

        parts = obs_dir.split("_")

        if len(parts) >= 4:
            obs_date = parts[-2]
            obs_time = parts[-1]
        else:
            obs_date = ""
            obs_time = ""

        results.append([
            obs_id,
            obs_dir,
            obs_date,
            obs_time,
            target_name,
            ra,
            dec,
            band,
            freq,
            bw,
            pulsar_redetection
        ])

csv_file = "all_targets_summary.csv"

with open(csv_file, "w", newline="") as fout:

    writer = csv.writer(fout)

    writer.writerow([
        "Observation_ID",
        "Observation_Directory",
        "Observation_Date",
        "Observation_Time",
        "Target_Name",
        "RA",
        "DEC",
        "Band",
        "Frequency_MHz",
        "Bandwidth_MHz",
        "Pulsar_Redetection"
    ])

    writer.writerows(results)

txt_file = "all_targets_summary.txt"

with open(txt_file, "w") as fout:

    fout.write("PULSAR SEARCH TARGET SUMMARY\n")
    fout.write("=" * 120 + "\n\n")

    fout.write(f"Total Targets Processed : {len(results)}\n")
    fout.write(f"Pulsar Observations     : {pulsar_count}\n")
    fout.write(f"Unique B/J Targets      : {len(unique_pulsars)}\n\n")

    fout.write(
        "{:<24} {:<10} {:<10} {:<25} {:<15} {:<15} {:<8} {:<10} {:<10} {:<5}\n".format(
            "ObservationDirectory",
            "ObsID",
            "Date",
            "Target",
            "RA",
            "DEC",
            "Band",
            "Freq",
            "BW",
            "Pulsar"
        )
    )

    fout.write("-" * 170 + "\n")

    for r in results:

        fout.write(
            "{:<24} {:<10} {:<10} {:<25} {:<15} {:<15} {:<8} {:<10} {:<10} {:<5}\n".format(
                r[1],   # Observation directory
                r[0],   # ObsID
                r[2],   # Date
                r[4],   # Target
                r[5],   # RA
                r[6],   # DEC
                r[7],   # Band
                r[8],   # Freq
                r[9],   # BW
                r[10]   # Pulsar
            )
        )

print("\nDone.")
print(f"Total targets processed : {len(results)}")
print(f"Pulsar observations     : {pulsar_count}")
print(f"Unique B/J targets      : {len(unique_pulsars)}")
print(f"CSV written to          : {csv_file}")
print(f"TXT written to          : {txt_file}")
