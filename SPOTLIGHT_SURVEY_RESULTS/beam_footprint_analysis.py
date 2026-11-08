#!/usr/bin/env python3

import os
import csv
import numpy as np
import pandas as pd

from scipy.spatial import ConvexHull
from scipy.spatial.distance import pdist, squareform


BASE = "/lustre_data/spotlight/data"
STATUS_FILE = "obs_per_scan_data_status.txt"

results = []
retained_beam_rows = []

def spherical_projection(ra, dec):
    """
    Convert RA/Dec from radians to locally projected coordinates.
    Output coordinates are in degrees.
    """

    ra_deg = np.degrees(ra)
    dec_deg = np.degrees(dec)

    dec0 = np.mean(dec_deg)

    x = ra_deg * np.cos(np.radians(dec0))
    y = dec_deg

    return x, y


with open(STATUS_FILE, "r") as f:

    reader = csv.DictReader(f)

    for row in reader:

        if row["Pulsar_search_status"].strip() != "True":
            continue

        obsid = row["ObsID"].strip()
        obsdir = row["ActualDirectory"].strip()
        target = row["ScanName"].strip()

        beamfile = os.path.join(
            BASE,
            obsdir,
            "PulsarPipeData",
            target,
            "folding_outputs",
            "Extracted_RA_Dec_beam_index.txt"
        )

        if not os.path.isfile(beamfile):
            print("Missing:", beamfile)
            continue

        data = []

        with open(beamfile) as fin:

            for line in fin:

                if line.startswith("#"):
                    continue

                if "RA (Rad)" in line:
                    continue

                cols = [x.strip() for x in line.split(",")]

                if len(cols) < 4:
                    continue

                try:

                    ra = float(cols[0])
                    dec = float(cols[1])
                    beam_index = int(cols[2])

                    data.append([ra, dec, beam_index])

                except Exception:
                    pass

        if len(data) < 10:
            continue

        data = np.array(data)

        ra = data[:, 0]
        dec = data[:, 1]

        x, y = spherical_projection(ra, dec)

        points = np.column_stack((x, y))

        # ------------------------------------
        # Estimate beam spacing
        # ------------------------------------

        D = squareform(pdist(points))

        np.fill_diagonal(D, np.inf)

        nearest = np.min(D, axis=1)

        median_spacing = np.median(nearest)

        if median_spacing <= 0:
            continue

        # ------------------------------------
        # Connectivity graph clustering
        # ------------------------------------

        threshold = 2.5 * median_spacing

        adjacency = D < threshold

        n_points = len(points)

        visited = np.zeros(
            n_points,
            dtype=bool
        )

        components = []

        for start in range(n_points):

            if visited[start]:
                continue

            stack = [start]
            component = []

            visited[start] = True

            while stack:

                node = stack.pop()

                component.append(node)

                neighbors = np.where(
                    adjacency[node]
                )[0]

                for nb in neighbors:

                    if not visited[nb]:

                        visited[nb] = True
                        stack.append(nb)

            components.append(component)

        largest_size = max(
            len(c)
            for c in components
        )

        core_mask = np.zeros(
            n_points,
            dtype=bool
        )

        kept_components = 0

        for comp in components:

            if len(comp) >= 0.5 * largest_size:

                core_mask[comp] = True
                kept_components += 1
        core_points = points[core_mask]
        component_sizes = sorted(
            [len(c) for c in components],
            reverse=True
        )

        largest_component_size = component_sizes[0]

        second_component_size = (
            component_sizes[1]
            if len(component_sizes) > 1
            else 0
        )

        component_ratio = (
            second_component_size /
            largest_component_size
        )
        fdx in np.where(core_mask)[0]:

            retained_beam_rows.append([
                obsid,
                obsdir,
                target,
                int(data[idx, 2]),
                np.degrees(ra[idx]),
                np.degrees(dec[idx])
            ])

        n_total = len(points) r idx in np.where(core_mask)[0]:

            retained_beam_rows.append([
                obsid,
     
        n_total = len(points)
        n_core = len(core_points)
        n_removed = n_total - n_core

        if n_core < 4:
            continue
        # ------------------------------------
        # Convex Hull Area
        # ------------------------------------

        try:

            hull = ConvexHull(core_points)

            # In 2D scipy stores area in hull.volume
            hull_area = hull.volume

        except Exception:

            hull_area = np.nan

        # ------------------------------------
        # Derived quantities
        # ------------------------------------

        core_fraction = n_core / n_total

        if np.isfinite(hull_area) and hull_area > 0:
            beam_density = n_core / hull_area
        else:
            beam_density = np.nan

        results.append([
            obsid,
            obsdir,
            target,
            n_total,
            n_core,
            n_removed,
            threshold,
            kept_components,
            largest_component_size,
            second_component_size,
            component_ratio,
            core_fraction,
            hull_area,
            beam_density
        ])
# ====================================
# Create DataFrame
# ====================================
retained_df = pd.DataFrame(
    retained_beam_rows,
    columns=[
        "ObsID",
        "ObservationDir",
        "Target",
        "BeamIndex",
        "BeamRA_deg",
        "BeamDec_deg"
    ]
)

retained_df.to_csv(
    "retained_beams.csv",
    index=False
)
df = pd.DataFrame(
    results,
    columns=[
        "ObsID",
        "ObservationDir",
        "Target",
        "N_total_beams",
        "N_core_beams",
        "N_removed_beams",
        "ConnectionThreshold_deg",
        "KeptComponents",
        "LargestComponent",
        "SecondLargestComponent",
        "ComponentRatio",
        "CoreFraction",
        "ConvexHullArea_deg2",
        "BeamDensity_per_deg2"
    ]
)

# ====================================
# CSV Output
# ====================================

csv_file = "beam_footprint_summary.csv"

df.to_csv(
    csv_file,
    index=False
)

# ====================================
# TXT Output
# ====================================

txt_file = "beam_footprint_summary.txt"

with open(txt_file, "w") as fout:

    fout.write("BEAM FOOTPRINT ANALYSIS SUMMARY\n")
    fout.write("=" * 180 + "\n\n")

    fout.write(f"Observations analysed : {len(df)}\n\n")

    fout.write(
        "{:<10} {:<30} {:>8} {:>8} {:>8} {:>6} {:>8} {:>8} {:>10} {:>12} {:>15} {:>15}\n".format(
            "ObsID",
            "Target",
            "Total",
            "Core",
            "Removed",
            "Keep",
            "L1",
            "L2",
            "Ratio",
            "CoreFrac",
            "HullArea",
            "Density"
        )
    )

    fout.write("-" * 180 + "\n")

    for _, row in df.iterrows():

        fout.write(
            "{:<10} {:<30} {:>8d} {:>8d} {:>8d} {:>6d} {:>8d} {:>8d} {:>10.3f} {:>12.4f} {:>15.6f} {:>15.3f}\n".format(
                row["ObsID"],
                str(row["Target"])[:30],
                int(row["N_total_beams"]),
                int(row["N_core_beams"]),
                int(row["N_removed_beams"]),
                int(row["KeptComponents"]),
                int(row["LargestComponent"]),
                int(row["SecondLargestComponent"]),
                float(row["ComponentRatio"]),
                float(row["CoreFraction"]),
                float(row["ConvexHullArea_deg2"]),
                float(row["BeamDensity_per_deg2"])
            )
        )

    fout.write("\n\n")
    fout.write("=" * 180 + "\n")
    fout.write("GLOBAL STATISTICS\n")
    fout.write("=" * 180 + "\n\n")

    fout.write("Convex Hull Area Statistics\n")
    fout.write(str(df["ConvexHullArea_deg2"].describe()))
    fout.write("\n\n")

    fout.write("Beam Density Statistics\n")
    fout.write(str(df["BeamDensity_per_deg2"].describe()))
    fout.write("\n\n")

    fout.write("Core Fraction Statistics\n")
    fout.write(str(df["CoreFraction"].describe()))
    fout.write("\n")

# ====================================
# Terminal Summary
# ====================================

print("\n====================================")
print("Observations analysed :", len(df))
print("CSV Output            :", csv_file)
print("TXT Output            :", txt_file)
print("====================================\n")

if len(df) > 0:

    print("Area statistics:")
    print(df["ConvexHullArea_deg2"].describe())

    print("\nBeam density statistics:")
    print(df["BeamDensity_per_deg2"].describe())

    print("\nCore fraction statistics:")
    print(df["CoreFraction"].describe())
