#!/usr/bin/env python3

import numpy as np
import pandas as pd
import healpy as hp
import matplotlib.pyplot as plt

# -----------------------------------
# Configuration
# -----------------------------------

NSIDE = 512

HWHM_LOOKUP = {
    "BAND3": 35.98,   # arcmin
    "BAND4": 21.71,
    "BAND5": 12.00
}

# -----------------------------------
# Read inputs
# -----------------------------------

beam_df = pd.read_csv("retained_beams.csv")

band_df = pd.read_csv("obs_band_lookup.csv")

band_lookup = dict(
    zip(
        band_df["Observation_Directory"],
        band_df["Band"]
    )
)

# -----------------------------------
# Create map
# -----------------------------------

npix = hp.nside2npix(NSIDE)

coverage = np.zeros(
    npix,
    dtype=np.int32
)

n_processed = 0

# -----------------------------------
# Paint every retained beam
# -----------------------------------

for _, row in beam_df.iterrows():

    obsdir = row["ObservationDir"]

    band = band_lookup.get(obsdir)

    if band is None:
        continue

    radius_deg = (
        HWHM_LOOKUP[band] / 60.0
    )

    ra = row["BeamRA_deg"]
    dec = row["BeamDec_deg"]

    theta = np.radians(
        90.0 - dec
    )

    phi = np.radians(
        ra
    )

    vec = hp.ang2vec(
        theta,
        phi
    )

    pixels = hp.query_disc(
        NSIDE,
        vec,
        np.radians(radius_deg)
    )

    coverage[pixels] += 1

    n_processed += 1

    if n_processed % 10000 == 0:
        print(
            "Processed",
            n_processed,
            "beams"
        )

# -----------------------------------
# Save FITS map
# -----------------------------------

hp.write_map(
    "coverage_map.fits",
    coverage,
    overwrite=True
)

# -----------------------------------
# Statistics
# -----------------------------------

pixel_area_deg2 = hp.nside2pixarea(
    NSIDE,
    degrees=True
)

covered_pixels = np.sum(
    coverage > 0
)

covered_area_deg2 = (
    covered_pixels *
    pixel_area_deg2
)

sky_fraction = (
    covered_pixels /
    len(coverage)
)

max_overlap = np.max(
    coverage
)

with open(
    "coverage_stats.txt",
    "w"
) as f:

    f.write(
        "Coverage statistics\n"
    )

    f.write(
        "===================\n\n"
    )

    f.write(
        f"NSIDE = {NSIDE}\n"
    )

    f.write(
        f"Retained beams = {len(beam_df)}\n"
    )

    f.write(
        f"Covered pixels = {covered_pixels}\n"
    )

    f.write(
        f"Covered area (deg^2) = {covered_area_deg2:.2f}\n"
    )

    f.write(
        f"Sky fraction = {sky_fraction:.6f}\n"
    )

    f.write(
        f"Maximum overlap = {max_overlap}\n"
    )

# -----------------------------------
# Binary coverage
# -----------------------------------

binary_map = (
    coverage > 0
).astype(int)

hp.mollview(
    binary_map,
    title="SPOTLIGHT Sky Coverage"
)

plt.savefig(
    "coverage_binary.png",
    dpi=300,
    bbox_inches="tight"
)

plt.close()

# -----------------------------------
# Density map
# -----------------------------------

hp.mollview(
    coverage,
    title="SPOTLIGHT Coverage Density",
    unit="Beam overlaps"
)

plt.savefig(
    "coverage_density.png",
    dpi=300,
    bbox_inches="tight"
)

plt.close()

print()
print("Finished")
print("coverage_map.fits")
print("coverage_binary.png")
print("coverage_density.png")
print("coverage_stats.txt")
