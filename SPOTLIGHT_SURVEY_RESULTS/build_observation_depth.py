import healpy as hp
import pandas as pd
import numpy as np

NSIDE = 512

HWHM_LOOKUP = {
    "BAND3":35.98,
    "BAND4":21.71,
    "BAND5":12.0
}

beam = pd.read_csv(
    "retained_beams.csv"
)

band = pd.read_csv(
    "obs_band_lookup.csv"
)

band_lookup = dict(
    zip(
        band["Observation_Directory"],
        band["Band"]
    )
)

obs_map = np.zeros(
    hp.nside2npix(NSIDE),
    dtype=np.int32
)

for obsdir, group in beam.groupby(
    "ObservationDir"
):

    band_name = band_lookup[obsdir]

    radius = (
        HWHM_LOOKUP[band_name]
        / 60.0
    )

    covered = set()

    for _, r in group.iterrows():

        vec = hp.ang2vec(
            np.radians(
                90-r["BeamDec_deg"]
            ),
            np.radians(
                r["BeamRA_deg"]
            )
        )

        pix = hp.query_disc(
            NSIDE,
            vec,
            np.radians(radius)
        )

        covered.update(pix)

    obs_map[list(covered)] += 1

    print(
        obsdir,
        len(covered)
    )

hp.write_map(
    "observation_depth_map.fits",
    obs_map,
    overwrite=True
)

print("written observation_depth_map.fits")
