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

for band_name in [
    "BAND3",
    "BAND4",
    "BAND5"
]:

    m = np.zeros(
        hp.nside2npix(NSIDE),
        dtype=np.int32
    )

    sub = beam[
        beam["ObservationDir"].map(
            band_lookup
        ) == band_name
    ]

    radius = (
        HWHM_LOOKUP[band_name]
        / 60.0
    )

    for _, r in sub.iterrows():

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

        m[pix] += 1

    hp.write_map(
        f"{band_name}_coverage.fits",
        m,
        overwrite=True
    )

    area = (
        np.sum(m>0)
        *
        hp.nside2pixarea(
            NSIDE,
            degrees=True
        )
    )

    print(
        band_name,
        "area=",
        area,
        "deg^2"
    )
