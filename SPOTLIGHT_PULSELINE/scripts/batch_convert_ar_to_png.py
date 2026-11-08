#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import os
import sys
import glob
import numpy as np
import psrchive
import multiprocessing as mp

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


# ------------------------------------------------------------
def compute_profile_snr(profile):
    """Robust SNR using lowest 20% bins as off-pulse"""
    prof = profile.copy()
    nbin = len(prof)
    off = np.sort(prof)[:max(1, int(0.2 * nbin))]
    return (prof.max() - off.mean()) / (off.std() + 1e-6)


# ------------------------------------------------------------
def plot_psrchive_diagnostic(args):
    arfile, outpng = args

    try:
        # ----------------------------------------------------
        # Load archive
        # ----------------------------------------------------
        ar = psrchive.Archive_load(arfile)
        ar.remove_baseline()

        nsub  = ar.get_nsubint()
        nchan = ar.get_nchan()
        nbin  = ar.get_nbin()

        phase = np.linspace(0.0, 1.0, nbin, endpoint=False)

        # ====================================================
        # Integrated profile (NO alignment)
        # ====================================================
        prof = np.zeros(nbin)
        for isub in range(nsub):
            for ichan in range(nchan):
                prof += ar.get_Profile(isub, 0, ichan).get_amps()

        prof -= np.median(prof)
        if prof.max() != 0:
            prof /= prof.max()

        snr_prof = compute_profile_snr(prof)

        # ====================================================
        # Frequency vs phase (RAW)
        # ====================================================
        freqs = np.zeros(nchan)
        freq_phase = np.zeros((nchan, nbin))

        for ichan in range(nchan):
            freqs[ichan] = ar.get_Integration(0).get_centre_frequency(ichan)

            tmp = np.zeros(nbin)
            for isub in range(nsub):
                tmp += ar.get_Profile(isub, 0, ichan).get_amps()

            tmp -= np.median(tmp)
            freq_phase[ichan] = tmp

        order = np.argsort(freqs)
        freqs = freqs[order]
        freq_phase = freq_phase[order]

        # ====================================================
        # Time vs phase (RAW)
        # ====================================================
        time_phase = np.zeros((nsub, nbin))

        for isub in range(nsub):
            tmp = np.zeros(nbin)
            for ichan in range(nchan):
                tmp += ar.get_Profile(isub, 0, ichan).get_amps()

            tmp -= np.median(tmp)
            time_phase[isub] = tmp

        try:
            tsub = ar.get_Integration(0).get_duration()
        except:
            tsub = 1.0
        total_time = nsub * tsub

        # ====================================================
        # Metadata (P, Pdot, DM)
        # ====================================================
        try:
            P = ar.get_Integration(0).get_folding_period()
        except:
            P = np.nan

        try:
            Pdot = ar.get_Integration(0).get_folding_period_derivative()
        except:
            Pdot = np.nan

        try:
            DM = ar.get_dispersion_measure()
        except:
            DM = np.nan

        fcen = ar.get_centre_frequency()
        bw   = abs(ar.get_bandwidth())

        # ====================================================
        # Colour scaling
        # ====================================================
        f_vmin, f_vmax = np.percentile(freq_phase, [1, 99])
        t_vmin, t_vmax = np.percentile(time_phase, [1, 99])

        # ====================================================
        # Layout
        # ====================================================
        fig = plt.figure(figsize=(15, 9))

        col1_w, col2_w, col3_w, gap = 0.18, 0.28, 0.28, 0.05
        x1 = 0.05
        x2 = x1 + col1_w + gap
        x3 = x2 + col2_w + gap

        ax_prof = fig.add_axes([x1, 0.64, col1_w, 0.30])
        ax_txt  = fig.add_axes([x1, 0.10, col1_w, 0.48])
        ax_freq = fig.add_axes([x2, 0.10, col2_w, 0.84])
        ax_time = fig.add_axes([x3, 0.10, col3_w, 0.84])

        # ====================================================
        # Profile
        # ====================================================
        ax_prof.plot(phase, prof, color="black", lw=2.0)
        ax_prof.set_xlim(0, 1)
        ax_prof.set_ylabel("Intensity")
        ax_prof.set_xticks([])
        ax_prof.set_title("Pulse Profile", fontweight="bold")

        # ====================================================
        # Metadata panel
        # ====================================================
        ax_txt.axis("off")
        ax_txt.text(
            0.0, 1.0,
            (
                "Candidate Summary\n"
                "=================\n\n"
                "Profile SNR : %.2f\n\n"
                "Period (P)  : %.9f s\n"
                "Pdot        : %.3e s/s\n"
                "DM          : %.2f pc cm$^{-3}$\n\n"
                "f$_c$       : %.1f MHz\n"
                "BW          : %.1f MHz\n\n"
                "Nchan       : %d\n"
                "Nsub        : %d\n"
                "Nbin        : %d\n\n"
            ) % (snr_prof, P, Pdot, DM, fcen, bw, nchan, nsub, nbin),
            va="top",
            fontsize=13
        )

        # ====================================================
        # Frequency vs phase
        # ====================================================
        ax_freq.imshow(
            freq_phase,
            aspect="auto",
            origin="lower",
            extent=[0, 1, freqs.min(), freqs.max()],
            vmin=f_vmin, vmax=f_vmax,
            cmap="viridis"
        )
        ax_freq.set_ylabel("Frequency (MHz)")
        ax_freq.set_xticks([])
        ax_freq.set_title("Frequency vs Phase", fontweight="bold")

        # ====================================================
        # Time vs phase
        # ====================================================
        ax_time.imshow(
            time_phase,
            aspect="auto",
            origin="lower",
            extent=[0, 1, 0, total_time],
            vmin=t_vmin, vmax=t_vmax,
            cmap="viridis"
        )
        ax_time.set_ylabel("Time (s)")
        ax_time.set_xticks([])
        ax_time.set_title("Time vs Phase", fontweight="bold")

        # ====================================================
        # Shared labels
        # ====================================================
        fig.text(0.60, 0.05, "Pulse phase", ha="center", fontsize=12)
        fig.text(0.50, 0.02, os.path.basename(arfile), ha="center", fontsize=11)

        plt.savefig(outpng, dpi=150)
        plt.close(fig)

        return True

    except Exception as e:
        sys.stderr.write("FAILED: %s (%s)\n" % (arfile, str(e)))
        return False


# ------------------------------------------------------------
def batch_convert_ar_to_png(input_dir, output_dir, workers):

    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)

    ar_files = sorted(glob.glob(os.path.join(input_dir, "*.ar")))
    tasks = [
        (f, os.path.join(output_dir, os.path.basename(f).replace(".ar", ".png")))
        for f in ar_files
    ]

    print("Total files :", len(tasks))
    print("Workers     :", workers)

    if workers <= 1:
        for t in tasks:
            plot_psrchive_diagnostic(t)
    else:
        pool = mp.Pool(processes=workers)
        pool.map(plot_psrchive_diagnostic, tasks)
        pool.close()
        pool.join()


# ------------------------------------------------------------
if __name__ == "__main__":

    if len(sys.argv) != 4:
        print("Usage: python plot_ar_diagnostics_batch.py <input_dir> <output_dir> <workers>")
        sys.exit(1)

    batch_convert_ar_to_png(sys.argv[1], sys.argv[2], int(sys.argv[3]))