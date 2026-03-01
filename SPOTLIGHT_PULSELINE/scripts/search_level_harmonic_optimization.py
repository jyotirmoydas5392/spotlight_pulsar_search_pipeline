import numpy as np
import os

def is_harmonically_related(p1, p2, dm1, dm2, max_harm, period_tol, DM_slope, DM_intercept):
    """
    Check if p1 and p2 are harmonically related up to max_harm and have similar DM.
    Uses dynamic DM tolerance calculated from fixed period (10 ms) for consistency.
    """
    tol_period = 0.050  # fixed period of 50 ms
    dm_tolerance = DM_intercept + DM_slope * tol_period
    period_ratio = p1 / p2
    
    for n in range(1, max_harm + 1):
        for m in range(1, max_harm + 1):
            if abs(period_ratio - (n / m)) < period_tol:
                if abs(dm1 - dm2) <= dm_tolerance:
                    return True
    return False

def harmonic_filtering(input_dir, output_dir, file_name, period_tol_harm, max_harm,
                       DM_filtering_cut_10, DM_filtering_cut_1000):
    """
    Perform harmonic filtering on candidates, keeping only the strongest (highest SNR)
    in each harmonic group, removing weaker harmonics immediately.
    """
    input_path = os.path.join(input_dir, f"{file_name}_all_sifted_candidates.txt")
    output_path = os.path.join(output_dir, f"{file_name}_all_sifted_harmonic_removed_candidates.txt")

    # Read input file
    try:
        with open(input_path, 'r') as f:
            lines = f.readlines()
    except FileNotFoundError:
        print(f"Input file {input_path} not found.")
        return

    if len(lines) == 1 and "No valid data found to process." in lines[0]:
        with open(output_path, 'w') as f_out:
            f_out.write("No valid data found to process.\n")
        print(f"No valid data found. Exiting. Response written to {output_path}")
        return

    try:
        data = np.loadtxt(input_path, dtype=float, skiprows=1)
        if data.size == 0:
            raise ValueError("No valid candidate data found in the file.")
    except Exception as e:
        with open(output_path, 'w') as f_out:
            f_out.write("No valid data found to process.\n")
        print(f"Error reading input file: {e}. Response written to {output_path}")
        return

    periods = data[:, 0]
    p_dots = data[:, 1]
    DMs = data[:, 2]
    SNRs = data[:, 3]
    n_candidates = len(periods)

    # Dynamic DM tolerance parameters
    DM_slope = (DM_filtering_cut_1000 - DM_filtering_cut_10) / (1.000 - 0.010)
    DM_intercept = DM_filtering_cut_10 - DM_slope * 0.010

    # Sort candidates by SNR (descending)
    sorted_indices = np.argsort(-SNRs)
    remaining_indices = set(range(n_candidates))
    final_indices = []

    for idx in sorted_indices:
        if idx not in remaining_indices:
            continue

        # Keep this candidate
        final_indices.append(idx)

        # Remove harmonic-related candidates (greedily)
        to_remove = set()
        for other_idx in remaining_indices:
            if other_idx == idx:
                continue
            higher_period = max(periods[idx], periods[other_idx])
            period_tol = higher_period * period_tol_harm
            if is_harmonically_related(periods[idx], periods[other_idx], DMs[idx], DMs[other_idx],
                                       max_harm, period_tol, DM_slope, DM_intercept):
                to_remove.add(other_idx)

        # Also remove self from remaining
        to_remove.add(idx)
        remaining_indices.difference_update(to_remove)

    # Write output
    filtered_data = data[final_indices]
    filtered_data = filtered_data[np.argsort(filtered_data[:, 0])[::-1]]  # sort by period descending

    with open(output_path, 'w') as f_out:
        f_out.write("Period(sec)   Pdot(s/s)  DM(pc/cc)   SNR   Harmonic_no\n")
        for row in filtered_data:
            f_out.write(f"{row[0]:.10f}     {row[1]:.6e}     {row[2]:.2f}     {row[3]:.2f}      {row[4]}\n")

    print(f"Harmonic filtering done. Kept {len(final_indices)} of {n_candidates} candidates.")
    print(f"Results saved to {output_path}")