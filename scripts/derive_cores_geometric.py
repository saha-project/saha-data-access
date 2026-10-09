#!/usr/bin/env python3
"""
derive_cores_geometric.py
Recover TMA core GROUPING for a v2 CosMx slide whose AtoMx Core_ID is empty (PROS_A3).

TMA punches are islands of tissue separated by bare glass, so cells are grouped
by connected tissue: bin both tables' cell centroids (shared frame, from SHAPES --
see transfer_cores_v2.py for why obs coordinates are wrong here) on a --bin-um
grid, close small gaps (glandular lumens) with --close-um, label connected
components, and drop components under --min-cells as debris (cells -> NA).

This yields how many cores there are and which cells belong together. It does
NOT yield donor identity or AtoMx's Core_ID numbering: labels are assigned in
reading order (rows top->bottom, then left->right) and prefixed `geo` so they
cannot be mistaken for AtoMx IDs.

Validation: if the slide has a filled --truth column (PROS_A2 donor_core: RNA from AtoMx, protein transferred), the same
method is scored against it -- per derived core, purity (share of its cells in
its majority true core) and whether each true core maps to exactly one derived
core. Run on A2 first; trust A3 only if A2 recovers 31/31 cores cleanly.

Output (nothing is written to the object): <out>/<slide>.<table>.cores.parquet
(obs_index, core), <out>/<slide>.cores.jpg, <out>/<slide>.cores.json.

Usage (on Cayuga):
    $P scripts/derive_cores_geometric.py --object .../SAHA_CosMx_PROS_A2.zarr --slide PROS_A2 --out release_qc/v2_cores
"""

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from transfer_cores_v2 import PX_UM, centroids  # noqa: E402


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--object", type=Path, required=True)
    ap.add_argument("--slide", required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--bin-um", type=float, default=25.0)
    ap.add_argument("--close-um", type=float, default=75.0)
    ap.add_argument("--min-cells", type=int, default=300)
    ap.add_argument("--truth", default="donor_core")
    ap.add_argument("--merge", action="append", default=[], metavar="X1,Y1:X2,Y2",
                    help="join the components under these two points (um, as plotted in <slide>.cores.jpg); "
                         "for one punch broken in two, confirmed on the same-section H&E. Repeatable.")
    a = ap.parse_args()

    import warnings
    warnings.filterwarnings("ignore")
    from scipy import ndimage
    from spatialdata import read_zarr

    sd = read_zarr(a.object)
    xy = {t: centroids(sd, t) for t in sd.tables}
    allxy = np.vstack([v[0] for v in xy.values()]) * PX_UM          # um
    lo = allxy.min(0)
    b = lambda p: ((p * PX_UM - lo) // a.bin_um).astype(int)        # noqa: E731
    shape = tuple(b(allxy / PX_UM).max(0) + 1)
    occ = np.zeros(shape, bool)
    for p, _ in xy.values():
        ij = b(p)
        occ[ij[:, 0], ij[:, 1]] = True
    r = max(1, int(round(a.close_um / a.bin_um)))
    occ = ndimage.binary_closing(occ, structure=np.ones((3, 3)), iterations=r, border_value=0) | occ
    lab, n = ndimage.label(occ)

    # cells per component across tables; small components = debris
    counts = np.zeros(n + 1, int)
    cell_lab = {}
    for t, (p, idx) in xy.items():
        ij = b(p)
        cell_lab[t] = lab[ij[:, 0], ij[:, 1]]
        counts += np.bincount(cell_lab[t], minlength=n + 1)
    # manual merges: each point -> component of the nearest cell
    if a.merge:
        from scipy.spatial import cKDTree
        allp = np.vstack([p for p, _ in xy.values()]) * PX_UM
        alll = np.concatenate(list(cell_lab.values()))
        tree = cKDTree(allp)
        for m in a.merge:
            k1, k2 = (int(alll[tree.query([float(v) for v in pt.split(",")])[1]]) for pt in m.split(":"))
            assert k1 and k2 and k1 != k2, f"--merge {m}: points resolve to components {k1}, {k2}"
            lab[lab == k2] = k1
            for t in cell_lab:
                cell_lab[t][cell_lab[t] == k2] = k1
            counts[k1] += counts[k2]; counts[k2] = 0
            print(f"merged component {k2} into {k1} ({m})")
    keep = [k for k in range(1, n + 1) if counts[k] >= a.min_cells]
    # reading order: rows = runs of centroids within 1000 um in y (core spacing ~2 mm), then x
    cen = ndimage.center_of_mass(np.ones_like(lab), lab, keep)        # (x_bin, y_bin)
    cen = np.array(cen) * a.bin_um
    row, start = {}, None
    for i in sorted(range(len(keep)), key=lambda i: cen[i][1]):
        if start is None or cen[i][1] - start > 1000:
            start = cen[i][1]
        row[i] = start
    order = sorted(range(len(keep)), key=lambda i: (row[i], cen[i][0]))
    name = {keep[i]: f"{a.slide}_geo{j + 1:02d}" for j, i in enumerate(order)}

    a.out.mkdir(parents=True, exist_ok=True)
    summ = {"slide": a.slide, "params": {k: getattr(a, k) for k in ("bin_um", "close_um", "min_cells")},
            "merges": a.merge, "n_components": int(n), "n_cores": len(keep), "tables": {}}
    labels = {}
    for t, (p, idx) in xy.items():
        core = pd.Series([name.get(k) for k in cell_lab[t]], index=idx, dtype=object)
        labels[t] = core
        pd.DataFrame({"obs_index": idx, "core": core.to_numpy()}).to_parquet(
            a.out / f"{a.slide}.{t}.cores.parquet", index=False)
        vc = core.value_counts()
        summ["tables"][t] = {"n_cells": int(len(core)), "assigned": int(core.notna().sum()),
                             "cells_per_core_min": int(vc.min()), "cells_per_core_max": int(vc.max())}
        print(f"{t}: {core.notna().sum():,}/{len(core):,} cells in {core.nunique()} cores "
              f"(min {vc.min():,}, max {vc.max():,} cells/core)")

    # validation against recorded cores, where they exist
    for t in sd.tables:
        obs = sd.tables[t].obs
        if a.truth not in obs or obs[a.truth].notna().sum() == 0:
            continue
        truth = obs[a.truth].astype(object).where(obs[a.truth].notna())
        df = pd.DataFrame({"geo": labels[t], "truth": truth}).dropna()
        ct = pd.crosstab(df.geo, df.truth)
        purity = (ct.max(1) / ct.sum(1))
        t_to_geo = ct.idxmax(0)
        one_to_one = t_to_geo.nunique() == ct.shape[1] == ct.shape[0]
        summ.setdefault("validation", {})[t] = {"true_cores": int(ct.shape[1]), "geo_cores": int(ct.shape[0]),
                              "one_to_one": bool(one_to_one), "purity_min": float(purity.min()),
                              "purity_median": float(purity.median()),
                              "truth_cells_unassigned": int(truth.notna().sum() - len(df))}
        print(f"VALIDATION vs {t}.{a.truth}: {ct.shape[1]} true vs {ct.shape[0]} derived cores, "
              f"one-to-one={one_to_one}, purity min {purity.min():.4f} median {purity.median():.4f}, "
              f"{summ['validation'][t]['truth_cells_unassigned']:,} true-labelled cells left NA")
        (a.out / f"{a.slide}.{t}.crosstab.csv").write_text(ct.to_csv())

    (a.out / f"{a.slide}.cores.json").write_text(json.dumps(summ, indent=2))

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    t0 = next(iter(xy))
    p, _ = xy[t0]
    p = p * PX_UM
    c = labels[t0].to_numpy()
    fig, ax = plt.subplots(figsize=(12, 12 * np.ptp(p[:, 1]) / max(np.ptp(p[:, 0]), 1)))
    na = pd.isna(c)
    ax.scatter(p[na, 0], p[na, 1], s=0.2, c="lightgrey", rasterized=True)
    codes = pd.Categorical(c[~na]).codes
    ax.scatter(p[~na, 0], p[~na, 1], s=0.2, c=codes % 20, cmap="tab20", rasterized=True)
    for k, i in zip(keep, range(len(keep))):
        ax.text(cen[i][0] + lo[0], cen[i][1] + lo[1], name[k].split("_geo")[1], ha="center", va="center",
                fontsize=9, weight="bold")
    ax.invert_yaxis(); ax.set_aspect("equal")
    ax.set_title(f"{a.slide} {t0}: {len(keep)} geometric cores (grey = NA/debris)")
    fig.savefig(a.out / f"{a.slide}.cores.jpg", dpi=110, bbox_inches="tight")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
