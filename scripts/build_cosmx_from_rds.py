#!/usr/bin/env python3
"""
build_cosmx_from_rds.py
Build a CosMx SpatialData object from an exported Seurat object, for the one
acquisition that has no AtoMx flat-file export.

`SAHA_CP_APE_A9_TAP03` (appendix protein, 145,165 cells) exists only as
APE_sep_Protein_SeuratObj.RDS. The protein run put bone marrow I17 on slide 1
and appendix A9 on slide 2, and only the slide-1 half was exported as flat
files -- that export is `SAHA_CP_BM_I17_TAP03`, despite sitting in a directory
named Protein/APE.

Coordinates
-----------
The RDS carries `x_slide_mm` / `y_slide_mm` but no `CenterX_global_px`. The
conversion was fitted on ILE_sep_Protein, where the same object and its flat-
file export share all 123,503 cells:

    global_px = 8313.8688 * slide_mm + offset      (max residual 0.00 px)

8313.8688 px/mm is exactly 1 / 0.12028 um, the CosMx pixel pitch, and both
axes share the sign -- there is no y flip. The per-acquisition offset cannot
be recovered without an export to fit against, so this object's global frame
is the physical slide frame in pixels. Each SpatialData object carries its own
coordinate system, so that is self-consistent; it simply is not the same
origin as the flat-file-derived objects.

Inputs (written by the R side):
    counts.mtx / features.txt / cells.txt
    negprobes.mtx / negprobes_features.txt   (appended to var, as the flat-file
                                              builder does)
    obs.csv

Usage:
    P=~/.venv-sd08/bin/python
    $P scripts/build_cosmx_from_rds.py --in-dir ape_export \\
        --sample SAHA_CP_APE_A9_TAP03 --out-dir /athena/project-saha/derived/build/zenodo_tier/<record_slug>
"""

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

PX_PER_MM = 8313.8688          # 1 / 0.12028 um; fitted exactly on ILE protein
PX_UM_PER_PX = 0.12028         # CosMx pixel pitch

RENAME = {
    "modified.celltyping.round_1": "cell_type",
    "modified.celltyping.final_cell_type": "cell_type_detailed",
    "spatialClusteringAssignments": "spatial_niche",
}


def read_lines(path: Path):
    """One name per line, NOT whitespace-split: protein markers legitimately
    contain spaces (`NF-kB p65`, `Ms IgG1`), and splitting on whitespace
    silently shifts every feature name after the first such marker."""
    return [ln.strip() for ln in path.read_text().splitlines() if ln.strip()]


def read_mtx(path: Path):
    from scipy.io import mmread
    return mmread(path).T.tocsr()          # mtx is features x cells


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--in-dir", type=Path, required=True)
    ap.add_argument("--sample", required=True)
    ap.add_argument("--out-dir", type=Path, required=True)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    import anndata as ad
    import geopandas as gpd
    import spatialdata as sd
    from shapely.geometry import Point
    from spatialdata.models import ShapesModel, TableModel

    d = args.in_dir
    cells = read_lines(d / "cells.txt")
    genes = read_lines(d / "features.txt")
    X = read_mtx(d / "counts.mtx")
    obs = pd.read_csv(d / "obs.csv")
    if len(obs) != len(cells) or X.shape != (len(cells), len(genes)):
        sys.exit(f"ERROR: shape mismatch  X={X.shape} cells={len(cells)} "
                 f"genes={len(genes)} obs={len(obs)}")
    if (obs["cell"].astype(str).to_numpy() != np.asarray(cells)).any():
        sys.exit("ERROR: obs row order does not match cells.txt")

    # negative-control probes go into var alongside the targets, matching
    # create_cosmx_zarr.py so the two builders produce the same shape of object
    npz = d / "negprobes.mtx"
    if npz.exists():
        NP = read_mtx(npz)
        np_names = read_lines(d / "negprobes_features.txt")
        from scipy.sparse import hstack
        X = hstack([X, NP]).tocsr()
        genes = genes + np_names

    # The RDS carries a display-cased duplicate of final_cell_type under the
    # name `cell_type` ("CD4+ T cell" vs "cd4_tcell"). Renaming round_1 onto
    # `cell_type` would collide with it, and spatialdata rejects the table.
    # Drop the duplicate: the sibling protein objects (BM I17/I18, ILE) all use
    # the snake_case vocabulary, and consistency across the release beats
    # prettier labels in one object.
    dup = [c for c in RENAME.values() if c in obs.columns and c not in RENAME]
    if dup:
        print(f"  dropping duplicate column(s) {dup} in favour of the "
              f"{list(RENAME)} mapping")
        obs = obs.drop(columns=dup)
    obs = obs.rename(columns=RENAME)
    if obs.columns.has_duplicates:
        sys.exit(f"ERROR: duplicate obs columns after rename: "
                 f"{obs.columns[obs.columns.duplicated()].tolist()}")
    obs.index = pd.Index([str(c) for c in cells], name=None)
    obs["cell"] = obs.index.astype(str)
    obs["sample_id"] = args.sample
    obs["region"] = pd.Categorical(["cells"] * len(obs))

    gx = obs["x_slide_mm"].to_numpy(dtype="float64") * PX_PER_MM
    gy = obs["y_slide_mm"].to_numpy(dtype="float64") * PX_PER_MM
    obs["CenterX_global_px"] = gx
    obs["CenterY_global_px"] = gy

    Xv = X.data if hasattr(X, "data") else np.asarray(X).ravel()
    integral = bool(np.all(Xv[:200000] == np.floor(Xv[:200000])))
    if not integral:
        print("  NOTE: X is NOT integer — this object holds processed "
              "(background-subtracted) intensities, not raw counts")
    table = ad.AnnData(X=X.astype("float32"), obs=obs,
                       var=pd.DataFrame(index=pd.Index(genes)))
    table.uns["x_is_raw_counts"] = integral
    table.obsm["spatial"] = np.c_[gx, gy]

    # no polygons in the RDS: circles of the recorded cell area, same fallback
    # the flat-file builder uses when an export ships without a polygons file.
    # Some objects record only `Area` in px^2; convert rather than fall back to
    # a constant radius, which would make every cell the same size.
    if "Area.um2" in obs.columns:
        area = obs["Area.um2"].to_numpy(dtype="float64")
    elif "Area" in obs.columns:
        area = obs["Area"].to_numpy(dtype="float64") * (PX_UM_PER_PX ** 2)
        print(f"  Area.um2 absent - derived from Area (px^2) at "
              f"{PX_UM_PER_PX} um/px")
    else:
        area = np.full(len(obs), 100.0)
        print("  WARNING: no area column - every cell gets a 100 um^2 circle")
    radii = np.sqrt(np.maximum(area, 1e-6) / np.pi)
    shapes = ShapesModel.parse(gpd.GeoDataFrame(
        {"geometry": [Point(x, y) for x, y in zip(gx, gy)], "radius": radii},
        index=table.obs.index))

    table = TableModel.parse(table, region="cells", region_key="region",
                             instance_key="cell")

    print(f"=== {args.sample}")
    print(f"  {table.n_obs:,} cells x {table.n_vars} features "
          f"({len(genes) - (len(np_names) if npz.exists() else 0)} targets"
          f"{' + ' + str(len(np_names)) + ' negprobes' if npz.exists() else ''})")
    print(f"  x {gx.min():,.0f} .. {gx.max():,.0f} px   "
          f"y {gy.min():,.0f} .. {gy.max():,.0f} px   "
          f"({(gx.max()-gx.min())/PX_PER_MM:.2f} x {(gy.max()-gy.min())/PX_PER_MM:.2f} mm)")
    for c in ("cell_type", "cell_type_detailed", "spatial_niche"):
        if c in table.obs.columns:
            v = table.obs[c]
            print(f"  {c:20s} {v.notna().sum():,} non-NA, {v.nunique()} levels")
    if args.dry_run:
        print("  [dry run - nothing written]")
        return 0

    out = args.out_dir / f"{args.sample}.zarr"
    sd.SpatialData(tables={"table": table}, shapes={"cells": shapes}).write(
        out, overwrite=True)
    print(f"  wrote {out}")

    back = sd.read_zarr(out)
    t = back.tables["table"]
    assert t.n_obs == len(cells) and t.n_vars == len(genes)
    assert set(t.obs["region"].astype(str)) == {"cells"}
    assert len(back.shapes["cells"]) == len(cells)
    print(f"  VERIFIED {t.n_obs:,} cells, {t.n_vars} features, "
          f"{len(back.shapes['cells']):,} shapes")
    return 0


if __name__ == "__main__":
    sys.exit(main())
