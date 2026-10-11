#!/usr/bin/env python3
"""
build_from_seurat.py
Build a SpatialData object from the intermediate written by export_seurat.R.

For the Northwestern / Dulai IBD cohort: 52 acquisitions (20 CosMx, 32 Xenium)
that exist only as Seurat RDS. Both platforms carry real segmentation
boundaries, so these get true cell polygons rather than the circle placeholders
some of the TAP objects fall back to.

Differs from build_cosmx_from_rds.py, which handles the single APE protein
acquisition: that one has only slide-millimetre coordinates and no polygons, so
it fits circles and derives pixels from millimetres. Here the centroids are
already in the image frame and the boundaries are present, so neither
approximation is needed.

Control probes (CosMx negprobes + falsecode, Xenium Blank/Control codewords) go
into var beside the targets, matching create_cosmx_zarr.py, so background can be
computed from the object alone.

Usage:
    P=~/.venv-sd08/bin/python
    $P scripts/build_from_seurat.py --in-dir <export> --manifest dulai_manifest.csv \
        --sample SAHA_CR_IBD_PD_A1_1_NU01 --out-dir .../dulai_zarr [--dry-run]
"""

import argparse
import json
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

# Seurat column names -> the release vocabulary.
RENAME = {
    "ct_major": "cell_type",
    "ct_minor": "cell_type_detailed",
    "Insitutype_Broad": "cell_type",
    "Insitutype_Labelled": "cell_type_detailed",
    "niche": "spatial_niche",
    "spatialClusteringAssignments": "spatial_niche",
}


def read_lines(path: Path):
    """One name per line, never whitespace-split -- marker names contain spaces."""
    return [ln.rstrip("\n").strip('"') for ln in path.read_text().splitlines() if ln.strip()]


def read_mtx(path: Path):
    from scipy.io import mmread
    return mmread(path).T.tocsr()          # mtx is features x cells


def polygons_for(path: Path, cells):
    """cell -> shapely Polygon, in the order given by `cells`.

    Boundary vertices arrive as one row per point, already in ring order.
    Rings with fewer than 3 points cannot form a polygon and are reported
    rather than silently dropped.
    """
    from shapely.geometry import Polygon

    seg = pd.read_csv(path)
    seg["cell"] = seg["cell"].astype(str)
    geoms, degenerate = {}, []
    for cell, g in seg.groupby("cell", sort=False):
        if len(g) < 3:
            degenerate.append(cell)
            continue
        geoms[cell] = Polygon(np.c_[g["x"].to_numpy(), g["y"].to_numpy()]).buffer(0)
    missing = [c for c in cells if c not in geoms]
    return geoms, degenerate, missing


def snap_polygons_to_fovs(shapes, cx, cy, fov, tol=50.0):
    """Translate each FOV's polygons onto its cells' centroids where they disagree.

    In the A2 NU02 CosMx RDS (4 acquisitions) the stored boundaries sit
    82-102k px from the cells' own centroids -- every FOV, by whole multiples of
    ~8512 px (2x the FOV pitch). Within a FOV the polygons are right: a per-FOV
    translation leaves a 3.8 px median residual, the same as the healthy
    objects (3.6 px). So each misplaced FOV is moved by its median offset.
    `shapes` must be row-aligned with cx/cy/fov. Returns (shapes, moved_fovs).
    """
    from shapely.affinity import translate
    c = shapes.geometry.centroid
    d = pd.DataFrame({"dx": cx - c.x.to_numpy(), "dy": cy - c.y.to_numpy(), "fov": fov})
    off = d.groupby("fov")[["dx", "dy"]].median()
    moved = off[np.hypot(off.dx, off.dy) > tol]
    if len(moved):
        shapes = shapes.copy()
        for f, (dx, dy) in moved.iterrows():
            m = (d.fov == f).to_numpy()
            shapes.loc[m, "geometry"] = shapes.geometry[m].apply(translate, xoff=dx, yoff=dy)
        c = shapes.geometry.centroid
        resid = np.median(np.hypot(cx - c.x.to_numpy(), cy - c.y.to_numpy()))
        if resid > tol:  # the per-FOV model does not hold here; refuse rather than guess
            raise ValueError(f"polygons still {resid:.0f} px from centroids after snapping")
    return shapes, moved


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--in-dir", type=Path, required=True)
    ap.add_argument("--manifest", type=Path, required=True)
    ap.add_argument("--sample", required=True)
    ap.add_argument("--out-dir", type=Path, required=True)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    import anndata as ad
    import geopandas as gpd
    import spatialdata as sd
    from spatialdata.models import ShapesModel, TableModel

    man = pd.read_csv(args.manifest).set_index("sample_id")
    if args.sample not in man.index:
        sys.exit(f"ERROR: {args.sample} not in {args.manifest}")
    row = man.loc[args.sample]

    d = args.in_dir
    cells = [str(c) for c in read_lines(d / "cells.txt")]
    genes = read_lines(d / "features.txt")
    X = read_mtx(d / "counts.mtx")
    obs = pd.read_csv(d / "obs.csv")
    obs["cell"] = obs["cell"].astype(str)

    if X.shape != (len(cells), len(genes)) or len(obs) != len(cells):
        sys.exit(f"ERROR: shape mismatch X={X.shape} cells={len(cells)} "
                 f"genes={len(genes)} obs={len(obs)}")
    if (obs["cell"].to_numpy() != np.asarray(cells)).any():
        sys.exit("ERROR: obs row order does not match cells.txt")

    ctrl = d / "controls.mtx"
    if ctrl.exists():
        from scipy.sparse import hstack
        C = read_mtx(ctrl)
        cnames = read_lines(d / "controls_features.txt")
        if C.shape[0] != len(cells):
            sys.exit(f"ERROR: controls have {C.shape[0]} cells, counts have {len(cells)}")
        X = hstack([X, C]).tocsr()
        genes = genes + cnames

    if len(set(genes)) != len(genes):
        dupes = pd.Series(genes)[pd.Series(genes).duplicated()].unique()[:5]
        sys.exit(f"ERROR: duplicate feature names after adding controls: {list(dupes)}")

    obs = obs.rename(columns={k: v for k, v in RENAME.items() if k in obs.columns})
    if obs.columns.has_duplicates:
        dup = obs.columns[obs.columns.duplicated()].tolist()
        sys.exit(f"ERROR: duplicate obs columns after rename: {dup}")

    # The AtoMx export carries both `cell_id` and `cell_ID`. SpatialData treats
    # names case-insensitively and refuses the table outright. Both are the same
    # identifier and neither is the instance key (`cell` is), so keep the first
    # spelling and drop the rest rather than letting the write fail 52 times.
    seen, casedrop = {}, []
    for c in obs.columns:
        k = str(c).lower()
        if k in seen:
            casedrop.append(c)
        else:
            seen[k] = c
    if casedrop:
        print(f"  dropping case-variant duplicate obs column(s): {casedrop}")
        obs = obs.drop(columns=casedrop)

    obs.index = pd.Index(cells, name=None)
    obs["sample_id"] = args.sample
    obs["platform"] = row["platform"]
    obs["assay_type"] = row["assay_type"]
    obs["organ"] = row["organ"]
    obs["condition"] = row["condition"]
    obs["disease"] = row["disease"]
    obs["disease_state"] = row["disease_state"]
    # Donor is per-cell everywhere in SAHA. Here one acquisition is one block,
    # so every cell shares a core -- but the column is written the same way so
    # it joins donors.parquet identically to the microarray acquisitions.
    obs["donor_core"] = pd.Categorical([row["donor_core"]] * len(obs))
    obs["donor_patient"] = pd.Categorical([row["donor_patient"]] * len(obs))
    obs["region"] = pd.Categorical(["cells"] * len(obs))

    gx = obs["x_centroid_px"].to_numpy(dtype="float64")
    gy = obs["y_centroid_px"].to_numpy(dtype="float64")

    Xv = X.data if hasattr(X, "data") else np.asarray(X).ravel()
    integral = bool(np.all(Xv[:200000] == np.floor(Xv[:200000])))
    if not integral:
        print("  NOTE: X is not integer -- these are processed values, not raw counts")

    table = ad.AnnData(X=X.astype("float32"), obs=obs,
                       var=pd.DataFrame(index=pd.Index(genes)))
    table.uns["x_is_raw_counts"] = integral
    table.obsm["spatial"] = np.c_[gx, gy]

    poly_path = d / "polygons.csv"
    if not poly_path.exists():
        sys.exit("ERROR: no polygons.csv -- refusing to build a boundary-free object")
    geoms, degenerate, missing = polygons_for(poly_path, cells)
    if degenerate:
        print(f"  {len(degenerate)} cell(s) had fewer than 3 boundary points")
    if missing:
        print(f"  {len(missing)} cell(s) have no polygon; filling with centroid circles")
    from shapely.geometry import Point
    radius = float(np.sqrt(np.nanmedian(obs.get("Area", pd.Series([100.0]*len(obs)))) / np.pi))
    shapes = gpd.GeoDataFrame(
        {"geometry": [geoms.get(c) or Point(x, y).buffer(radius)
                      for c, x, y in zip(cells, gx, gy)]},
        index=pd.Index(cells))
    if "fov" in obs:
        shapes, moved = snap_polygons_to_fovs(shapes, gx, gy, obs["fov"].to_numpy())
        if len(moved):
            print(f"  moved polygons of {len(moved)} FOV(s) onto their cell centroids")
    shapes = ShapesModel.parse(shapes)

    table.obs["region"] = pd.Categorical(["cells"] * len(table))
    tbl = TableModel.parse(table, region="cells", region_key="region",
                           instance_key="cell")

    n_poly = len(cells) - len(missing)
    print(f"  {args.sample}: {len(cells):,} cells x {len(genes):,} features "
          f"({n_poly:,} polygons, {100*n_poly/len(cells):.1f}%), "
          f"donor_core={row['donor_core']}")

    if args.dry_run:
        print("  [dry-run] nothing written")
        return

    sdata = sd.SpatialData(shapes={"cells": shapes}, tables={"table": tbl})
    out = args.out_dir / f"{args.sample}.zarr"
    out.parent.mkdir(parents=True, exist_ok=True)
    sdata.write(out, overwrite=True)
    print(f"  wrote {out}")


if __name__ == "__main__":
    main()
