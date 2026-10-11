#!/usr/bin/env python3
"""
transfer_donor_core.py
Carry per-cell donor identity from one acquisition of a slide to another.

Donor is a property of the tissue, not of the scan, so where one acquisition of
a slide has resolved cores and another does not, the labels transfer by
position. Same approach used for v1's LN protein (FOV-layout transfer).

The two scans do NOT share a coordinate frame -- the slide was reloaded, so the
FOV grid and origin differ. The transform is recovered from the tissue
footprint (cell centroids rasterised at 100 um, source-coverage maximised over
translation via FFT and over the 8 flips/transposes), then each target cell
takes the core of its nearest labelled source cell.

Guards, because a wrong donor label is worse than none:
  * the target must cover --min-cover of the labelled source footprint, or the
    pair is refused
  * a cell whose nearest labelled neighbour is further than --max-dist um gets
    NA rather than a guess
  * per-core counts and the assigned fraction are reported for eyeballing

Usage:
    P=~/.venv-sd08/bin/python
    $P scripts/transfer_donor_core.py --zarr-dir /athena/project-saha/derived/build/zenodo_tier/<record_slug> \\
        --source SAHA_CR_LIV_A2_TAP01 --target SAHA_CP_LIV_A2_TAP01 --dry-run
"""

import argparse
import sys
from pathlib import Path

import numpy as np

BIN_MM = 0.1
PX_UM = 0.12028
XF = [
    ("id", lambda z: z),
    ("flipY", np.flipud),
    ("flipX", np.fliplr),
    ("rot180", lambda z: np.flipud(np.fliplr(z))),
    ("T", lambda z: z.T),
    ("Tf", lambda z: np.flipud(z.T)),
    ("Tfx", lambda z: np.fliplr(z.T)),
    ("T180", lambda z: np.flipud(np.fliplr(z.T))),
]


def raster(x_mm, y_mm, thresh=3):
    gx = np.floor((x_mm - x_mm.min()) / BIN_MM).astype(int)
    gy = np.floor((y_mm - y_mm.min()) / BIN_MM).astype(int)
    g = np.zeros((gy.max() + 1, gx.max() + 1), int)
    np.add.at(g, (gy, gx), 1)
    return g >= thresh


def best_alignment(A, B):
    """Best (cover, iou, name, dy, dx, shape) placing B onto A.

    Maximises |A and B| / |A| -- the fraction of the SOURCE footprint the target
    covers -- not IoU. The source is the acquisition that carries donor labels
    and it routinely scans less tissue than the target, so IoU penalises a
    correct alignment simply for the target being larger. IoU is still reported
    as a diagnostic.
    """
    from numpy.fft import rfft2, irfft2
    best = (-1.0, 0.0, None, 0, 0, None)
    for name, T in XF:
        Bt = T(B)
        H, W = A.shape[0] + Bt.shape[0], A.shape[1] + Bt.shape[1]
        inter = np.rint(irfft2(rfft2(A.astype(np.float32), (H, W))
                               * rfft2(Bt[::-1, ::-1].astype(np.float32), (H, W)),
                               (H, W))).astype(np.int64)
        cover = inter / max(int(A.sum()), 1)
        idx = np.unravel_index(cover.argmax(), cover.shape)
        if cover[idx] > best[0]:
            iou = inter[idx] / max(int(A.sum()) + int(Bt.sum()) - int(inter[idx]), 1)
            best = (float(cover[idx]), float(iou), name,
                    idx[0] - Bt.shape[0] + 1, idx[1] - Bt.shape[1] + 1, Bt.shape)
    return best


def apply_xf(name, x, y, w, h):
    """Same 8 operations as XF, in grid-bin coordinates."""
    if name == "id":     return x, y
    if name == "flipY":  return x, h - y
    if name == "flipX":  return w - x, y
    if name == "rot180": return w - x, h - y
    if name == "T":      return y, x
    if name == "Tf":     return y, w - x
    if name == "Tfx":    return h - y, x
    if name == "T180":   return h - y, w - x
    raise ValueError(name)


def coords_mm(table):
    xy = np.asarray(table.obsm["spatial"], dtype="float64")
    return xy[:, 0] * PX_UM / 1000.0, xy[:, 1] * PX_UM / 1000.0


def transfer(zdir: Path, source: str, target: str, min_cover: float,
             max_dist_um: float, dry: bool):
    import anndata as ad
    import pandas as pd
    import spatialdata as sd
    from scipy.spatial import cKDTree

    src = ad.read_zarr(zdir / f"{source}.zarr" / "tables" / "table")
    if "donor_core" not in src.obs.columns:
        sys.exit(f"ERROR: {source} has no donor_core")
    sx, sy = coords_mm(src)
    lab = src.obs["donor_core"].astype(object).to_numpy()
    keep = pd.notna(lab)
    if keep.sum() == 0:
        sys.exit(f"ERROR: {source}.donor_core is empty")
    sx, sy, lab = sx[keep], sy[keep], lab[keep]

    sdata = sd.read_zarr(zdir / f"{target}.zarr")
    tgt = sdata.tables["table"]
    tx, ty = coords_mm(tgt)

    A = raster(sx, sy)                        # source footprint (labelled cells)
    B = raster(tx, ty)                        # target footprint
    cover, iou, name, dy, dx, bshape = best_alignment(A, B)
    print(f"  align {target} -> {source}: source-coverage {cover:.2f} "
          f"(IoU {iou:.2f}), {name}, shift ({dx}, {dy}) bins")
    if cover < min_cover:
        print(f"  REFUSED: coverage {cover:.2f} < {min_cover} — the target does "
              f"not contain the labelled tissue")
        return None

    # target bins -> transformed -> source bins -> mm in the source frame
    bx = (tx - tx.min()) / BIN_MM
    by = (ty - ty.min()) / BIN_MM
    h, w = B.shape[0] - 1, B.shape[1] - 1
    ax, ay = apply_xf(name, bx, by, w, h)
    mx = (ax + dx) * BIN_MM + sx.min()
    my = (ay + dy) * BIN_MM + sy.min()

    d, i = cKDTree(np.c_[sx, sy]).query(np.c_[mx, my], k=1)
    far = d * 1000.0 > max_dist_um
    out = np.where(far, None, lab[i])
    n = int((~far).sum())
    print(f"  assigned {n:,}/{len(out):,} ({100*n/len(out):.1f}%); "
          f"{int(far.sum()):,} beyond {max_dist_um:.0f} um left NA")
    import collections
    for k, v in sorted(collections.Counter(x for x in out if x is not None).items()):
        print(f"      {k:14s} {v:8,d}")
    if n == 0:
        print("  nothing assigned — not writing")
        return None
    if dry:
        print("  [dry run — nothing written]")
        return None

    tgt.obs["donor_core"] = pd.Categorical(out)
    sdata.delete_element_from_disk("table")
    sdata.write_element("table")
    back = sd.read_zarr(zdir / f"{target}.zarr").tables["table"]
    print(f"  WROTE donor_core: {int(back.obs['donor_core'].notna().sum()):,} non-NA, "
          f"{back.obs['donor_core'].nunique()} cores")
    return n


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--zarr-dir", type=Path, required=True)
    ap.add_argument("--source", required=True)
    ap.add_argument("--target", required=True)
    ap.add_argument("--min-cover", type=float, default=0.80,
                    help="minimum fraction of the labelled source footprint the "
                         "target must cover (default 0.80)")
    ap.add_argument("--max-dist", type=float, default=150.0,
                    help="um; a cell further than this from any labelled source "
                         "cell gets NA (default 150)")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    print(f"=== {a.source} -> {a.target}")
    transfer(a.zarr_dir, a.source, a.target, a.min_cover, a.max_dist, a.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
