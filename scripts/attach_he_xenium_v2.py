#!/usr/bin/env python3
"""
attach_he_xenium_v2.py
Attach the registered serial-section H&E to a v2 Xenium SpatialData object.

register_he_xenium.py already computed the transform and passed its own QC; it
simply never wrote the image into the store. This does that, and nothing else.

Units -- the part that is easy to get wrong
-------------------------------------------
The Xenium object's `global` coordinate system is PIXELS, not microns. Verified
rather than assumed:

    image[morphology_focus]   Identity,              45558 x 71478 px
    label[cell_labels]        Identity,              45558 x 71478 px
    shape[cell_boundaries]    Scale [4.70588, ...],  stored 25.1..9674.8

4.70588 is 1/0.2125, the Xenium pixel size, so the shapes are stored in microns
and scaled up into pixel space. `obsm['spatial']` also holds microns, but obsm
carries no transform and therefore says nothing about the coordinate system --
reading it as if it did is what makes this look like a scale bug when it is not.

The stored affine maps H&E full-res px -> Xenium global MICRONS, so it must be
converted to pixels before use:

    A_global = diag([1/px_um, 1/px_um, 1]) @ A_um
    A_level  = A_global @ diag([ds, ds, 1])     # coarser pyramid level

Accuracy
--------
The Xenium registration JSON has no `refined` key, so it is a pass-1 mask fit:
~10-20 um, per register_he.py's own reporting. Combined with
`cell_centroid_on_tissue_frac` around 0.58 -- expected for a SERIAL section,
where tissue moves between cuts -- the overlay is good for orientation and
region context, NOT for cell-to-cell comparison. That is recorded in the image's
metadata so downstream users are not misled.

Writes a NEW object; the source store is never modified. v1 established that
in-place SpatialData writes can destroy tables.

Usage:
    P=~/.venv-sd08/bin/python
    $P scripts/attach_he_xenium_v2.py \\
        --object .../processed/zarr/SAHA_XR_PROS_A2.zarr \\
        --he .../processed/he/PROS_A2_Serial_Section_H&E.svs \\
        --registration .../qc/registration_xenium/..._registration.json \\
        --out .../derived/v2_objects/SAHA_XR_PROS_A2.zarr [--he-level 2] [--dry-run]
"""

import argparse
import json
import sys
from pathlib import Path

import numpy as np


def he_dask(path: Path, level: int = 0, chunk: int = 2048):
    """Lazily read an Aperio SVS as a (y, x, c) dask array.

    Same approach as merge_slide.he_dask: a fresh OpenSlide handle per task,
    because handles cannot be shared across dask workers.
    """
    import dask
    import dask.array as da
    import openslide

    sl = openslide.OpenSlide(str(path))
    try:
        W, H = sl.level_dimensions[level]
        ds = float(sl.level_downsamples[level])
        mpp = float(sl.properties.get(openslide.PROPERTY_NAME_MPP_X, 0.2476))
    finally:
        sl.close()

    def tile(y0, x0, h, w):
        s = openslide.OpenSlide(str(path))
        try:
            return np.array(s.read_region((int(x0 * ds), int(y0 * ds)), level,
                                          (w, h)).convert("RGB"))
        finally:
            s.close()

    rows = []
    for y0 in range(0, H, chunk):
        h = min(chunk, H - y0)
        cols = [da.from_delayed(dask.delayed(tile)(y0, x0, h, min(chunk, W - x0)),
                                shape=(h, min(chunk, W - x0), 3), dtype=np.uint8)
                for x0 in range(0, W, chunk)]
        rows.append(da.concatenate(cols, axis=1))
    return da.concatenate(rows, axis=0), mpp * ds, mpp


def corners(A, w, h):
    """The four image corners pushed through a 3x3 affine (x, y order)."""
    pts = np.array([[0, 0, 1], [w, 0, 1], [0, h, 1], [w, h, 1]], float).T
    out = A @ pts
    return out[0], out[1]


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--object", type=Path, required=True)
    ap.add_argument("--he", type=Path, required=True)
    ap.add_argument("--registration", type=Path,
                    help="register_he_xenium.py JSON (pass-1 mask fit). Used only "
                         "if --st2he-dir is not given; the st2he fit is better.")
    ap.add_argument("--st2he-dir", type=Path,
                    help="st2he-toolkit run dir (alignment_matrix_level0.csv + "
                         "warped_spots_level0.csv + metrics.json). Preferred: it is "
                         "a refined affine and ships per-cell correspondences, so "
                         "the transform can be verified cell by cell.")
    ap.add_argument("--max-residual-um", type=float, default=5.0,
                    help="with --st2he-dir, fail if the median per-cell residual "
                         "between warped_spots and the applied transform exceeds this")
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--he-level", type=int, default=2)
    ap.add_argument("--element", default="he_serial_section")
    ap.add_argument("--min-overlap", type=float, default=0.70,
                    help="refuse to write if the H&E footprint covers less than "
                         "this fraction of the cell bounding box on either axis")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    import warnings
    warnings.filterwarnings("ignore")
    from spatialdata import read_zarr
    from spatialdata.models import Image2DModel
    from spatialdata.transformations import Affine

    if not args.st2he_dir and not args.registration:
        sys.exit("ERROR: pass --st2he-dir (preferred) or --registration")

    import pandas as pd

    if args.st2he_dir:
        # st2he's matrix maps ST microns -> H&E level-0 px. We need the opposite,
        # H&E px -> microns, so invert it.
        M = np.loadtxt(args.st2he_dir / "alignment_matrix_level0.csv", delimiter=",")
        A_um = np.linalg.inv(M)
        reg = json.loads((args.st2he_dir / "manifest.json").read_text())
        met = json.loads((args.st2he_dir / "metrics.json").read_text())
        reg = {
            "method": "st2he-toolkit align.py (refined affine)",
            "orientation": f"theta {met['affine']['theta_deg']:.2f} deg",
            "final_coverage": reg["scores"]["spots_on_tissue"],
            "cell_centroid_on_tissue_frac": reg["scores"]["spots_on_tissue"],
            "refined": True,
            "shear": met["affine"]["shear"],
            "nit": met["affine"]["nit"],
            "frac_out_of_bounds": reg["qc"]["frac_out_of_bounds"],
        }
    else:
        reg = json.loads(args.registration.read_text())
        key = "affine_he_fullres_px_to_xenium_global_um"
        if key not in reg:
            sys.exit(f"ERROR: {args.registration} has no '{key}' (keys: {sorted(reg)})")
        A_um = np.array(reg[key], float)
        reg["method"] = "register_he_xenium.py (pass-1 mask fit)"

    sd = read_zarr(args.object)
    t = sd.tables["table"]
    px_um = t.uns.get("pixel_size_um")
    if not px_um:
        sys.exit("ERROR: object table has no uns['pixel_size_um']; cannot convert "
                 "the micron affine into the object's pixel frame")

    # microns -> global pixels
    A_global = np.diag([1.0 / px_um, 1.0 / px_um, 1.0]) @ A_um

    arr, he_um, he_mpp = he_dask(args.he, args.he_level)
    ds = he_um / he_mpp
    A_lvl = A_global @ np.diag([ds, ds, 1.0])
    H, W = arr.shape[0], arr.shape[1]

    print(f"  object      : {args.object.name}")
    print(f"  pixel_size  : {px_um} um/px  -> global is PIXELS")
    print(f"  H&E level {args.he_level}: {H} x {W} at {he_um:.4f} um/px (full-res {he_mpp:.4f}); ds={ds:.1f}")
    print(f"  registration: {reg.get('method')}")
    print(f"                orientation {reg.get('orientation')}, "
          f"on-tissue {reg.get('cell_centroid_on_tissue_frac', float('nan')):.3f}, "
          f"refined={bool(reg.get('refined'))}")

    # With st2he we have per-cell correspondences, so verify the transform cell by
    # cell instead of trusting a bounding-box overlap. Pushing each cell's recorded
    # H&E pixel position through A_global must land on that cell's own position in
    # the object. A wrong inverse, a unit slip or a flipped axis all show up here as
    # a large residual; none of them would move the bounding box much.
    if args.st2he_dir:
        ws = pd.read_csv(args.st2he_dir / "warped_spots_level0.csv")
        ws = ws[ws["in_bounds"].astype(bool)]
        t_obs = sd.tables["table"].obs
        idcol = "cell_id" if "cell_id" in t_obs.columns else None
        obs_ids = (t_obs[idcol].astype(str).to_numpy() if idcol
                   else t_obs.index.astype(str).to_numpy())
        pos = pd.Series(np.arange(len(obs_ids)), index=obs_ids)
        hit = ws["cell_id"].astype(str).map(pos)
        ws = ws[hit.notna()]
        rows = hit.dropna().astype(int).to_numpy()
        if len(ws) < 1000:
            sys.exit(f"ERROR: only {len(ws)} warped spots matched cells in the object "
                     f"-- cannot verify the transform")
        he_xy = np.column_stack([ws["x_he_l0"].to_numpy(), ws["y_he_l0"].to_numpy(),
                                 np.ones(len(ws))])
        got = (A_global @ he_xy.T).T[:, :2]      # H&E l0 px -> object global px
        cbv = sd.shapes["cell_boundaries"]
        from spatialdata.transformations import get_transformation as _gt
        sc0 = _gt(cbv)
        s0 = getattr(sc0, "scale", np.array([1.0, 1.0]))
        cc = cbv.geometry.centroid
        want = np.column_stack([cc.x.to_numpy() * float(s0[0]),
                                cc.y.to_numpy() * float(s0[1])])[rows]
        resid_px = np.linalg.norm(got - want, axis=1)
        resid_um = resid_px * px_um
        print(f"  per-cell check: {len(ws):,} cells matched, residual um "
              f"median {np.median(resid_um):.3f}  p95 {np.percentile(resid_um, 95):.3f}  "
              f"max {resid_um.max():.1f}")
        if np.median(resid_um) > args.max_residual_um:
            sys.exit(f"ERROR: median per-cell residual {np.median(resid_um):.2f} um "
                     f"exceeds {args.max_residual_um} um -- the transform is wrong; "
                     f"refusing to write")

    # Does the H&E actually land on the cells, in the object's own frame?
    hx, hy = corners(A_lvl, W, H)
    cb = sd.shapes["cell_boundaries"]
    from spatialdata.transformations import get_transformation
    sc = get_transformation(cb)
    s = getattr(sc, "scale", np.array([1.0, 1.0]))
    c = cb.geometry.centroid
    cx = c.x.to_numpy() * float(s[0]); cy = c.y.to_numpy() * float(s[1])

    ok = True
    for name, h, cc in (("x", hx, cx), ("y", hy, cy)):
        lo = max(h.min(), cc.min()); hi = min(h.max(), cc.max())
        span = cc.max() - cc.min()
        frac = max(0.0, hi - lo) / span
        print(f"  {name}: H&E {h.min():10.1f}..{h.max():10.1f}   "
              f"cells {cc.min():10.1f}..{cc.max():10.1f}   covers {frac:.3f}")
        if frac < args.min_overlap:
            ok = False
    if not ok:
        sys.exit(f"ERROR: H&E footprint covers less than {args.min_overlap:.0%} of the "
                 f"cell extent -- the affine or its unit conversion is wrong; "
                 f"refusing to write")

    img = Image2DModel.parse(
        arr, dims=("y", "x", "c"), c_coords=["r", "g", "b"],
        transformations={"global": Affine(A_lvl, input_axes=("x", "y"),
                                          output_axes=("x", "y"))},
        scale_factors=[2, 2, 2, 2], chunks={"c": 3, "y": 2048, "x": 2048},   # by name: a tuple is applied as (c, y, x)
    )
    sd.images[args.element] = img

    # Say out loud, in the object, how far this overlay can be trusted.
    note = ("Serial section: the H&E is a DIFFERENT cut from the measured section, "
            "so tissue architecture corresponds only approximately and individual "
            "cells do not. Use for orientation and region-level context.")
    if not reg.get("refined"):
        note = "Pass-1 mask fit, ~10-20 um. " + note
    sd.tables["table"].uns[f"{args.element}_registration"] = {
        "source": str(args.he),
        "section": "serial",
        "method": reg.get("method"),
        "orientation": reg.get("orientation"),
        "on_tissue_frac": reg.get("cell_centroid_on_tissue_frac"),
        "refined": bool(reg.get("refined")),
        "accuracy_note": note,
    }

    if args.dry_run:
        print("\n[dry-run] nothing written")
        return 0

    args.out.parent.mkdir(parents=True, exist_ok=True)
    print(f"\n  writing {args.out}")
    sd.write(args.out)

    # Re-read: a write that reports success and drops elements is the v1 lesson.
    back = read_zarr(args.out)
    assert args.element in back.images, f"{args.element} missing after write"
    assert set(back.tables) == set(sd.tables), "tables changed across write"
    for k in ("cell_boundaries", "nucleus_boundaries"):
        assert k in back.shapes, f"shape {k} lost across write"
    n_before = sd.tables["table"].n_obs
    n_after = back.tables["table"].n_obs
    assert n_before == n_after, f"cell count changed {n_before} -> {n_after}"
    print(f"  verified: {args.element} present, {len(back.images)} images, "
          f"{len(back.shapes)} shapes, {n_after:,} cells")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
