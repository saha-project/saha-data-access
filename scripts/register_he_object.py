#!/usr/bin/env python3
"""
register_he_object.py
Register an H&E whole-slide image to a release SpatialData object's own frame.

The v2 pipeline (2026_SAHA_data/processed/code/register_he.py) registers H&E to
a mosaic it rebuilds from raw CellStatsDir + FOV positions in AtoMx global px.
Most v1 exports give FOV positions only as stage millimetres, so rebuilding
that frame would mean re-deriving each object's pixel convention -- and a
silent flip or offset there is exactly the error that has passed checks before.
Instead this registers against the object's OWN nuclear image (CosMx DNA,
Xenium DAPI), read through its stored transform, so the result is in the
object's `global` frame by construction. The search itself is v2's, unchanged:
tissue masks, 8 dihedral orientations by phase correlation, then ECC affine.

Writes, per object, into --out:
    <sample>.he_registration.json   affine (H&E full-res px -> object global px)
                                    + coverage + cells-on-H&E-tissue
    <sample>.he_qc.jpg              whole object + zoom insets, cells overlaid

Accuracy measures
    coverage            fraction of the object's tissue mask covered by H&E tissue
    cells_on_he_tissue  fraction of cell centroids that land on H&E tissue.
                        Same section: ~0.95+. Serial section: lower, since
                        tissue moves between cuts. A wrong fit: ~background.

Usage (on Cayuga):
    P=~/.venv-sd08/bin/python
    $P scripts/register_he_object.py --object .../SAHA_CR_COL_A1_EC02.zarr \
        --he .../SAHA_HECR_COL_A1_EC02.svs --section same --out .../he_v1/reg
"""

import argparse
import json
import sys
import warnings
from pathlib import Path

import numpy as np

V2_CODE = Path("/athena/masonlab/scratch/users/jip2007/2026_SAHA_data/processed/code")
sys.path.insert(0, str(V2_CODE))

UM_PER_GLOBAL_PX = {"cosmx": 0.12028, "xenium": 0.2125}


def log(m):
    print(f"[register_he_object] {m}", flush=True)


def pixel_to_global(el, level_name):
    """3x3 (col,row) -> global (x,y) for one pyramid level of an image element."""
    from spatialdata.transformations import get_transformation
    names = list(el.children)
    base = next(iter(el[names[0]].data_vars.values()))
    arr = next(iter(el[level_name].data_vars.values()))
    M = get_transformation(base, "global").to_affine_matrix(("x", "y"), ("x", "y"))
    s = np.diag([base.shape[-1] / arr.shape[-1], base.shape[-2] / arr.shape[-2], 1.0])
    return arr, M @ s


def nuclear_channel(arr):
    chans = [str(c) for c in arr.coords["c"].values] if "c" in arr.coords else []
    for i, c in enumerate(chans):
        if any(k in c for k in ("DNA", "DAPI", "Nuclear")):
            return i
    return 0


def is_ome(path) -> bool:
    """OpenSlide opens OME-TIFF pyramids as ONE full-resolution level (a 19 GB read for
    Xenium_WCM_APE); tifffile sees all levels. Route OME files to tifffile."""
    return str(path).lower().endswith((".ome.tif", ".ome.tiff"))


def read_he_any(path: Path, target_um: float, mpp_override=None):
    """(inverted gray at ~target_um, level um/px, full-res mpp). SVS via openslide,
    OME-TIFF via tifffile (openslide cannot read every OME pyramid)."""
    import cv2
    try:
        if is_ome(path):
            raise ValueError("OME-TIFF: read the pyramid with tifffile")
        import openslide
        sl = openslide.OpenSlide(str(path))
        mpp = float(mpp_override or sl.properties.get(openslide.PROPERTY_NAME_MPP_X) or 0)
        if not mpp:
            raise ValueError("no mpp in slide properties")
        lvl = int(np.argmin([abs(d - target_um / mpp) for d in sl.level_downsamples]))
        img = np.array(sl.read_region((0, 0), lvl, sl.level_dimensions[lvl]).convert("RGB"))
        um = mpp * float(sl.level_downsamples[lvl])
        sl.close()
    except Exception as e:
        if mpp_override and path.suffix.lower() in (".tif", ".tiff"):
            import tifffile          # flat RGB export with no pyramid and no scale
            img = tifffile.imread(path)[..., :3]
            k = max(1, int(round(target_um / mpp_override)))
            img = img[::k, ::k]
            um, mpp = mpp_override * k, mpp_override
            gray = 255.0 - cv2.cvtColor(img.astype(np.uint8), cv2.COLOR_RGB2GRAY).astype(np.float32)
            log(f"H&E {path.name}: flat TIFF, assumed {mpp_override:.3f} um/px, read at {um:.2f}")
            return gray, um, mpp, img
        log(f"openslide could not use {path.name} ({e}); trying OME-TIFF")
        import tifffile
        import xml.etree.ElementTree as ET
        with tifffile.TiffFile(path) as tf:
            ome = ET.fromstring(tf.ome_metadata)
            px = next(el for el in ome.iter() if el.tag.endswith("Pixels"))
            mpp = float(px.get("PhysicalSizeX"))
            levels = tf.series[0].levels
            full_w = levels[0].shape[-2] if levels[0].shape[-1] in (3, 4) else levels[0].shape[-1]
            def width(l):
                return l.shape[-2] if l.shape[-1] in (3, 4) else l.shape[-1]
            lvl = int(np.argmin([abs(full_w / width(l) - target_um / mpp) for l in levels]))
            img = levels[lvl].asarray()
            if img.shape[0] in (3, 4) and img.ndim == 3:
                img = np.moveaxis(img, 0, -1)
            img = img[..., :3]
            um = mpp * full_w / width(levels[lvl])
    gray = 255.0 - cv2.cvtColor(img.astype(np.uint8), cv2.COLOR_RGB2GRAY).astype(np.float32)
    log(f"H&E {path.name}: {gray.shape[1]}x{gray.shape[0]} at {um:.2f} um/px (full res {mpp:.4f})")
    return gray, um, mpp, img


class HERegions:
    """Read H&E regions at any pyramid level: Aperio SVS via openslide, OME-TIFF via tifffile."""

    def __init__(self, path: Path):
        self.path = Path(path)
        try:
            if is_ome(path):
                raise ValueError("OME-TIFF")
            import openslide
            self.sl = openslide.OpenSlide(str(path))
            self.ds = [float(d) for d in self.sl.level_downsamples]
            self.kind = "openslide"
        except Exception:
            import tifffile
            import zarr
            tf = tifffile.TiffFile(path)
            g = zarr.open(tf.series[0].aszarr(), mode="r")   # the whole pyramid, one group
            self.levels = ([g] if hasattr(g, "shape") else
                           [g[str(i)] for i in range(len(tf.series[0].levels))])
            self.rgb_last = tf.series[0].levels[0].shape[-1] in (3, 4)
            w0 = self._w(self.levels[0])
            self.ds = [w0 / self._w(l) for l in self.levels]
            self.kind = "ome"

    def _w(self, l):
        return l.shape[-2] if l.shape[-1] in (3, 4) else l.shape[-1]

    def level_near(self, want_ds):
        return int(np.argmin([abs(d - want_ds) for d in self.ds]))

    def read(self, level, x0, y0, w, h):
        """RGB uint8 (h, w, 3) at `level`; x0/y0 in that level's pixels, clipped to the slide."""
        x0, y0 = max(int(x0), 0), max(int(y0), 0)
        if self.kind == "openslide":
            d = self.ds[level]
            return np.array(self.sl.read_region((int(x0 * d), int(y0 * d)), level, (int(w), int(h))).convert("RGB"))
        l = self.levels[level]
        a = l[y0:y0 + int(h), x0:x0 + int(w)] if self.rgb_last else np.moveaxis(l[:, y0:y0 + int(h), x0:x0 + int(w)], 0, -1)
        return np.asarray(a)[..., :3].astype(np.uint8)


def refine(A_full, el, um_g, he: "HERegions", mpp, cx, cy, log=print):
    """Nuclei-level correction of the mask fit, coarse to fine (after refine_he.py, v2).

    The mask fit is good to a few tens of microns -- on SAHA_CR_COL_A1_EC02 it
    left every cell ~35 um off, inside the crypt lumens. Per 300 um window at
    ~1 um/px, the H&E haematoxylin channel is warped into the object's DNA image
    through the current affine and phase-correlated against it.

    The per-window shifts are fit by RANSAC, not least squares: periodic tissue
    (colon crypts every ~100-150 um) makes some windows lock onto a neighbouring
    crypt and report a confident but wrong shift, which pulls any averaging fit
    off. RANSAC keeps the correction most windows agree on (within 12 um) and
    refits on those. Larger windows were tried and are worse: they straddle FOV
    gaps, which dominate the correlation.
    Returns (A_new, offset_um_before, offset_um_after, n_windows_used).
    """
    import cv2
    from skimage.color import rgb2hed
    from skimage.registration import phase_cross_correlation

    def level_near(want_um):
        best = None
        for lv in el.children:
            arr, G = pixel_to_global(el, lv)
            um = um_g * np.sqrt(abs(np.linalg.det(G[:2, :2])))
            if best is None or abs(np.log(um / want_um)) < abs(np.log(best[3] / want_um)):
                best = (lv, arr, G, um)
        return best

    def centers(window_um, n):
        win_g = window_um / um_g
        bins = max(3, int((cx.max() - cx.min()) / win_g))
        H, xe, ye = np.histogram2d(cx, cy, bins=bins)
        thr = np.percentile(H[H > 0], 50)
        cand = [((xe[i] + xe[i + 1]) / 2, (ye[j] + ye[j + 1]) / 2)
                for i, j in zip(*np.nonzero(H >= thr))]
        rng = np.random.default_rng(0)
        return [cand[k] for k in sorted(rng.choice(len(cand), min(n, len(cand)), replace=False))]

    def measure(A, lv, arr, G1, um1, window_um, pts):
        ch = nuclear_channel(arr)
        he_lvl = he.level_near(um1 / mpp)
        dsl = he.ds[he_lvl]
        half = int(window_um / um1 / 2)
        invG1 = np.linalg.inv(G1)
        out = []
        for gx, gy in pts:
            c0, r0 = (invG1 @ [gx, gy, 1])[:2]
            ry0, rx0 = int(r0) - half, int(c0) - half
            if ry0 < 0 or rx0 < 0:
                continue
            ref = np.asarray(arr.values[ch, ry0:ry0 + 2 * half, rx0:rx0 + 2 * half], dtype=np.float32)
            if ref.shape != (2 * half, 2 * half):
                continue
            T = np.array([[1, 0, rx0], [0, 1, ry0], [0, 0, 1]], float)
            W = np.diag([1 / dsl, 1 / dsl, 1]) @ np.linalg.inv(A) @ G1 @ T   # ref px -> H&E level px
            cs = W @ np.array([[0, 2 * half, 0, 2 * half], [0, 0, 2 * half, 2 * half], [1, 1, 1, 1]])
            x0, y0 = cs[0].min() - 20, cs[1].min() - 20
            if x0 < 0 or y0 < 0:
                continue
            rgb = he.read(he_lvl, x0, y0, cs[0].max() - x0 + 40, cs[1].max() - y0 + 40)
            if rgb.ndim != 3 or min(rgb.shape[:2]) < 8:          # window runs off the scan
                continue
            Wc = np.array([[1, 0, -x0], [0, 1, -y0], [0, 0, 1]]) @ W
            hema = rgb2hed(rgb)[..., 0].astype(np.float32)
            mov = cv2.warpAffine(hema, Wc[:2], (2 * half, 2 * half),
                                 flags=cv2.INTER_LINEAR | cv2.WARP_INVERSE_MAP)
            refl = np.log1p(ref)
            # texture: nuclei must be present in both, not a gap or blank glass
            if refl.std() < 0.05 * (refl.mean() + 1e-6) or mov.std() < 1e-3 or (mov > 0).mean() < 0.5:
                continue
            a = (refl - refl.mean()) / refl.std()
            b = (mov - mov.mean()) / mov.std()
            (dy, dx), err, _ = phase_cross_correlation(a, b, upsample_factor=10, normalization=None)
            if max(abs(dx), abs(dy)) > 0.4 * half * 2:           # wrapped-around peak
                continue
            d_g = G1[:2, :2] @ [dx, dy]
            out.append((gx, gy, d_g[0], d_g[1], err))
        return np.array(out)

    def fit(m, tol_um=12.0, iters=500):
        """RANSAC affine on (window centre -> centre + shift); returns (C, inlier mask)."""
        src, dst = m[:, :2], m[:, :2] + m[:, 2:4]
        X = np.c_[src, np.ones(len(src))]
        tol = tol_um / um_g
        rng = np.random.default_rng(0)
        best = (np.zeros(len(m), bool), np.eye(3))
        for _ in range(iters):
            k = rng.choice(len(m), 3, replace=False)
            try:
                C = np.linalg.solve(X[k], dst[k]).T
            except np.linalg.LinAlgError:
                continue
            inl = np.hypot(*((X @ C.T) - dst).T) < tol
            if inl.sum() > best[0].sum():
                best = (inl, C)
        inl = best[0]
        if inl.sum() < 3:                                   # no consensus: translation by median
            C = np.eye(3); C[:2, 2] = np.median(m[:, 2:4], axis=0)
            return C, np.ones(len(m), bool)
        C, *_ = np.linalg.lstsq(X[inl], dst[inl], rcond=None)
        return np.vstack([C.T, [0, 0, 1]]), inl

    def offset_um(m):
        return float(np.median(np.hypot(m[:, 2], m[:, 3]))) * um_g if len(m) else None

    A = A_full
    fine = level_near(1.0)
    pts = centers(300.0, 80)
    m0 = measure(A, *fine, 300.0, pts)
    if len(m0) < 6:
        log(f"refine: only {len(m0)} usable windows -- keeping the mask fit")
        return A_full, None, None, 0
    _, inl0 = fit(m0)
    before = offset_um(m0[inl0])
    for it in range(3):
        m = measure(A, *fine, 300.0, pts)
        C, inl = fit(m)
        A = C @ A
        log(f"refine pass {it + 1}: {inl.sum()}/{len(m)} windows agree; their median shift "
            f"{offset_um(m[inl]):.1f} um")
    m1 = measure(A, *fine, 300.0, pts)
    _, inl1 = fit(m1)
    after = offset_um(m1[inl1])
    frac = inl1.mean()
    log(f"refine: consensus offset {before:.1f} -> {after:.1f} um; "
        f"{inl1.sum()}/{len(m1)} windows agree after ({frac:.0%})")
    if after >= before:
        log("refine: no improvement -- keeping the mask fit")
        return A_full, before, before, 0
    return A, before, after, int(inl1.sum())


def cell_xy(sd, um_g):
    t = next(iter(sd.tables.values()))
    for xk, yk in (("CenterX_global_px", "CenterY_global_px"), ("x_centroid_px", "y_centroid_px")):
        if xk in t.obs:
            return t.obs[xk].to_numpy(float), t.obs[yk].to_numpy(float)
    sp = np.asarray(t.obsm["spatial"])[:, :2] / um_g             # Xenium: microns -> global px
    return sp[:, 0], sp[:, 1]


def density_reference(cx, cy, um_g, target_um):
    """No image (e.g. TAP protein rebuilt from RDS): rasterise cell centroids instead."""
    b = target_um / um_g
    x0, y0 = cx.min() - 20 * b, cy.min() - 20 * b
    W, H = int((cx.max() - x0) / b) + 40, int((cy.max() - y0) / b) + 40
    img = np.zeros((H, W), np.float32)
    np.add.at(img, (((cy - y0) / b).astype(int), ((cx - x0) / b).astype(int)), 1)
    G = np.array([[b, 0, x0], [0, b, y0], [0, 0, 1]], float)    # density px -> global px
    return np.log1p(img), G


def register(obj: Path, he: Path, section: str, out: Path, target_um: float, he_mpp=None,
             orientation=None, he_roi=None, tag=""):
    warnings.filterwarnings("ignore")
    import cv2
    from skimage.registration import phase_cross_correlation
    from spatialdata import read_zarr
    from cosmx_mosaic import DIHEDRAL, coverage, ecc_refine, orient_affine, tissue_mask, warp

    sid = obj.name.removesuffix(".zarr")
    platform = "xenium" if "_XR_" in sid else "cosmx"
    sd = read_zarr(obj)
    um_g = UM_PER_GLOBAL_PX[platform]
    cx, cy = cell_xy(sd, um_g)
    name = next((k for k in ("morphology", "morphology_focus") if k in sd.images), None)
    el = sd.images[name] if name else None

    if el is None:
        ref, G = density_reference(cx, cy, um_g, target_um)
        ref_um, lv, name = target_um, "cell density", "cells"
    else:                                                  # pyramid level nearest target_um
        best = None
        for lv in el.children:
            arr, M = pixel_to_global(el, lv)
            um = um_g * float(np.sqrt(abs(np.linalg.det(M[:2, :2]))))
            if best is None or abs(um - target_um) < abs(best[2] - target_um):
                best = (arr, M, um, lv)
        arr, G, ref_um, lv = best
        ref = np.asarray(arr.values[nuclear_channel(arr)], dtype=np.float32)
        ref = np.log1p(ref)                                # fluorescence is heavy-tailed
    log(f"{sid}: {name}/{lv} {ref.shape[1]}x{ref.shape[0]} at {ref_um:.2f} um/px")
    ref_mask = tissue_mask(ref)
    ref_f = cv2.GaussianBlur(ref_mask.astype(np.float32), (0, 0), 3)

    if he_mpp == "auto":     # no scale on file: match tissue AREA to the object's (orientation-free)
        import tifffile
        raw = tifffile.imread(he)[..., :3]
        hm = tissue_mask(255.0 - cv2.cvtColor(raw.astype(np.uint8), cv2.COLOR_RGB2GRAY).astype(np.float32))
        he_mpp = float(np.sqrt(ref_mask.sum() * ref_um ** 2 / hm.sum()))
        log(f"estimated H&E scale from tissue area: {he_mpp:.3f} um/px")
    he_gray, he_um, mpp, he_rgb = read_he_any(he, target_um, float(he_mpp) if he_mpp else None)
    he_mask = tissue_mask(he_gray)
    background_mask, search, T0 = he_mask, he_mask, np.eye(3)
    if he_roi:   # several sections on one scan: search only the one this object was imaged on.
        # CROP, not mask: phase correlation cannot tell a large shift from its wrap-around, so a
        # section far down the scan is found at the wrapped (empty) position (MS01_02/03, 2026-10-08).
        x0, y0, x1, y1 = (max(int(round(v * mpp / he_um)), 0) for v in he_roi)
        keep = np.zeros_like(he_mask)
        keep[y0:y1, x0:x1] = True
        he_mask = he_mask & keep
        search = he_mask[y0:y1, x0:x1]
        T0 = np.array([[1, 0, -x0], [0, 1, -y0], [0, 0, 1]], float)   # H&E level px -> crop px
        log(f"H&E search limited to full-res px box {he_roi}")
    rel = he_um / ref_um
    he_s = cv2.resize(search.astype(np.float32), None, fx=rel, fy=rel, interpolation=cv2.INTER_AREA)
    S = np.diag([rel, rel, 1.0])

    best = None
    for oname, fn, lin in [d for d in DIHEDRAL if orientation in (None, d[0])]:
        oriented = fn(he_s)
        cand = cv2.GaussianBlur(oriented, (0, 0), 3)
        O = orient_affine(lin, he_s.shape)
        Hc, Wc = max(ref_f.shape[0], cand.shape[0]), max(ref_f.shape[1], cand.shape[1])
        A = np.zeros((Hc, Wc), np.float32); A[:ref_f.shape[0], :ref_f.shape[1]] = ref_f
        B = np.zeros((Hc, Wc), np.float32); B[:cand.shape[0], :cand.shape[1]] = cand
        (dy, dx), _, _ = phase_cross_correlation(A, B, upsample_factor=4, normalization=None)
        M_s = np.array([[1, 0, float(dx)], [0, 1, float(dy)], [0, 0, 1]]) @ O
        score = coverage(warp(he_s, M_s, ref_f.shape), ref_mask)
        log(f"  {oname:8s} coverage={score:.3f}")
        if best is None or score > best[0]:
            best = (score, oname, M_s)
    score, oname, M_s = best
    R, _ = ecc_refine(ref_f, warp(he_s, M_s, ref_f.shape), log)
    refined = R @ M_s
    rscore = coverage(warp(he_s, refined, ref_f.shape), ref_mask)
    M_ref, final, used = (refined, rscore, "ecc_refined") if rscore >= score else (M_s, score, "coarse_only")

    D = np.diag([mpp / he_um, mpp / he_um, 1.0])            # H&E full-res px -> H&E level px
    A_mask = G @ M_ref @ S @ T0 @ D                           # H&E full-res px -> object global px

    # cells on H&E tissue: centroids -> H&E level px, sample the H&E tissue mask
    heR = HERegions(he)
    if el is None:   # nothing to match nuclei against: tissue-level fit only
        A_full, res_before, res_after, n_win = A_mask, None, None, 0
    else:
        A_full, res_before, res_after, n_win = refine(A_mask, el, um_g, heR, mpp, cx, cy, log=log)
    inv = np.linalg.inv(A_full)
    pts = np.c_[cx, cy, np.ones_like(cx)] @ inv.T              # -> H&E full-res px
    hp = pts[:, :2] * (mpp / he_um)                            # -> H&E level px
    c, r = hp[:, 0].astype(int), hp[:, 1].astype(int)
    inside = (c >= 0) & (c < he_mask.shape[1]) & (r >= 0) & (r < he_mask.shape[0])
    on = float(he_mask[r[inside], c[inside]].mean()) if inside.any() else 0.0
    cells_on = on * inside.mean()
    rng = np.random.default_rng(0)
    ry, rx = rng.integers(0, he_mask.shape[0], 20000), rng.integers(0, he_mask.shape[1], 20000)
    background = float(background_mask[ry, rx].mean())   # whole scan, so --he-roi cannot lower the bar
    log(f"{sid}: {oname}/{used} coverage {final:.3f}; cells on H&E tissue {cells_on:.3f} "
        f"(random H&E pixel {background:.3f})")

    out.mkdir(parents=True, exist_ok=True)
    rec = {"sample_id": sid, "object": str(obj), "he_file": str(he), "section": section,
           "he_mpp": mpp, "global_px_um": um_g, "reference": f"{name}/{lv}",
           "reference_um": ref_um, "orientation": oname, "fit": used,
           "coverage": final, "cells_on_he_tissue": cells_on,
           "nuclear_offset_um_mask_fit": res_before, "nuclear_offset_um_refined": res_after,
           "refine_windows": n_win, "affine_mask_fit": A_mask.tolist(),
           "he_tissue_fraction_random": background, "he_roi_fullres_px": he_roi,
           "affine_he_fullres_px_to_global_px": A_full.tolist()}
    stem = f"{sid}.{tag}" if tag else sid   # tagged names written directly: parallel runs of one
    (out / f"{stem}.he_registration.json").write_text(json.dumps(rec, indent=1))   # object raced on a rename
    qc_figure(sid, he_rgb, he_um, mpp, A_full, ref, G, cx, cy, rec, out / f"{stem}.he_qc.jpg", heR)
    return rec


def qc_figure(sid, he_rgb, he_um, mpp, A_full, ref, G, cx, cy, rec, path, heR=None):
    """Whole object + 2 insets: H&E in the object's global frame, cell centroids on top."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.transforms import Affine2D
    A_lvl = A_full @ np.diag([he_um / mpp, he_um / mpp, 1.0])   # H&E level px -> global
    fig, axes = plt.subplots(1, 3, figsize=(18, 6.4))
    rng = np.random.default_rng(0)
    sel = rng.choice(len(cx), min(len(cx), 80000), replace=False)
    # densest two 2x2-percentile windows for the insets
    H, xe, ye = np.histogram2d(cx, cy, bins=20)
    order = np.argsort(H.ravel())[::-1]
    wins = []
    for k in order:
        i, j = np.unravel_index(k, H.shape)
        cxw, cyw = (xe[i] + xe[i + 1]) / 2, (ye[j] + ye[j + 1]) / 2
        if all(abs(cxw - a) + abs(cyw - b) > (xe[1] - xe[0]) * 4 for a, b in wins):
            wins.append((cxw, cyw))
        if len(wins) == 2:
            break
    half = 250 / rec["global_px_um"]                              # 500 um windows
    for ax, win in zip(axes, [None] + wins):
        if win is None or heR is None:
            im = ax.imshow(he_rgb, interpolation="nearest")
            im.set_transform(Affine2D(A_lvl) + ax.transData)
        else:   # inset: read the H&E at ~0.5 um/px around the window
            lvl = heR.level_near(0.5 / mpp)
            while lvl + 1 < len(heR.ds) and (2 * half * rec["global_px_um"]) / (mpp * heR.ds[lvl]) > 2500:
                lvl += 1                                     # keep a 500 um inset under ~2500 px
            d = heR.ds[lvl]
            inv = np.linalg.inv(A_full)
            corners = np.array([[win[0] - half, win[1] - half, 1], [win[0] + half, win[1] - half, 1],
                                [win[0] - half, win[1] + half, 1], [win[0] + half, win[1] + half, 1]]) @ inv.T / d
            x0, y0 = corners[:, 0].min(), corners[:, 1].min()
            crop = heR.read(lvl, x0, y0, corners[:, 0].max() - x0, corners[:, 1].max() - y0)
            M = A_full @ np.diag([d, d, 1]) @ np.array([[1, 0, max(x0, 0)], [0, 1, max(y0, 0)], [0, 0, 1]])
            im = ax.imshow(crop, interpolation="bilinear")
            im.set_transform(Affine2D(M) + ax.transData)
        if win is None:
            ax.scatter(cx[sel], cy[sel], s=0.05, c="#00d0ff", alpha=0.25, lw=0)
            ax.set_xlim(np.percentile(cx, 0.5), np.percentile(cx, 99.5))
            ax.set_ylim(np.percentile(cy, 99.5), np.percentile(cy, 0.5))
            ax.set_title("whole object: H&E + cell centroids (cyan)")
        else:
            m = (abs(cx - win[0]) < half) & (abs(cy - win[1]) < half)
            ax.scatter(cx[m], cy[m], s=3, facecolors="none", edgecolors="#00e5ff", lw=0.5)
            ax.set_xlim(win[0] - half, win[0] + half)
            ax.set_ylim(win[1] + half, win[1] - half)
            ax.set_title("500 µm inset: cells should sit on nuclei")
        ax.set_aspect("equal"), ax.set_xticks([]), ax.set_yticks([])
    nr = rec.get("nuclear_offset_um_refined")
    fig.suptitle(f"{sid}  ·  {Path(rec['he_file']).name} ({rec['section']} section)  ·  "
                 f"coverage {rec['coverage']:.2f}  ·  cells on H&E tissue {rec['cells_on_he_tissue']:.2f}"
                 + (f"  ·  nuclear offset {rec['nuclear_offset_um_mask_fit']:.1f} -> {nr:.1f} um" if nr else ""),
                 fontsize=11)
    fig.tight_layout()
    fig.savefig(path, dpi=110, pil_kwargs={"quality": 85})
    plt.close(fig)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--object", type=Path, required=True)
    ap.add_argument("--he", type=Path, required=True)
    ap.add_argument("--section", choices=["same", "serial"], required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--target-um", type=float, default=8.0)
    ap.add_argument("--he-mpp", help="um/px for scans with no scale on file, or 'auto' (tissue area)")
    ap.add_argument("--orientation", help="force one dihedral orientation (e.g. 'id'), when siblings agree")
    ap.add_argument("--tag", default="", help="suffix for output names, to compare candidate scans")
    ap.add_argument("--he-roi", help="x0,y0,x1,y1 in H&E full-res px: search only this box "
                                     "(a scan holding several sections)")
    a = ap.parse_args()
    roi = [float(v) for v in a.he_roi.split(",")] if a.he_roi else None
    register(a.object, a.he, a.section, a.out, a.target_um, a.he_mpp, a.orientation, roi, a.tag)


if __name__ == "__main__":
    main()
