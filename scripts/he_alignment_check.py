#!/usr/bin/env python3
"""Aligned or just attached? Per store with an he_* image:
row 0: H&E (full) + cells | spatial plot by cell type/cluster | nuclear image (full)
rows 1-3: three 300 um windows at 1 um/px: nuclear+centroids | H&E+centroids | overlay
Up to 12 windows that are fully imaged and on tissue are measured: normalized cross-correlation
of hematoxylin vs nuclear stain (or a cell-density map when the object has no nuclear image),
searched +/-150 um. Same-section Xenium scores ~0.75 at zero shift; serial sections ~0.05 with
the best shift scattered.
Usage (Cayuga): he_alignment_check.py OUTDIR STORE [STORE ...]
"""
import json
import os
import sys
import warnings
from pathlib import Path

import numpy as np
import zarr
import pandas as pd

sys.path.insert(0, "/athena/masonlab/scratch/users/jip2007/saha-data-access/scripts")
warnings.filterwarnings("ignore")
import logging
logging.disable(logging.WARNING)
from qc_release_objects import affine_xy, apply, centroids, label_col, labels, lowest_level, NUCLEAR  # noqa

STEP_UM, WIN_UM, SEARCH_UM = 1.0, 300.0, 150.0


def levels(img):
    """[(dataarray cyx, level px -> global 3x3)], finest first."""
    if not hasattr(img, "children"):
        return [(img, affine_xy(img))]
    names = list(img.children)
    base = next(iter(img[names[0]].data_vars.values()))
    M = affine_xy(base)
    out = []
    for n in names:
        a = next(iter(img[n].data_vars.values()))
        out.append((a, M @ np.diag([base.shape[-1] / a.shape[-1], base.shape[-2] / a.shape[-2], 1.0])))
    return out


def sample(img, gx, gy, um_per_gpx, chan=None):
    """Nearest-sample img on the global grid (gx, gy); picks the coarsest level still <= STEP_UM."""
    lv = levels(img)
    pick = lv[0]
    for a, M in lv:
        if np.sqrt(abs(np.linalg.det(M[:2, :2]))) * um_per_gpx <= STEP_UM * 1.01:
            pick = (a, M)
    a, M = pick
    inv = np.linalg.inv(M)
    px = apply(inv, np.c_[gx.ravel(), gy.ravel()])
    c, r = np.round(px[:, 0]).astype(int), np.round(px[:, 1]).astype(int)
    H, W = a.shape[-2:]
    ok = (c >= 0) & (c < W) & (r >= 0) & (r < H)
    if not ok.any():
        return None
    c0, c1, r0, r1 = c[ok].min(), c[ok].max() + 1, r[ok].min(), r[ok].max() + 1
    sub = np.asarray(a[..., r0:r1, c0:c1].values if chan is None else a[chan, r0:r1, c0:c1].values, np.float32)
    shp = gx.shape
    if sub.ndim == 3:
        out = np.zeros((sub.shape[0], ok.size), np.float32)
        out[:, ok] = sub[:, r[ok] - r0, c[ok] - c0]
        return out.reshape((sub.shape[0],) + shp)
    out = np.zeros(ok.size, np.float32)
    out[ok] = sub[r[ok] - r0, c[ok] - c0]
    return out.reshape(shp)


def cell_xy(sd):
    t = next(iter(sd.tables.values()))
    a = t.uns.get("spatialdata_attrs") or {}
    reg = a.get("region")
    reg = reg if isinstance(reg, str) else (reg[0] if reg else None)
    if reg in sd.shapes and a.get("instance_key") in t.obs:
        g, keys = sd.shapes[reg], t.obs[a["instance_key"]]
    else:
        keys = t.obs["cell_id"] if "cell_id" in t.obs else pd.Series(t.obs.index, index=t.obs.index)
        g = max(sd.shapes.values(), key=lambda g: keys.astype(str).isin(g.index.astype(str)).mean())
    pos = pd.Index(g.index.astype(str)).get_indexer(keys.astype(str).values)
    xy = np.full((len(t.obs), 2), np.nan)
    xy[pos >= 0] = apply(affine_xy(g), centroids(g.iloc[pos[pos >= 0]]))
    return t, xy


def nuclear(sd):
    for k in ("morphology", "morphology_focus", "morphology_mip"):
        if k in sd.images:
            a = levels(sd.images[k])[0][0]
            ch = [str(c) for c in a.coords["c"].values] if "c" in a.coords else []
            i = next((i for i, c in enumerate(ch) if any(n in c for n in NUCLEAR)), 0)
            return k, i, ch[i] if ch else "c0"
    return None, None, None


def norm(x):
    lo, hi = np.percentile(x, [1, 99.5])
    return np.clip((x - lo) / (hi - lo + 1e-9), 0, 1)


def check(store, outdir, hn):
    from scipy.ndimage import gaussian_filter
    from skimage.color import rgb2hed
    from skimage.registration import phase_cross_correlation
    from spatialdata import read_zarr
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.transforms import Affine2D

    store = Path(store)
    sid = store.name.removesuffix(".zarr")
    sd = read_zarr(store)
    t, xy = cell_xy(sd)
    reg = t.uns.get(f"{hn}_registration", {})
    reg = {k: (v.item() if hasattr(v, "item") else v) for k, v in dict(reg).items()}
    Mh0 = levels(sd.images[hn])[0][1]
    # objects registered outside register_he_object.py (v2) carry no he_um_per_px; global units
    # are the platform's pixels there, so the table's own pixel_size_um is the same quantity
    um_per_gpx = (float(reg["he_um_per_px"]) / np.sqrt(abs(np.linalg.det(Mh0[:2, :2]))) if "he_um_per_px" in reg
                  else float(np.asarray(t.uns["pixel_size_um"])))
    nk, ni, nch = nuclear(sd)

    good = np.isfinite(xy).all(1)
    P = xy[good]
    # windows: 300 um bins with >= 30 cells; measure up to 12 that are fully imaged and on tissue
    from skimage.feature import match_template
    w = WIN_UM / um_per_gpx
    b = np.floor((P - P.min(0)) / w).astype(int)
    key, cnt = np.unique(b, axis=0, return_counts=True)
    rng0 = np.random.default_rng(int(os.environ.get("HE_CHECK_SEED", 1)))   # new seed = fresh windows
    cand = rng0.permutation(np.flatnonzero(cnt >= 30))
    n, m = int(WIN_UM / STEP_UM), int(SEARCH_UM / STEP_UM)
    wins = []
    for j in cand:
        if len(wins) >= 12:
            break
        x0, y0 = P.min(0) + key[j] * w
        ax_ = np.arange(-m, n + m) * STEP_UM / um_per_gpx
        gx, gy = np.meshgrid(x0 + ax_, y0 + ax_)
        rgb = sample(sd.images[hn], gx, gy, um_per_gpx)
        if rgb is None:
            continue
        he_big = np.moveaxis(rgb, 0, -1).clip(0, 255).astype(np.uint8)
        hema_big = gaussian_filter(rgb2hed(he_big)[..., 0], 1.0)
        he, hema = he_big[m:m + n, m:m + n], hema_big[m:m + n, m:m + n]
        inw = ((P[:, 0] >= x0) & (P[:, 0] < x0 + w) & (P[:, 1] >= y0) & (P[:, 1] < y0 + w))
        cw = (P[inw] - [x0, y0]) * um_per_gpx / STEP_UM
        if nk:
            raw = sample(sd.images[nk], gx[m:m + n, m:m + n], gy[m:m + n, m:m + n], um_per_gpx, chan=ni)
            if (raw > 0).mean() < 0.95:          # window crosses an FOV edge
                continue
            ref = gaussian_filter(raw, 1.0)
            refname = f"{nk} [{nch}]"
        else:
            ref = np.zeros((n, n), np.float32)
            ij = np.clip(cw.astype(int), 0, n - 1)
            np.add.at(ref, (ij[:, 1], ij[:, 0]), 1)
            ref = gaussian_filter(ref, 3)
            refname = "cell density (no nuclear image)"
        tissue = float((he.mean(-1) < 215).mean())
        if tissue < 0.5:
            continue
        ncc = match_template(hema_big, ref)               # (2m+1)^2, centre = zero shift
        pk = np.unravel_index(np.argmax(ncc), ncc.shape)
        d = (np.array([pk[1], pk[0]]) - m) * STEP_UM       # H&E content sits this far from the cells (x, y um)
        wins.append(dict(x0=x0, y0=y0, he=he, hema=hema, ref=ref, refname=refname, cw=cw, n_cells=int(inw.sum()),
                         dx=float(d[0]), dy=float(d[1]), shift_um=float(np.hypot(*d)),
                         ncc_peak=float(ncc.max()), ncc_zero=float(ncc[m, m]), tissue=round(tissue, 2)))
    shown = sorted(wins, key=lambda d: -d["n_cells"])[:3]

    # figure
    nrow = 1 + len(shown)
    fig, axes = plt.subplots(nrow, 3, figsize=(16, 5.3 * nrow))
    lo, Mlo, _ = lowest_level(sd.images[hn])
    rng = np.random.default_rng(0)
    sel = rng.choice(np.flatnonzero(good), min(good.sum(), 60000), replace=False)
    ax = axes[0, 0]
    im = ax.imshow(np.moveaxis(np.asarray(lo.values), 0, -1).astype(np.uint8))
    im.set_transform(Affine2D(Mlo) + ax.transData)
    ax.scatter(*xy[sel[:15000]].T, s=0.3, c="#00d0ff", alpha=0.4, lw=0)
    for wd in shown:
        ax.add_patch(plt.Rectangle((wd["x0"], wd["y0"]), w, w, fill=False, ec="yellow", lw=1.5))
    ax.set_title(f"{hn} + cells (cyan), windows (yellow)")
    ax = axes[0, 1]
    col, kind = label_col(t.obs)
    if col:
        v = labels(t.obs[col])[sel]
        cats = pd.Series(v).value_counts().index
        cmap = plt.get_cmap("tab20" if len(cats) <= 20 else "gist_ncar", len(cats))
        lut = {c: cmap(k) for k, c in enumerate(cats)}
        ax.scatter(*xy[sel].T, s=0.5, c=[lut[x] for x in v], lw=0)
        if len(cats) <= 30:
            ax.legend(handles=[plt.Line2D([], [], marker="o", ls="", color=lut[c], label=c) for c in cats],
                      fontsize=6, loc="upper left", bbox_to_anchor=(1, 1), frameon=False)
    ax.set_title(f"spatial plot: {kind} ({col})")
    ax = axes[0, 2]
    if nk:
        nlo, nM, _ = lowest_level(sd.images[nk])
        g = np.asarray(nlo[ni].values, np.float32)
        im = ax.imshow(norm(g), cmap="gray")
        im.set_transform(Affine2D(nM) + ax.transData)
        ax.set_title(f"{nk} [{nch}]")
    else:
        ax.scatter(*xy[sel].T, s=0.3, c="k", lw=0)
        ax.set_title("no nuclear image in object")
    q = np.nanpercentile(P, [0, 100], axis=0)
    for ax in axes[0]:
        ax.set_aspect("equal"), ax.set_xlim(q[0, 0], q[1, 0]), ax.set_ylim(q[1, 1], q[0, 1])
        ax.set_xticks([]), ax.set_yticks([])
    for r, wd in enumerate(shown, 1):
        a0, a1, a2 = axes[r]
        a0.imshow(norm(wd["ref"]), cmap="gray")
        a0.scatter(*wd["cw"].T, s=4, facecolors="none", edgecolors="#ff3b30", lw=0.5)
        a0.set_title(f"window {r}: {wd['refname']} + centroids", fontsize=9)
        a1.imshow(wd["he"])
        a1.scatter(*wd["cw"].T, s=4, facecolors="none", edgecolors="#00d0ff", lw=0.5)
        a1.set_title(f"H&E + centroids · {wd['n_cells']} cells · best shift ({wd['dx']:+.0f}, {wd['dy']:+.0f}) um, "
                     f"NCC {wd['ncc_zero']:.2f} at 0 -> {wd['ncc_peak']:.2f}", fontsize=8)
        a2.imshow(np.dstack([norm(wd["hema"]), norm(wd["ref"]), norm(wd["hema"])]))
        a2.set_title("overlay: hematoxylin (magenta) vs nuclear (green)", fontsize=9)
        for a in (a0, a1, a2):
            a.set_xticks([]), a.set_yticks([])
    shifts = [wd["shift_um"] for wd in wins]
    med = float(np.median(shifts)) if shifts else None
    z = float(np.median([wd["ncc_zero"] for wd in wins])) if wins else None
    pkm = float(np.median([wd["ncc_peak"] for wd in wins])) if wins else None
    fig.suptitle(f"{sid}  ·  {hn}  ·  registration says offset {reg.get('nuclear_offset_um')} um, "
                 f"cells on tissue {reg.get('cells_on_he_tissue')}  ·  measured residual median "
                 f"{'n/a' if med is None else f'{med:.1f} um'} over {len(wins)} windows; NCC at 0 {z if z is None else round(z, 2)} vs best {pkm if pkm is None else round(pkm, 2)}", fontsize=11)
    fig.tight_layout()
    out = Path(outdir)
    out.mkdir(parents=True, exist_ok=True)
    tier = "aws" if "/release/" in str(store) else "zenodo"
    fig.savefig(out / f"{sid}.{tier}.{hn}.jpg", dpi=70, pil_kwargs={"quality": 82})
    plt.close(fig)
    res = dict(sample_id=sid, tier=tier, element=hn, n_cells=int(len(xy)), he_scan=reg.get("source"),
               reg_offset_um=reg.get("nuclear_offset_um"), cells_on_he=reg.get("cells_on_he_tissue"),
               he_um_per_px=reg.get("he_um_per_px"), nuclear=nk and f"{nk}[{nch}]",
               windows=[{k: wd[k] for k in ("x0", "y0", "n_cells", "dx", "dy", "shift_um", "ncc_peak", "ncc_zero", "tissue")} for wd in wins],
               median_shift_um=med, median_ncc_zero=z, median_ncc_peak=pkm, he_shape=list(levels(sd.images[hn])[0][0].shape))
    (out / f"{sid}.{tier}.{hn}.json").write_text(json.dumps(res, default=str))
    print(json.dumps(res, default=str))


if __name__ == "__main__":
    for s in sys.argv[2:]:
        try:
            names = [k for k in zarr.open_group(f"{s}/images", mode="r").keys() if k.startswith("he_")]
            for hn in names:
                check(s, sys.argv[1], hn)
        except Exception as e:
            import traceback
            traceback.print_exc()
            print(json.dumps(dict(store=s, error=f"{type(e).__name__}: {e}")))
