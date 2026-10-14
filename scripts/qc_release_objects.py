#!/usr/bin/env python3
"""
qc_release_objects.py
Open every release SpatialData object, assert what must be true of it, and
render one figure per object so a person can look at it.

Why figures: on SAHA every serious error so far was visible in a scatter plot
and invisible in a summary statistic (see project memory). So each object gets
morphology + cell centroids coloured by donor_core, and by cell type where the
object has one. The checks are semantic, not existence checks:

  * cells_expected   table n_obs == samples.parquet n_cells
  * donor_cores      per-core cell counts in the object == measurements.csv
  * linked           the table annotates a shapes element, and its instance ids
                     resolve to geometries (otherwise spatialdata-plot cannot
                     colour cells by anything in the table)
  * on_tissue        cell centroids, mapped through the stored transforms, sit
                     on brighter-than-typical morphology pixels. A wrong or
                     dropped transform puts cells on background and this ratio
                     falls to ~1 while every other check still passes.
  * coord_systems    every element is in 'global'

Usage (on Cayuga):
    P=~/.venv-sd08/bin/python
    $P scripts/qc_release_objects.py --list            # enumerate the targets
    $P scripts/qc_release_objects.py --index 7 --out OUT   # one object (slurm array)
    $P scripts/qc_release_objects.py --summarize --out OUT # qc.csv + gallery.html
"""

import argparse
import json
import os
import sys
import traceback
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

# Reference tables (samples.parquet, measurements.csv). SAHA_REF points at a
# fresh copy when the repo checkout on the cluster lags the local one.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from package_for_zenodo import sample_id_of  # noqa: E402

REPO = Path(os.environ.get("SAHA_REF", Path(__file__).resolve().parent.parent))
D = Path("/athena/project-saha/derived")

# Layout (scripts/organize_derived.py): release/s3 mirrors the bucket,
# build/zenodo_tier holds the 4x CosMx stores the Zenodo zips are made from, and
# symlinks for the Dulai/Xenium stores that serve both tiers.
ZENODO_TIER = D / "build/zenodo_tier"
S3 = D / "release/s3/processed"
S3_V2 = D / "release/s3_v2/processed"
UNALIGNED = "he_unaligned"   # H&E kept in its own coordinate system (not registered to the cells)
IMAGE_PREF = ["morphology", "morphology_focus", "he_same_section", "he", "morphology_mip"]
NUCLEAR = ("DNA", "DAPI", "Nuclear", "nuclear")
# obs columns holding each cell's centroid in the shapes' pixel frame
OBS_CENTROIDS = [("x_centroid_px", "y_centroid_px"), ("CenterX_global_px", "CenterY_global_px")]
CELLTYPE_COLS = ["cell_type_broad", "cell_type", "celltype", "annotation", "cell_type_l1"]
# cell_type_broad gets ONE colour per type across every object (annotate_broad.py's palette), so the
# gallery reads the same organ to organ; other label columns are coloured by frequency rank
BROAD = ["Epithelial", "Stem", "Lymphoid-T", "Lymphoid-B", "Lymphoid-Plasma", "Lymphoid-DC", "Lymphoid-Other",
         "Myeloid", "Endothelial", "Mesenchymal-Stromal", "Neuronal-Glial", "Erythroid"]


def colour_lut(col, v):
    """(categories in legend order, {category: colour}) for one label column's values."""
    import matplotlib.pyplot as plt
    cats = pd.Series(v).value_counts().index
    if col == "cell_type_broad":
        tab = plt.get_cmap("tab20")
        lut = {c: tab(i) for i, c in enumerate(BROAD)}
        cats = [c for c in BROAD if c in set(cats)] + [c for c in cats if c not in lut]
        lut.update({c: (0.8, 0.8, 0.8, 1) for c in cats if c not in lut})   # NA and anything unexpected: grey
        return cats, lut
    cmap = plt.get_cmap("tab20" if len(cats) <= 20 else "gist_ncar", len(cats))
    return cats, {c: cmap(k) for k, c in enumerate(cats)}
# no cell types: colour by an existing clustering, coarsest first
CLUSTER_COLS = ["leiden_res0.3", "leiden_res0.5", "leiden_res0.1", "leiden_res1.0", "leiden",
                "gene_expression_graphclust", "seurat_clusters", "louvain"]


def label_col(obs):
    """(column, kind) to colour cells by: cell type, else clustering, else donor core."""
    ok = lambda c: c in obs and 1 < obs[c].nunique() <= 80
    for kind, cols in (("cell type", CELLTYPE_COLS), ("cluster", CLUSTER_COLS), ("donor", ["donor_core"])):
        c = next((c for c in cols if ok(c)), None)
        if c:
            return c, kind
    return None, None


def targets(only=None):
    """(store, tier) for every release store; `only` = a directory under derived/.

    zenodo  4x CosMx store in build/zenodo_tier (symlinks there are skipped:
            they point at the s3 copy, which is checked once as "both")
    aws     full-res store in release/s3 that has a 4x twin
    both    store in release/s3 that is also the Zenodo copy (Dulai, Xenium)
    v2      store in release/s3_v2
    """
    zen = {p.name for p in ZENODO_TIER.glob("*/*.zarr") if not p.is_symlink()}
    out = [(str(p), "zenodo") for p in sorted(ZENODO_TIER.glob("*/*.zarr")) if not p.is_symlink()]
    out += [(str(p), "aws" if p.name in zen else "both") for p in sorted(S3.glob("*/*.zarr"))]
    out += [(str(p), "v2") for p in sorted(S3_V2.glob("*/*.zarr"))]
    if only:   # same tier labels as a full run (a blanket "both" put every object twice in the gallery)
        want = {str(p) for p in (D / only).glob("*.zarr")}
        return [t for t in out if t[0] in want]
    return out


def label(path):
    """Where a store lives, short: zenodo_tier/<slug>, s3/<slug>, s3_v2/<slug>."""
    rel = Path(path).parent.relative_to(D).parts
    if rel[:2] == ("build", "zenodo_tier"):
        return f"zenodo_tier/{rel[2]}"
    if rel[0] == "release":                       # release/<s3|s3_v2>/processed/<slug>
        return f"{rel[1]}/{rel[-1]}"
    return "/".join(rel)                          # e.g. a staged copy passed with --dir


# --- geometry helpers -------------------------------------------------------

def affine_xy(el, cs="global"):
    """3x3 matrix mapping element (x, y) to cs (x, y)."""
    from spatialdata.transformations import get_transformation
    t = get_transformation(el, cs)
    return t.to_affine_matrix(input_axes=("x", "y"), output_axes=("x", "y"))


def apply(M, xy):
    return xy @ M[:2, :2].T + M[:2, 2]


def lowest_level(img):
    """(array cyx, pixel->global 3x3) for the smallest pyramid level."""
    if hasattr(img, "children"):
        names = list(img.children)
        base = next(iter(img[names[0]].data_vars.values()))
        lo = next(iter(img[names[-1]].data_vars.values()))
        M = affine_xy(base)
        s = np.diag([base.shape[-1] / lo.shape[-1], base.shape[-2] / lo.shape[-2], 1.0])
        return lo, M @ s, tuple(base.shape)
    return img, affine_xy(img), tuple(img.shape)


def to_gray(arr):
    """Display/sampling channel: nuclear stain for IF, darkness for H&E."""
    chans = [str(c) for c in arr.coords["c"].values] if "c" in arr.coords else []
    a = np.asarray(arr.values, dtype=np.float32)
    if a.ndim == 2:
        return a, "single"
    if a.shape[0] == 3 and (arr.dtype == np.uint8 or not chans or chans[0] in ("r", "0")):
        return 255.0 - a.mean(0), "H&E (inverted)"
    for i, c in enumerate(chans):
        if any(k in c for k in NUCLEAR):
            return a[i], c
    return a[0], chans[0] if chans else "c0"


def labels(col):
    """Categorical obs column as plain strings, missing -> "NA" (astype(str) keeps NaN)."""
    return col.astype(object).where(col.notna(), "NA").astype(str).values


def centroids(gdf):
    g = gdf.geometry
    if (g.geom_type == "Point").all():
        return np.c_[g.x.values, g.y.values]
    c = g.centroid
    return np.c_[c.x.values, c.y.values]


# --- per-object -------------------------------------------------------------

def qc_one(path, tier, outdir):
    warnings.filterwarnings("ignore")
    import logging
    logging.disable(logging.WARNING)
    from spatialdata import read_zarr

    path = Path(path)
    sid = path.name.removesuffix(".zarr")
    rec = {"sample_id": sid, "tier": tier, "dir": label(path), "path": str(path),
           "problems": []}
    P = rec["problems"]
    sd = read_zarr(path)

    imgs = [k for k in sd.images if "fov" not in k and k != UNALIGNED]
    rec["images"] = {k: list(lowest_level(sd.images[k])[2]) for k in imgs}
    if UNALIGNED in sd.images:
        rec["images"][UNALIGNED] = list(level_arrays(sd.images[UNALIGNED])[0].shape)
    rec["n_fov_images"] = len(sd.images) - len(imgs) - (UNALIGNED in sd.images)
    rec["labels"], rec["shapes"], rec["points"] = list(sd.labels), list(sd.shapes), list(sd.points)
    rec["tables"] = {k: int(t.n_obs) for k, t in sd.tables.items()}
    if not sd.tables:
        P.append("no table")
        return rec

    # coordinate systems
    from spatialdata.transformations import get_transformation
    missing = [n for kind, n, e in sd.gen_elements()
               if kind != "tables" and n != UNALIGNED and "global" not in get_transformation(e, get_all=True)]
    if missing:
        P.append(f"not in global: {missing[:5]}")

    # expected cells / cores
    s1 = pd.read_parquet(REPO / "data/samples.parquet").set_index("sample_id")
    s2 = pd.read_parquet(REPO / "data/v2/samples.parquet").set_index("sample_id")
    meas = pd.read_csv(REPO / "zenodo/measurements.csv")
    if tier == "v2":  # v2 object names drop the run suffix; CosMx merges RNA+protein
        pat = "^" + sid.replace("CosMx", "C[RP]") + "_"
        exp_n = int(s2.n_cells[s2.index.str.match(pat)].sum()) or -1
    else:
        exp_n = int(s1.n_cells.get(sample_id_of(path), -1))
    rec["n_cells_expected"] = exp_n

    panels = []
    for tname, t in sd.tables.items():
        obs = t.obs
        tr = {"table": tname, "n_obs": int(t.n_obs), "n_vars": int(t.n_vars),
              "n_obs_cols": obs.shape[1]}
        attrs = t.uns.get("spatialdata_attrs")
        gdf, keys = None, None
        if attrs:
            region = attrs["region"] if isinstance(attrs["region"], str) else attrs["region"][0]
            tr["region"] = region
            for k in (attrs["region_key"], attrs["instance_key"]):
                if k not in obs:
                    P.append(f"{tname}: key column {k!r} missing from obs")
            if region in sd.shapes and attrs["instance_key"] in obs:
                gdf, keys = sd.shapes[region], obs[attrs["instance_key"]]
            elif region not in sd.shapes and region not in sd.labels:
                P.append(f"{tname}: annotates {region!r}, which is not in the object")
        else:
            P.append(f"{tname}: not linked to any element (no spatialdata_attrs)")
        if gdf is None:
            # Labels-annotated (spatialdata_io Xenium) or unlinked: find the
            # shapes element whose index the cell ids match, for plotting.
            cands = {"cell_id": obs["cell_id"]} if "cell_id" in obs else {}
            cands["obs index"] = pd.Series(obs.index, index=obs.index)
            best = max(((ser.astype(str).isin(g.index.astype(str)).mean(), sname, cname)
                        for sname, g in sd.shapes.items() for cname, ser in cands.items()),
                       default=(0, None, None))
            if best[0] > 0:
                gdf, keys = sd.shapes[best[1]], cands[best[2]]
                tr["region"] = f"{best[1]} (by {best[2]})"

        xy = None
        if gdf is not None:
            idx = pd.Index(gdf.index.astype(str))
            pos = idx.get_indexer(keys.astype(str).values)
            tr["frac_resolved"] = round(float((pos >= 0).mean()), 4)
            if tr["frac_resolved"] < 0.999:
                P.append(f"{tname}: only {tr['frac_resolved']:.1%} of instance ids resolve to {tr['region']}")
            ok = pos >= 0
            local = centroids(gdf.iloc[pos[ok]])
            xy = np.full((len(obs), 2), np.nan)
            xy[ok] = apply(affine_xy(gdf), local)
            # The table's own centroid columns must agree with its polygons. With
            # no image to align against, this is the only thing that catches
            # polygons placed on the wrong FOV grid (Dulai A2 NU02).
            pair = next((p for p in OBS_CENTROIDS if set(p) <= set(obs.columns)), None)
            if pair:
                ref = obs.loc[ok, list(pair)].to_numpy(float)
                tr["shape_obs_offset_px"] = off = round(float(np.nanmedian(np.hypot(*(local - ref).T))), 1)
                if off > 50:
                    P.append(f"{tname}: polygons sit a median {off:,.0f} px from obs {pair[0]}/{pair[1]}")

        if "donor_core" in obs:
            vc = pd.Series(labels(obs["donor_core"])).value_counts().drop(["NA", "None"], errors="ignore")
            tr["frac_donor_core"] = round(float(vc.sum() / len(obs)), 4)
            tr["n_cores"] = int(len(vc))
            m = meas[meas.sample_id == sample_id_of(path)].set_index("donor_core").cells
            if len(m):
                diff = (vc.reindex(m.index.union(vc.index), fill_value=0)
                        - m.reindex(m.index.union(vc.index), fill_value=0))
                bad = diff[diff != 0]
                tr["cores_mismatch"] = {k: int(v) for k, v in bad.items()}
                if len(bad):
                    P.append(f"{tname}: donor_core counts differ from measurements.csv on {len(bad)} cores")
        elif tier != "v2":
            P.append(f"{tname}: no donor_core column")

        ct, tr["label_kind"] = label_col(obs)
        tr["celltype_col"] = ct
        if tr["label_kind"] == "donor":
            ct = None  # donor_core already has its own panel
        rec.setdefault("table_checks", []).append(tr)
        panels.append((tname, obs, xy, ct))

    got = sum(rec["tables"].values())
    if exp_n < 0:
        P.append("sample not found in samples.parquet")
    elif got != exp_n:
        P.append(f"n_obs {got} != samples.parquet n_cells {exp_n}")

    # image + on-tissue check
    img_name = next((k for k in IMAGE_PREF if k in sd.images), imgs[0] if imgs else None)
    rec["image_used"] = img_name
    gray = ext = None
    if img_name:
        lo, Mpx, _ = lowest_level(sd.images[img_name])
        gray, chan = to_gray(lo)
        rec["channel"] = chan
        if not np.isfinite(gray).all() or gray.max() == gray.min():
            P.append(f"{img_name}: lowest level is blank")
        h, w = gray.shape
        corners = apply(Mpx, np.array([[0, 0], [w, 0], [0, h], [w, h]], float))
        ext = corners
        xy = next((p[2] for p in panels if p[2] is not None), None)
        if xy is not None:
            good = np.isfinite(xy).all(1)
            pix = apply(np.linalg.inv(Mpx), xy[good])
            c, r = pix[:, 0].astype(int), pix[:, 1].astype(int)
            inside = (c >= 0) & (c < w) & (r >= 0) & (r < h)
            rec["frac_cells_in_image"] = round(float(inside.mean()), 4)
            if inside.any():
                at_cells = np.median(gray[r[inside], c[inside]])
                bg = np.median(gray)
                rec["on_tissue_ratio"] = round(float((at_cells + 1) / (bg + 1)), 2)
                if rec["on_tissue_ratio"] < 1.3:
                    P.append(f"cells do not sit on tissue (ratio {rec['on_tissue_ratio']})")
            if rec["frac_cells_in_image"] < 0.98:
                P.append(f"only {rec['frac_cells_in_image']:.1%} of cells fall inside {img_name}")

    # H&E: cells must land on H&E tissue far more often than a random H&E pixel does.
    # A dropped or wrong transform puts them on glass at the background rate.
    he_panel = None
    xy = next((p[2] for p in panels if p[2] is not None), None)
    for hn in [k for k in sd.images if k.startswith("he_") and k != UNALIGNED]:
        from skimage.filters import threshold_otsu
        lo, Mh, _ = lowest_level(sd.images[hn])
        g, _ = to_gray(lo)
        m = g > threshold_otsu(g)
        rec.setdefault("he", {})[hn] = h = {"shape": list(lowest_level(sd.images[hn])[2])}
        if xy is not None:
            good = np.isfinite(xy).all(1)
            pix = apply(np.linalg.inv(Mh), xy[good])
            c, rr = pix[:, 0].astype(int), pix[:, 1].astype(int)
            inside = (c >= 0) & (c < m.shape[1]) & (rr >= 0) & (rr < m.shape[0])
            h["cells_on_he_tissue"] = on = round(float(m[rr[inside], c[inside]].mean() * inside.mean()), 3) if inside.any() else 0.0
            h["he_tissue_random"] = bg = round(float(m.mean()), 3)
            # lift over random on tissue, OR off tissue: where tissue fills most of the frame
            # (resections, bg > 50%) "2x random" is unreachable even for a perfect fit
            if on < 2 * bg and (1 - on) > 0.5 * (1 - bg):
                P.append(f"{hn}: cells land on H&E tissue {on:.0%} vs {bg:.0%} at random -- transform wrong?")
        he_panel = (hn, np.moveaxis(np.asarray(lo.values), 0, -1).astype(np.uint8), Mh)

    render(rec, panels, gray, Mpx if img_name else None, ext, outdir, he_panel)
    if xy is not None:
        tab = next(iter(sd.tables.values()))
        for hn in [k for k in sd.images if k.startswith("he_") and k != UNALIGNED]:
            render_he(rec, sd.images[hn], hn, xy, dict(tab.uns.get(f"{hn}_registration", {})), outdir)
    if UNALIGNED in sd.images:
        tab = next(iter(sd.tables.values()))
        render_unaligned(rec, sd.images[UNALIGNED], dict(tab.uns.get(f"{UNALIGNED}_registration", {})), outdir)
    return rec


def level_arrays(el):
    """Pyramid levels of an image element, finest first."""
    return [next(iter(el[n].data_vars.values())) for n in el.children] if hasattr(el, "children") else [el]


def render_unaligned(rec, el, reg, outdir, long_px=1400):
    """Gallery view of an H&E that is NOT registered (own coordinate system): the whole scan, no cells."""
    from PIL import Image
    levels = level_arrays(el)
    a = next((l for l in reversed(levels) if max(l.shape[-2:]) >= long_px), levels[0])
    img = Image.fromarray(np.moveaxis(np.asarray(a.values), 0, -1)[..., :3].astype(np.uint8))
    img.thumbnail((long_px, long_px))
    out = Path(outdir) / "figs" / rec["dir"]
    out.mkdir(parents=True, exist_ok=True)
    p = out / f"{rec['sample_id']}.{UNALIGNED}.jpg"
    img.save(p, quality=82)
    rel = str(p.relative_to(outdir))   # no cell overlay exists: "cells" shows the same image
    rec.setdefault("he_figs", []).append({"element": UNALIGNED, "plain": rel, "cells": rel,
                                          "source": str(reg.get("source", "")), "note": str(reg.get("accuracy_note", ""))})


def render_he(rec, el, hn, xy, reg, outdir, long_px=1400):
    """Gallery view of one H&E element: the object's extent at ~long_px, in the global frame
    (same orientation as the other panels), once plain and once with cell centroids."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.transforms import Affine2D

    P = xy[np.isfinite(xy).all(1)]
    lo, hi = np.percentile(P, [0, 100], axis=0)
    pad = (hi - lo) * 0.03
    lo, hi = lo - pad, hi + pad
    per_px = float(max(hi - lo)) / long_px                     # global units per output pixel
    names = list(el.children) if hasattr(el, "children") else [None]
    arrs = [next(iter(el[n].data_vars.values())) if n else el for n in names]
    M0 = affine_xy(arrs[0])
    a, M = arrs[0], M0
    for arr in arrs:                                           # coarsest level still finer than the output
        Ml = M0 @ np.diag([arrs[0].shape[-1] / arr.shape[-1], arrs[0].shape[-2] / arr.shape[-2], 1.0])
        if np.sqrt(abs(np.linalg.det(Ml[:2, :2]))) <= per_px * 1.01:
            a, M = arr, Ml
    cg = np.array([[lo[0], lo[1]], [hi[0], lo[1]], [lo[0], hi[1]], [hi[0], hi[1]]])
    cp = apply(np.linalg.inv(M), cg)
    H, W = a.shape[-2:]
    c0, r0 = (max(int(np.floor(v)), 0) for v in cp.min(0))
    c1, r1 = min(int(np.ceil(cp[:, 0].max())), W), min(int(np.ceil(cp[:, 1].max())), H)
    if c1 <= c0 or r1 <= r0:
        return
    rgb = np.moveaxis(np.asarray(a[:, r0:r1, c0:c1].values), 0, -1)[..., :3].astype(np.uint8)
    T = M @ np.array([[1, 0, c0], [0, 1, r0], [0, 0, 1]], float)
    ext = hi - lo
    fig = plt.figure(figsize=(ext[0] / per_px / 100, ext[1] / per_px / 100), dpi=100, facecolor="white")
    ax = fig.add_axes([0, 0, 1, 1])
    im = ax.imshow(rgb, interpolation="bilinear")
    im.set_transform(Affine2D(T) + ax.transData)
    ax.set_xlim(lo[0], hi[0]), ax.set_ylim(hi[1], lo[1]), ax.axis("off")
    out = Path(outdir) / "figs" / rec["dir"]
    out.mkdir(parents=True, exist_ok=True)
    plain, cells = out / f"{rec['sample_id']}.{hn}.jpg", out / f"{rec['sample_id']}.{hn}.cells.jpg"
    fig.savefig(plain, dpi=100, pil_kwargs={"quality": 82})
    sel = np.random.default_rng(0).choice(len(P), min(len(P), 40000), replace=False)
    ax.scatter(*P[sel].T, s=0.15, c="#00e5ff", alpha=0.5, lw=0)
    fig.savefig(cells, dpi=100, pil_kwargs={"quality": 82})
    plt.close(fig)
    rec.setdefault("he_figs", []).append({
        "element": hn, "plain": str(plain.relative_to(outdir)), "cells": str(cells.relative_to(outdir)),
        "source": str(reg.get("source", "")), "note": str(reg.get("accuracy_note", ""))})


def render(rec, panels, gray, Mpx, ext, outdir, he_panel=None):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.transforms import Affine2D

    tname, obs, xy, ct = next((p for p in panels if p[2] is not None), panels[0])
    cols = [("donor_core", "donor_core" in obs)] + [(ct, ct is not None)]
    cols = [c for c, ok in cols if ok] or [None]
    n = len(cols) + (gray is not None) + (he_panel is not None)
    fig, axes = plt.subplots(1, n, figsize=(6 * n, 6.4), squeeze=False)
    axes = axes[0]
    rng = np.random.default_rng(0)
    sel = None
    if xy is not None:
        good = np.flatnonzero(np.isfinite(xy).all(1))
        sel = rng.choice(good, min(len(good), 120_000), replace=False)

    def show_img(ax, alpha=1.0):
        lo, hi = np.percentile(gray, [1, 99.5])
        im = ax.imshow(np.clip((gray - lo) / (hi - lo + 1e-9), 0, 1), cmap="gray",
                       alpha=alpha, interpolation="nearest")
        im.set_transform(Affine2D(Mpx) + ax.transData)

    i = 0
    if gray is not None:
        show_img(axes[0])
        if sel is not None:
            axes[0].scatter(*xy[sel[:20000]].T, s=0.3, c="#ff3b30", alpha=0.35, lw=0)
        axes[0].set_title(f"{rec['image_used']} [{rec.get('channel')}] + cells (red)")
        i = 1
    if he_panel is not None:
        hn, rgb, Mh = he_panel
        im = axes[i].imshow(rgb, interpolation="nearest")
        im.set_transform(Affine2D(Mh) + axes[i].transData)
        if sel is not None:
            axes[i].scatter(*xy[sel[:20000]].T, s=0.3, c="#00d0ff", alpha=0.35, lw=0)
        axes[i].set_title(f"{hn} + cells (cyan)")
        i += 1
    for ax, col in zip(axes[i:], cols):
        if gray is not None:
            show_img(ax, 0.35)
        if sel is not None and col is not None:
            v = labels(obs[col])[sel]
            cats, lut = colour_lut(col, v)
            ax.scatter(*xy[sel].T, s=0.4, c=[lut[x] for x in v], lw=0)
            if len(cats) <= 40:
                ax.legend(handles=[plt.Line2D([], [], marker="o", ls="", color=lut[c], label=c)
                                   for c in cats], fontsize=6, markerscale=0.8,
                          loc="upper left", bbox_to_anchor=(1, 1), frameon=False)
        elif sel is not None:
            ax.scatter(*xy[sel].T, s=0.4, c="k", lw=0)
        ax.set_title(col or "cells")
    for ax in axes:
        ax.set_aspect("equal")
        ax.invert_yaxis()
        # frame on the cells (the image may extend past them), else the image
        q = np.nanpercentile(xy[sel], [0, 100], axis=0) if sel is not None and len(sel) else \
            (np.r_[[ext.min(0)], [ext.max(0)]] if ext is not None else None)
        if q is not None:
            pad = (q[1] - q[0]) * 0.03
            ax.set_xlim(q[0, 0] - pad[0], q[1, 0] + pad[0])
            ax.set_ylim(q[1, 1] + pad[1], q[0, 1] - pad[1])
        ax.set_xticks([]), ax.set_yticks([])
    # the figure doubles as the public gallery panel: QC status only when it failed
    bad = f"  —  {len(rec['problems'])} PROBLEM(S)" if rec["problems"] else ""
    fig.suptitle(f"{rec['sample_id']}  ·  {sum(rec['tables'].values()):,} cells{bad}",
                 fontsize=12, color="#c00" if bad else "k")
    fig.tight_layout()
    out = Path(outdir) / "figs" / rec["dir"]
    out.mkdir(parents=True, exist_ok=True)
    rec["figure"] = str((out / f"{rec['sample_id']}.jpg").relative_to(outdir))
    fig.savefig(Path(outdir) / rec["figure"], dpi=90, pil_kwargs={"quality": 80})
    plt.close(fig)

    # gallery thumbnail: cells coloured by cell type, else cluster, else donor core
    col, kind = label_col(obs)
    rec["thumb_by"] = f"{kind}: {col}" if col else None
    if sel is None:
        return
    fig, ax = plt.subplots(figsize=(3, 3))
    if col:
        v = labels(obs[col])[sel]
        cats, lut = colour_lut(col, v)
        ax.scatter(*xy[sel].T, s=0.15, c=[lut[x] for x in v], lw=0)
    else:
        ax.scatter(*xy[sel].T, s=0.15, c="#555", lw=0)
    ax.set_aspect("equal"), ax.invert_yaxis(), ax.axis("off")
    fig.subplots_adjust(0, 0, 1, 1)
    rec["thumb"] = str((out / f"{rec['sample_id']}.thumb.jpg").relative_to(outdir))
    fig.savefig(Path(outdir) / rec["thumb"], dpi=80, pil_kwargs={"quality": 80})
    plt.close(fig)


# --- summary ----------------------------------------------------------------

def groups(df):
    """release / organ / modality per row, from the manifests, never the id."""
    s1 = pd.read_parquet(REPO / "data/samples.parquet").set_index("sample_id")
    s2 = pd.read_parquet(REPO / "data/v2/samples.parquet").set_index("sample_id")
    out = {}
    for i, r in df.iterrows():
        if r.tier == "v2":
            m = s2[s2.index.str.match("^" + r.sample_id.replace("CosMx", "C[RP]") + "_")]
            rel = "v2"
        else:
            m = s1.loc[[sample_id_of(Path(r.sample_id + ".zarr"))]] if \
                sample_id_of(Path(r.sample_id + ".zarr")) in s1.index else s1.iloc[:0]
            rel = "v1"
        if m.empty:
            out[i] = (rel, "unknown", "unknown", "unknown")
            continue
        m = m.sort_values("assay_type", ascending=False)  # rna before protein
        out[i] = (rel, m.organ.iloc[0], f"{m.platform.iloc[0]} {'+'.join(m.assay_type.unique())}",
                  " + ".join(m.panel_name.unique()))
    return pd.DataFrame.from_dict(out, orient="index", columns=["release", "organ", "modality", "panel"])


def summarize(outdir, public=None):
    outdir = Path(outdir)
    recs = [json.loads(p.read_text()) for p in sorted((outdir / "qc").rglob("*.json"))]
    expected = {Path(p).name.removesuffix(".zarr") + "@" + label(p) for p, _ in targets()}
    seen = {r["sample_id"] + "@" + r["dir"] for r in recs}
    rows = []
    for r in recs:
        rows.append({"sample_id": r["sample_id"], "dir": r["dir"], "tier": r["tier"],
                     "n_obs": sum(r.get("tables", {}).values()),
                     "n_expected": r.get("n_cells_expected"),
                     "images": ";".join(f"{k}{v}" for k, v in r.get("images", {}).items()),
                     "frac_in_image": r.get("frac_cells_in_image"),
                     "on_tissue_ratio": r.get("on_tissue_ratio"),
                     "n_problems": len(r["problems"]), "problems": " | ".join(r["problems"]),
                     "figure": r.get("figure"), "thumb": r.get("thumb"),
                     "thumb_by": r.get("thumb_by"), "he_figs": json.dumps(r.get("he_figs", []))})
    for k in sorted(expected - seen):
        sid, d = k.split("@")
        rows.append({"sample_id": sid, "dir": d, "n_problems": 1, "problems": "QC job produced no result"})
    df = pd.DataFrame(rows).sort_values(["n_problems", "dir", "sample_id"], ascending=[False, True, True])

    # cross-tier: the AWS copy must hold the same cells as the Zenodo copy
    z = df[df.tier == "zenodo"].set_index("sample_id")
    for i, r in df[df.tier == "aws"].iterrows():
        if r.sample_id in z.index and z.loc[r.sample_id, "n_obs"] != r.n_obs:
            df.loc[i, "problems"] += f" | n_obs differs from Zenodo copy ({z.loc[r.sample_id, 'n_obs']})"
            df.loc[i, "n_problems"] += 1

    df = df.join(groups(df), how="left")
    df.to_csv(outdir / "qc.csv", index=False)

    n_bad = int((df.n_problems > 0).sum())
    write_gallery(df, outdir / "gallery.html")
    if public:
        write_gallery(df, Path(public) / "index.html", src_root=outdir, public=True)
    print(f"{len(df)} stores · {n_bad} with problems · {len(expected - seen)} missing results")
    print(df[df.n_problems > 0][["sample_id", "dir", "problems"]].to_string(index=False))


CONTACT = 'Questions or suggestions? Contact Jiwoon Park, Ph.D. (<a href="mailto:jip2007@med.cornell.edu">jip2007@med.cornell.edu</a>) and Christopher E. Mason, Ph.D. (<a href="mailto:chm2042@med.cornell.edu">chm2042@med.cornell.edu</a>).'


def write_gallery(df, path, src_root=None, public=False):
    """One card per object (the AWS full-res copy is folded into it).

    public=True writes a self-contained folder for the website/repo: images are
    copied next to the page, and nothing QC-internal is shown (no problems
    toggle, no tier or check wording).
    """
    import html
    import shutil
    path.parent.mkdir(parents=True, exist_ok=True)
    aws_prob = df[df.tier == "aws"].set_index("sample_id").problems
    g = df[(df.tier != "aws") & df.n_obs.notna()].copy()   # "no result" rows have nothing to draw
    g["problems"] = [" | ".join(x for x in (p, f"AWS copy: {aws_prob[sid]}" if aws_prob.get(sid) else "") if x)
                     for sid, p in zip(g.sample_id, g.problems.fillna(""))]
    if public:
        g["problems"] = ""
        for col in ("figure", "thumb"):   # copy images beside the page, flat names
            new = []
            for f in g[col]:
                if not isinstance(f, str):
                    new.append(f)
                    continue
                dst = Path("img") / Path(f).name
                (path.parent / "img").mkdir(exist_ok=True)
                shutil.copy2(Path(src_root) / f, path.parent / dst)
                new.append(str(dst))
            g[col] = new
        he = []
        for js in g.he_figs.fillna("[]"):
            figs = json.loads(js)
            for f in figs:
                for k in ("plain", "cells"):
                    dst = Path("img") / Path(f[k]).name
                    shutil.copy2(Path(src_root) / f[k], path.parent / dst)
                    f[k] = str(dst)
            he.append(json.dumps(figs))
        g["he_figs"] = he
    g["kind"] = g.modality.str.replace("+", " + ") + " · " + g.panel
    palette = {"cosmx rna": ["#1f5fbf", "#5aa3e8", "#123a73"], "cosmx protein": ["#d08a12"],
               "cosmx rna+protein": ["#7a3fc4"], "xenium rna": ["#2e8b57", "#7ccf8f", "#155e3a"]}
    color, used = {}, {}
    for k, mod in g[["kind", "modality"]].drop_duplicates().sort_values("kind").itertuples(index=False):
        pal = palette.get(mod, ["#888"])
        color[k] = pal[used.get(mod, 0) % len(pal)]
        used[mod] = used.get(mod, 0) + 1
    cid = {k: f"k{i}" for i, k in enumerate(color)}

    body = []
    for (rel, organ), grp in g.sort_values(["release", "organ", "kind", "sample_id"]).groupby(
            ["release", "organ"], sort=False):
        cards = []
        for _, r in grp.iterrows():
            bad = bool(r.problems)
            prob = html.escape(r.problems)
            src = r.thumb if isinstance(r.thumb, str) else ""
            by = (r.thumb_by or "").split(":")[0] if isinstance(r.thumb_by, str) else "no labels"
            if public and by == "cluster":
                by = "Leiden cluster"
            meta = f"{int(r.n_obs or 0):,} cells · {html.escape(r.kind)}"
            he = r.he_figs if isinstance(r.get("he_figs"), str) else "[]"
            badge = ' <span class=he>H&amp;E</span>' if he != "[]" else ""
            cards.append(
                f'<div class="card{" bad" if bad else ""}" data-k="{cid[r.kind]}" style="--c:{color[r.kind]}" '
                f'data-fig="{r.figure if isinstance(r.figure, str) else ""}" data-meta="{meta}" data-prob="{prob}" data-he="{html.escape(he)}" '
                f'onclick="openBox(this)"><div class=thumb>{f"<img loading=lazy src={src!r}>" if src else ""}</div>'
                f'<h3>{r.sample_id}</h3><p>{int(r.n_obs or 0):,} cells · <i>{by}</i>{badge}'
                f'{"<br><b>" + prob + "</b>" if bad else ""}</p></div>')
        body.append(f'<section data-rel="{rel}"><h2>{organ.replace("_", " ")} <small></small></h2>'
                    f'<div class=grid>{"".join(cards)}</div></section>')
    chips = "".join(f'<label class=chip style="--c:{c}"><input type=checkbox checked value={cid[k]} onchange="show()">'
                    f'{html.escape(k)} <span id=n{cid[k]}></span></label>' for k, c in color.items())
    rels = sorted(g.release.unique())
    tabs = "".join(f'<button data-rel="{r}" onclick="rel=this.dataset.rel;show()">{r} '
                   f'({(g.release == r).sum()} objects)</button>' for r in rels)
    n_bad = int((g.problems != "").sum())
    if public:
        title = "SAHA spatial omics — object gallery"
        intro = (f"{len(g)} SpatialData objects · {int(g.n_obs.sum()):,} cells · {g.organ.nunique()} organs. "
                 "Thumbnails colour cells by annotated cell type, or by Leiden cluster where no annotation "
                 "exists yet. Click a card for the morphology image, donor-core and label panels; cards marked "
                 "H&E also open the registered H&E stain.")
        toggle = ""
    else:
        title = "SAHA object QC"
        intro = (f"{len(g)} objects · every one checked in both release tiers (Zenodo, AWS full-resolution) · "
                 f"{n_bad} with problems. Thumbnails colour cells by cell type, else an existing clustering, "
                 "else donor core. Click a card for all panels.")
        toggle = '<label style="margin-left:12px"><input type=checkbox id=bad onchange="show()"> problems only</label>'
    path.write_text(f"""<!doctype html><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1">
<title>{title}</title><style>
body{{font:12px system-ui;margin:16px;background:#fafafa;color:#222}}
.grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:8px}}
.card{{background:#fff;border:2px solid var(--c);border-top-width:6px;padding:4px;border-radius:5px;cursor:zoom-in}}
.card.bad{{outline:3px solid #c00}} .thumb{{aspect-ratio:1;display:flex;align-items:center;justify-content:center}}
.thumb img{{max-width:100%;max-height:100%}} h3{{margin:3px 0 1px;font-size:10.5px;word-break:break-all}}
p{{margin:0;color:#555;font-size:11px}} b{{color:#c00}} i{{color:#888}}
h2{{margin:20px 0 6px;padding-bottom:3px;border-bottom:2px solid #333;font-size:16px;text-transform:capitalize}} h2 small{{color:#777;font-weight:normal;text-transform:none}}
.bar{{position:sticky;top:0;background:#fafafa;padding:8px 0;z-index:2;border-bottom:1px solid #ddd}}
button{{font:13px system-ui;padding:5px 14px;margin-right:4px;border:1px solid #888;background:#fff;border-radius:4px;cursor:pointer}} button.on{{background:#222;color:#fff}}
.chip{{display:inline-block;margin:6px 6px 0 0;padding:3px 8px;border:2px solid var(--c);border-left-width:10px;border-radius:4px;background:#fff;cursor:pointer}}
#box{{display:none;position:fixed;inset:0;background:rgba(0,0,0,.85);z-index:9;padding:24px;overflow:auto;cursor:zoom-out}}
#box img{{display:block;max-width:100%;max-height:80vh;margin:0 auto;background:#fff}} #btxt{{color:#fff;text-align:center;margin:8px auto 0;font-size:13px;max-width:900px}}
#tabs{{text-align:center;margin-bottom:8px;color:#fff;cursor:default}} #tabs button{{background:#333;color:#fff;border-color:#777}} #tabs button.on{{background:#fff;color:#111}}
.he{{font-size:9px;background:#8a2be2;color:#fff;border-radius:3px;padding:0 3px;margin-left:3px;vertical-align:middle}}
footer{{margin:32px 0 8px;color:#999;font-size:10.5px;text-align:center}}footer a{{color:inherit}}
</style><h1>{title}</h1>
<p>{intro}</p>
<div class=bar>{tabs} {toggle}
<span id=count style="margin-left:12px;color:#555"></span><br>{chips}</div>
{"".join(body)}<div id=box onclick="this.style.display='none'"><div id=tabs onclick="event.stopPropagation()"></div><img id=bimg alt=""><div id=btxt></div></div>
<footer>{CONTACT}</footer>
<script>
let rel='{rels[0]}';const bad=document.getElementById('bad');
let cur=null,he=[],ci=-1;
function openBox(c){{if(!c.dataset.fig)return;cur=c;he=JSON.parse(c.dataset.he||'[]');
let h='<button data-i=-1 onclick="pick(-1)">Panels</button>';
he.forEach((e,i)=>h+='<button data-i='+i+' onclick="pick('+i+')">H&amp;E · '+e.element.replace('he_','').replace('_section',' section')+'</button>');
if(he.length)h+='<label style="margin-left:10px"><input type=checkbox id=cellsT checked onchange="pick(ci)"> show cells</label>';
tabs.innerHTML=h;pick(-1);box.style.display='block'}}
function pick(i){{ci=i;for(const b of tabs.querySelectorAll('button'))b.classList.toggle('on',+b.dataset.i==i);
const name='<b style="color:#fff">'+cur.querySelector('h3').textContent+'</b> · '+cur.dataset.meta;
if(i<0){{bimg.src=cur.dataset.fig;btxt.innerHTML=name+(cur.dataset.prob?'<br><span style="color:#f88">'+cur.dataset.prob+'</span>':'');return}}
const e=he[i],ct=document.getElementById('cellsT');bimg.src=(ct&&ct.checked)?e.cells:e.plain;
btxt.innerHTML=name+'<br><span style="color:#ccc">'+(e.source?e.source+' — ':'')+e.note+'</span>'}}
document.addEventListener('keydown',e=>{{if(e.key==='Escape')box.style.display='none'}});
function show(){{const on=new Set([...document.querySelectorAll('.chip input:checked')].map(i=>i.value)),cnt={{}};let n=0,cells=0;
for(const b of document.querySelectorAll('button[data-rel]'))b.classList.toggle('on',b.dataset.rel==rel);
for(const s of document.querySelectorAll('section')){{let k=0;
 for(const c of s.querySelectorAll('.card')){{const inRel=s.dataset.rel==rel;if(inRel)cnt[c.dataset.k]=(cnt[c.dataset.k]||0)+1;
  const v=inRel&&on.has(c.dataset.k)&&(!(bad&&bad.checked)||c.classList.contains('bad'));c.style.display=v?'':'none';
  if(v){{k++;n++;cells+=parseInt(c.dataset.meta.replace(/,/g,''))}}}}
 s.style.display=k?'':'none';s.querySelector('h2 small').textContent=k+' objects'}}
for(const ch of document.querySelectorAll('.chip')){{const v=ch.querySelector('input').value;ch.style.display=cnt[v]?'':'none';document.getElementById('n'+v).textContent='('+(cnt[v]||0)+')'}}
count.textContent=n+' objects · '+cells.toLocaleString()+' cells shown'}}show()
</script>""")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--index", type=int)
    ap.add_argument("--summarize", action="store_true")
    ap.add_argument("--public", help="with --summarize: also write a public gallery folder here")
    ap.add_argument("--dir", help="check only this directory under derived/ (e.g. a staged copy)")
    ap.add_argument("--out", default="/athena/masonlab/scratch/users/jip2007/release_qc")
    a = ap.parse_args()
    T = targets(a.dir)
    if a.list:
        for i, (p, t) in enumerate(T):
            print(i, t, p)
        return
    if a.summarize:
        return summarize(a.out, a.public)
    path, tier = T[a.index]
    try:
        rec = qc_one(path, tier, a.out)
    except Exception as e:  # record it; a crash is a finding, not a skip
        rec = {"sample_id": Path(path).name.removesuffix(".zarr"), "dir": label(path),
               "tier": tier, "problems": [f"QC crashed: {type(e).__name__}: {e}"],
               "trace": traceback.format_exc()}
    out = Path(a.out) / "qc" / rec["dir"]
    out.mkdir(parents=True, exist_ok=True)
    (out / f"{rec['sample_id']}.json").write_text(json.dumps(rec, indent=1, default=str))
    print(json.dumps({k: rec.get(k) for k in ("sample_id", "dir", "problems")}))


if __name__ == "__main__":
    sys.exit(main())
