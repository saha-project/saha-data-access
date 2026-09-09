#!/usr/bin/env python3
"""
resolve_donor_cores.py
Resolve per-cell donor identity for the CosMx objects, and render a QC figure
per slide so the assignment can be checked by eye against the RNA counterpart.

These slides are tissue microarrays: one acquisition covers several donor cores,
so donor is a per-CELL property. A single object such as SAHA_CR_COL_A1_EC02
spans all seven COL_A1 cores.

Sources, in order of trust:

  1. per-cell `SAHA_Sample` in the published organ-level h5ads   (RNA)
  2. per-cell `patientID` in the PDAC h5ad                       (RNA)
  3. recorded FOV -> core maps in ProjectConfig_*.yaml and the
     FOV_Metadata CSVs                                           (mostly protein)
  4. geometric registration of a protein run against its RNA
     counterpart on the same slide                               (last resort)

Every recorded map is validated before use, not trusted: coverage must be
complete, the core labels must belong to this organ, and each core's cells must
form a spatially compact cluster. That last test alone is NOT sufficient --
CosMx numbers FOVs along a serpentine scan, so any contiguous grouping of FOV
indices looks compact. Without the organ constraint a lymph-node sample matches
a pancreas map with a perfect-looking score.

Registration is a similarity transform (scale, rotation, reflection,
translation) fitted between protein FOV clusters and RNA core centroids, solved
by alternating Hungarian assignment and Kabsch fitting from several starts. It
is accepted only when the residual is far below the runner-up: for COL_A1 the
correct assignment fits to 280 px against 28,000+ for every alternative. That
margin is the evidence, not the fit itself.

Note that RNA and protein are consecutive sections and are NOT in the same
pixel scale -- the COL_A1 fit needs scale 1.13. Anything assuming a shared
coordinate frame fails silently.

Usage:
    python scripts/resolve_donor_cores.py --zarr-dir ~/SAHA_zarr_build \
        --out-dir zenodo/donor_qc --dry-run
"""

import argparse
import csv
import glob
import os
import re
import sys
import warnings
from pathlib import Path

import numpy as np

warnings.filterwarnings("ignore")

CONFIG_DIR = Path("/Users/jiwoonpark/Dropbox (Personal)/2025- MasonLab/SAHA/"
                  "02_Code/Configs/ProjectConfigs")
FOVMETA_DIR = Path("/Users/jiwoonpark/Dropbox (Personal)/2025- MasonLab/Projects/"
                   "Old projects/202202_SAHA/Data/CosMx/FOV_Metadata")
H5AD_RNA = Path("/Users/jiwoonpark/Dropbox (Personal)/2025- MasonLab/2025_SAHA/"
                "Data/preprocessed/h5ad/RNA")
H5AD_EXTRA = [
    Path("/Users/jiwoonpark/Dropbox (Personal)/2025- MasonLab/2025_SAHA/"
         "Analysis/IBD_integration/adata.joint.h5ad"),
    Path("/Users/jiwoonpark/Dropbox (Personal)/2025- MasonLab/Projects/Old projects/"
         "202202_SAHA/Data/CosMx/SeuratObject_EC/RNA_PDAC_scVI.h5ad"),
]

ORGAN = {"APE": "APE", "COL": "COL", "ILE": "ILE", "LN": "LN", "PANC": "PANC",
         "STO": "STO", "PDAC": "PANC_CA", "IBD": "IBD"}

# protein run -> the RNA run on the same slide to register against
RNA_COUNTERPART = {
    "SAHA_CP_COL_A1_EC02": "SAHA_CR_COL_A1_EC02",
    "SAHA_CP_ILE_A1_SJ01": "SAHA_CR_ILE_A1_SJ01",
    "SAHA_CP_LN_A1_EC03": "SAHA_CR_LN_A1_EC03",
    "SAHA_CP_PANC_A1_SJ01": "SAHA_CR_PANC_A1_SJ01",
    "SAHA_CP_STO_A1_EC03": "SAHA_CR_STO_A1_EC03",
}

COMPACT_MAX = 0.40        # within-core spread / nearest-core distance
MARGIN_MIN = 5.0          # runner-up residual / best residual
FOV_MATCH_PX = 2128       # half a FOV: below this, two runs share a FOV layout

# Samples whose cores do not separate by spatial gap, with the core count known
# from the slide. CP_PANC_A1_SJ01 packs 905 FOVs tightly enough that gap
# clustering returns one blob; PANC_A1 carries 12 cores like its sibling runs.
FORCE_K = {"SAHA_CP_PANC_A1_SJ01": 12}

# Corrections to labels in the upstream sources, each confirmed by at least two
# independent runs on the same slide disagreeing with a third.
#
# SAHA_Sample in SAHA_APE_RNA.h5ad has cores 2 and 3 transposed for the SJ01
# run only: CR_APE_A9_EC01 (same h5ad) and CP_APE_A9_SJ01 (recorded FOV map)
# agree with each other at a Procrustes residual of 0.11, while CR_APE_A9_SJ01
# disagrees with both at 1.85-2.01. Reported by Jiwoon from the QC figure.
CORE_LABEL_FIX = {
    "SAHA_CR_APE_A9_SJ01": {"APE_A9_2": "APE_A9_3", "APE_A9_3": "APE_A9_2"},
}


def organ_of(sample_id: str) -> str:
    p = sample_id.split("_")
    return ORGAN.get(p[2], p[2]) if len(p) > 2 else ""


# --------------------------------------------------------------------------
# recorded FOV -> core maps
# --------------------------------------------------------------------------

def _walk(d, path=""):
    for k, v in d.items():
        if isinstance(v, dict):
            yield from _walk(v, f"{path}/{k}" if path else k)
        elif isinstance(v, list):
            yield path, k, v


def load_maps() -> dict:
    """name -> {fov:int -> core:str}, from configs and FOV metadata CSVs."""
    import yaml
    maps = {}
    for f in glob.glob(str(CONFIG_DIR / "*.yaml")) + \
            glob.glob(str(CONFIG_DIR / "individual_RDS_configs" / "*.yaml")):
        try:
            d = yaml.safe_load(open(f))
        except Exception:
            continue
        if not isinstance(d, dict) or not isinstance(d.get("FOV_maps"), dict):
            continue
        grp = {}
        for path, core, fovs in _walk(d["FOV_maps"]):
            for x in fovs:
                n = re.search(r"_(\d+)$", str(x))
                if n:
                    grp.setdefault(path, {})[int(n.group(1))] = str(core)
        for path, m in grp.items():
            if len(m) >= 10:
                maps[f"{os.path.basename(f)}:{path}"] = m

    for f in glob.glob(str(FOVMETA_DIR / "*.csv")):
        try:
            rows = list(csv.DictReader(open(f, encoding="utf-8-sig")))
        except Exception:
            continue
        if not rows or "Sample" not in rows[0]:
            continue
        fk = next((k for k in rows[0] if k and k.lower() in ("fov", "fov_name")), None)
        if not fk:
            continue
        by = {}
        for r in rows:
            try:
                by.setdefault(r.get("Assay", "?"), {})[int(float(r[fk]))] = r["Sample"]
            except Exception:
                continue
        for a, m in by.items():
            if len(m) >= 10:
                maps[f"{os.path.basename(f)}:{a}"] = m
    return maps


def core_patient_map() -> dict:
    """PDAC core -> patient, from Patient_maps (KRAS G12R / G12D groups)."""
    import yaml
    out = {}
    p = CONFIG_DIR / "ProjectConfig_PANC_Combined.yaml"
    if not p.exists():
        return out
    d = yaml.safe_load(open(p))
    pm = d.get("Patient_maps")
    if not isinstance(pm, dict):
        return out
    for _, patient, cores in _walk(pm):
        for c in cores:
            out[str(c).replace("-", "_")] = str(patient).replace("-", "_")
    return out


def compactness(xy, core) -> float:
    """median (within-core spread / distance to nearest other core)."""
    ok = core.notna()
    if ok.sum() == 0:
        return np.inf
    g = xy[ok].groupby(core[ok])
    cen = g.mean().to_numpy()
    spr = g.std().mean(axis=1).to_numpy()
    if len(cen) < 2:
        return np.inf
    D = np.sqrt(((cen[:, None] - cen[None]) ** 2).sum(-1))
    np.fill_diagonal(D, np.inf)
    return float(np.median(spr / D.min(1)))


def try_recorded(obs, sample_id, maps):
    """Best validated recorded map for this sample, or None."""
    org = organ_of(sample_id)
    fv = obs["fov"].astype(int)
    xy = obs[["CenterX_global_px", "CenterY_global_px"]]
    best = None
    for name, m in maps.items():
        cores = {str(v).upper().replace("-", "_") for v in m.values()}
        if not any(c.startswith(org.upper()) for c in cores):
            continue                       # map does not describe this organ
        core = fv.map(m)
        if core.notna().mean() < 0.99:
            continue
        r = compactness(xy, core)
        if r < COMPACT_MAX and (best is None or r < best[2]):
            best = (name, core, r)
    return best


# --------------------------------------------------------------------------
# per-cell sources
# --------------------------------------------------------------------------

def cell_key(df):
    if "cell" in df.columns:
        return df["cell"].astype(str)
    return "c_1_" + df["fov"].astype(str) + "_" + df["cell_ID"].astype(str)


def try_h5ad(obs, sample_id):
    """per-cell SAHA_Sample / patientID, keyed on `cell`."""
    import anndata as ad
    cands = []
    code = organ_of(sample_id)
    for n in (f"SAHA_{code}_RNA.h5ad",):
        p = H5AD_RNA / n
        if p.exists():
            cands.append(p)
    cands += [p for p in H5AD_EXTRA if p.exists()]

    for p in cands:
        try:
            a = ad.read_h5ad(p, backed="r")
        except Exception:
            continue
        col = next((c for c in ("SAHA_Sample", "patientID") if c in a.obs.columns), None)
        if col is None:
            a.file.close()
            continue
        o = a.obs
        has_sample_col = any(f in o.columns for f in ("SAHA_name", "sample_id"))
        if has_sample_col:
            # The h5ad knows which sample each cell came from, so an absent
            # sample means this h5ad simply does not describe it. Do NOT fall
            # back to a slide-wide selection: cell keys are `c_<slide>_<fov>_
            # <cell>` in BOTH assays, so an RNA key collides with an unrelated
            # protein cell and would silently graft RNA donors onto protein.
            hit = False
            for f in ("SAHA_name", "sample_id"):
                if f in o.columns and (o[f].astype(str) == sample_id).any():
                    o = o[o[f].astype(str) == sample_id]
                    hit = True
                    break
            if not hit:
                a.file.close()
                continue
        else:
            # Pooled objects with no sample column (the IBD joint h5ad): the
            # core label itself names the slide, so select on that.
            parts = sample_id.split("_")
            slide = f"{parts[2]}_{parts[3]}" if len(parts) > 3 else ""
            if not slide:
                a.file.close()
                continue
            o = o[o[col].astype(str).str.startswith(slide + "_")]
        o = o.copy()
        a.file.close()
        if not len(o):
            continue
        try:
            o.index = cell_key(o)
        except Exception:
            continue
        s = o[col].astype(str)
        s = s[~s.isin(["nan", ""])]
        s = s[~s.index.duplicated()]
        mapped = obs.index.map(s)
        if mapped.notna().mean() >= 0.5:
            return p.name, col, mapped, float(mapped.notna().mean())
    return None


def try_h5ad_fovmap(obs, sample_id):
    """Derive a FOV -> core map from an h5ad with no usable cell key.

    Some pooled objects (the IBD spatial h5ad) index cells synthetically, so
    per-cell joining is impossible -- but they carry `fov` alongside the core
    label, and on these slides a FOV never spans two cores, so the mapping is
    exact rather than inferred.
    """
    import anndata as ad
    parts = sample_id.split("_")
    slide = f"{parts[2]}_{parts[3]}" if len(parts) > 3 else ""
    if not slide:
        return None

    for p in H5AD_EXTRA:
        if not p.exists():
            continue
        try:
            a = ad.read_h5ad(p, backed="r")
        except Exception:
            continue
        col = next((c for c in ("SAHA_Sample", "patientID") if c in a.obs.columns), None)
        if col is None or "fov" not in a.obs.columns:
            a.file.close()
            continue
        o = a.obs[["fov", col]].copy()
        a.file.close()
        o[col] = o[col].astype(str)
        sub = o[o[col].str.startswith(slide + "_")]
        if not len(sub):
            continue
        t = sub.groupby(sub["fov"].astype(int))[col].agg(["nunique", "first"])
        if (t["nunique"] > 1).any():
            continue                       # a FOV spanning cores is not a clean map
        core = obs["fov"].astype(int).map(t["first"].to_dict())
        cov = float(core.notna().mean())
        if cov < 0.9:
            continue
        r = compactness(obs[["CenterX_global_px", "CenterY_global_px"]], core)
        if r < COMPACT_MAX:
            return p.name, col, core, cov, r
    return None


# --------------------------------------------------------------------------
# geometric registration
# --------------------------------------------------------------------------

FOV_PX = 4256
MASK_GRID = 32
CORR_MIN = 0.25           # a match must actually look like the core
CORR_MARGIN = 1.5         # ...and beat the next-best core by this factor


def footprint(x, y, grid=MASK_GRID):
    """Binary occupancy mask of one core, normalised to its own bounding box.

    Matching on centroids alone fails here: TMA cores sit on a regular grid,
    which is near-symmetric, so many assignments fit almost equally well
    (observed margins of 1.0-1.5x). Tissue morphology -- the holes and ragged
    edges within a core -- survives between consecutive sections and is far
    more distinctive than position.
    """
    x = np.asarray(x, float); y = np.asarray(y, float)
    rx = max(x.max() - x.min(), 1.0)
    ry = max(y.max() - y.min(), 1.0)
    h, _, _ = np.histogram2d((y - y.min()) / ry, (x - x.min()) / rx,
                             bins=grid, range=[[0, 1], [0, 1]])
    pos = h[h > 0]
    return (h > np.percentile(pos, 10)).astype(float) if pos.size else h


def _corr(u, v):
    u = u - u.mean(); v = v - v.mean()
    d = np.sqrt((u ** 2).sum() * (v ** 2).sum())
    return float((u * v).sum() / d) if d else 0.0


def spatial_clusters(obs, thr=1.6, k=None):
    """Group FOVs into cores by spatial gaps. Returns fov -> cluster id.

    With `k`, cut the tree to exactly k groups instead of by distance -- for
    runs whose cores are packed too tightly for a gap to separate them.
    """
    from scipy.cluster.hierarchy import linkage, fcluster
    f = obs.groupby(obs["fov"].astype(int))[
        ["CenterX_global_px", "CenterY_global_px"]].mean()
    if k:
        # Ward on FOV centroids cuts across cores here; k-means recovers the
        # compact, roughly circular punches (compactness 0.19 vs a mess).
        from scipy.cluster.vq import kmeans2
        pts = f.to_numpy(float)
        cen, lab = kmeans2(pts, k, minit="++", seed=0, iter=50)
        lab = lab + 1
    else:
        lab = fcluster(linkage(f.to_numpy(float), method="single"),
                       t=thr * FOV_PX, criterion="distance")
    return dict(zip(f.index, lab))


def register_by_shape(obs, rna_obs, rna_core, sample_id=None):
    """Assign protein clusters to RNA cores by footprint similarity.

    Each cluster is matched independently rather than one-to-one, so a core
    split across two clusters resolves correctly instead of forcing a bijection.
    """
    ref = {}
    ok = rna_core.notna()
    for k, g in rna_obs[ok].groupby(rna_core[ok]):
        ref[k] = footprint(g.CenterX_global_px, g.CenterY_global_px)

    fov2cl = spatial_clusters(obs, k=FORCE_K.get(sample_id))
    cl = obs["fov"].astype(int).map(fov2cl)
    cand, detail = [], []
    for c, g in obs.groupby(cl):
        if len(g) < 50:
            continue
        m = footprint(g.CenterX_global_px, g.CenterY_global_px)
        scores = sorted(((_corr(m, ref[k]), k) for k in ref), reverse=True)
        best, runner = scores[0], (scores[1] if len(scores) > 1 else (0.0, None))
        good = best[0] >= CORR_MIN and best[0] >= CORR_MARGIN * max(runner[0], 1e-9)
        cand.append([int(c), best[1], best[0], runner[0], good])

    # A core is one physical punch, so it cannot appear in two spatially
    # separate clusters. Where that happens, keep the better correlation and
    # drop the other -- without this, COL_A1_3 was assigned to two distant
    # regions of near-equal size, one of which is really core 6.
    bycore = {}
    for row in cand:
        if not row[4]:
            continue
        k = row[1]
        if k in bycore and bycore[k][2] >= row[2]:
            row[4] = False
            row.append("duplicate of cluster %d" % bycore[k][0])
        elif k in bycore:
            bycore[k][4] = False
            bycore[k].append("duplicate of cluster %d" % row[0])
            bycore[k] = row
        else:
            bycore[k] = row

    out = {r[0]: r[1] for r in cand if r[4]}
    core = cl.map(out)
    detail = [tuple(r) for r in cand]
    return core, detail


# --------------------------------------------------------------------------

def fov_centroids(obs):
    return obs.groupby(obs["fov"].astype(int))[
        ["CenterX_global_px", "CenterY_global_px"]].mean()


def try_fov_layout(obs, sample_id, resolved, obs_cache):
    """Transfer a resolved run's FOV -> core map when both share a FOV layout.

    Consecutive sections re-imaged with the same FOV plan put FOV n at the same
    physical place in both runs, so the map carries over exactly. For LN_A1 the
    two runs share 246 of 250 FOVs with a median offset of 566 px -- an eighth
    of a FOV -- while ILE and PANC_SJ01 differ by 15-30 FOV widths and are
    correctly refused.
    """
    parts = sample_id.split("_")
    slide = f"{parts[2]}_{parts[3]}" if len(parts) > 3 else ""
    if not slide:
        return None
    mine = fov_centroids(obs)

    for other, (core, src, *_ ) in resolved.items():
        if core is None or other == sample_id:
            continue
        op = other.split("_")
        if len(op) < 4 or f"{op[2]}_{op[3]}" != slide:
            continue
        theirs = fov_centroids(obs_cache[other])
        common = sorted(set(mine.index) & set(theirs.index))
        if len(common) < 0.8 * len(mine):
            continue
        d = np.sqrt(((mine.loc[common].to_numpy(float)
                      - theirs.loc[common].to_numpy(float)) ** 2).sum(1))
        offset = float(np.median(d))
        if offset > FOV_MATCH_PX:
            continue
        f2c = obs_cache[other].groupby(obs_cache[other]["fov"].astype(int))[
            "__core__"].agg(lambda s: s.dropna().iloc[0] if s.notna().any() else None) \
            if "__core__" in obs_cache[other] else None
        if f2c is None:
            tmp = obs_cache[other].assign(__c__=core)
            f2c = tmp.groupby(tmp["fov"].astype(int))["__c__"].agg(
                lambda s: s.dropna().iloc[0] if s.notna().any() else None)
        mapped = obs["fov"].astype(int).map(f2c.to_dict())
        cov = float(mapped.notna().mean())
        if cov < 0.9:
            continue
        r = compactness(obs[["CenterX_global_px", "CenterY_global_px"]], mapped)
        if r < COMPACT_MAX:
            return other, mapped, cov, r, offset
    return None


STRAY_FRAC = 0.10         # a piece below this share of its core is a stray


def drop_stray_pieces(obs, core):
    """Remove cells stranded far from the body of their core.

    NOT "keep only the largest piece": a core is one punch but its tissue can
    legitimately arrive in several fragments -- ILE_A1_1 and ILE_A1_2 each
    split roughly 60/40, and discarding the smaller half threw away a quarter
    of the sample. Only pieces below STRAY_FRAC of the core are dropped, which
    catches the 1% specks that recorded maps strand in a neighbouring core.
    """
    from scipy.cluster.hierarchy import linkage, fcluster
    core = core.copy()
    fov = obs["fov"].astype(int)
    for k in [x for x in core.dropna().unique()]:
        m = core == k
        if m.sum() < 200:
            continue
        sub = obs.loc[m]
        f = sub.groupby(fov[m])[["CenterX_global_px", "CenterY_global_px"]].mean()
        if len(f) < 2:
            continue
        lab = fcluster(linkage(f.to_numpy(float), method="single"),
                       t=1.6 * FOV_PX, criterion="distance")
        if len(set(lab)) < 2:
            continue
        per_fov = sub.groupby(fov[m]).size()
        total = int(per_fov.sum())
        drop_fovs = set()
        for c in set(lab):
            fovs = [int(f.index[i]) for i in range(len(f)) if lab[i] == c]
            if per_fov[fovs].sum() < STRAY_FRAC * total:
                drop_fovs |= set(fovs)
        if drop_fovs:
            core[m & fov.isin(drop_fovs)] = None
    return core


def plot_slide(slide, panels, out_png):
    """One panel per sample, cells coloured by donor core."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    n = len(panels)
    fig, axes = plt.subplots(1, n, figsize=(6.2 * n, 6.4), squeeze=False)
    cores = sorted({c for _, _, cc in panels for c in set(cc.dropna())})
    cmap = plt.get_cmap("tab20")
    colour = {c: cmap(i % 20) for i, c in enumerate(cores)}

    for ax, (sample, xy, core) in zip(axes[0], panels):
        ok = core.notna()
        ax.scatter(xy.loc[~ok, "CenterX_global_px"], xy.loc[~ok, "CenterY_global_px"],
                   s=0.4, c="0.85", linewidths=0, rasterized=True)
        for c in sorted(set(core[ok])):
            m = core == c
            ax.scatter(xy.loc[m, "CenterX_global_px"], xy.loc[m, "CenterY_global_px"],
                       s=0.4, color=colour[c], linewidths=0, rasterized=True, label=c)
            cx, cy = xy.loc[m, "CenterX_global_px"].mean(), xy.loc[m, "CenterY_global_px"].mean()
            ax.annotate(str(c).replace(slide + "_", ""), (cx, cy), fontsize=8,
                        ha="center", va="center", weight="bold",
                        bbox=dict(fc="white", ec="none", alpha=0.7, pad=0.6))
        assay = "protein" if "_CP_" in sample else "RNA"
        unres = any(str(c).startswith("?") for c in set(core[ok]))
        label = ("UNRESOLVED — spatial clusters only"
                 if unres else f"{core[ok].nunique()} cores")
        ax.set_title(f"{sample}\n{assay} · {ok.sum():,}/{len(core):,} cells · {label}",
                     fontsize=9, color=("crimson" if unres else "black"))
        ax.set_aspect("equal"); ax.invert_yaxis()
        ax.set_xticks([]); ax.set_yticks([])
    fig.suptitle(f"{slide} — donor cores  (compare RNA vs protein)", fontsize=11)
    fig.tight_layout()
    fig.savefig(out_png, dpi=130, bbox_inches="tight")
    plt.close(fig)


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--zarr-dir", type=Path, required=True)
    ap.add_argument("--out-dir", type=Path, required=True)
    ap.add_argument("--only", nargs="*")
    ap.add_argument("--manual", type=Path,
                    help="CSV of hand-assigned clusters (sample_id,cluster,core). "
                         "Overrides everything else. A template listing the "
                         "unresolved clusters is written to the out-dir each run.")
    ap.add_argument("--fov-overrides", type=Path,
                    help="CSV of per-FOV corrections (sample_id,fov,core). "
                         "Applied last; `core` empty unassigns the FOV.")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    import spatialdata as sd
    import pandas as pd

    args.out_dir.mkdir(parents=True, exist_ok=True)
    maps = load_maps()
    c2p = core_patient_map()
    print(f"{len(maps)} recorded FOV->core maps; "
          f"{len(c2p)} core->patient entries\n")

    stores = sorted(args.zarr_dir.glob("*.zarr"))
    if args.only:
        stores = [z for z in stores if z.stem in args.only]

    obs_cache, result = {}, {}
    for z in stores:
        s = z.stem
        obs = sd.read_zarr(z).tables["table"].obs
        obs_cache[s] = obs
        got = try_h5ad(obs, s)
        if got:
            f, col, mapped, cov = got
            result[s] = (pd.Series(mapped, index=obs.index), f"h5ad:{f}:{col}", cov, np.nan)
            print(f"  {s:24s} h5ad {col:12s} {cov:6.1%}  {pd.Series(mapped).nunique()} cores  ({f})")
            continue
        rec = try_recorded(obs, s, maps)
        if rec:
            name, core, r = rec
            result[s] = (core, f"map:{name}", 1.0, r)
            print(f"  {s:24s} map  {core.nunique():2d} cores  compact {r:.3f}  ({name})")
            continue
        got = try_h5ad_fovmap(obs, s)
        if got:
            f, col, core, cov, r = got
            result[s] = (core, f"h5ad-fovmap:{f}:{col}", cov, r)
            print(f"  {s:24s} h5ad-fovmap {core.nunique():2d} cores  {cov:6.1%}  "
                  f"compact {r:.3f}  ({f})")
            continue
        print(f"  {s:24s} unresolved — needs registration")
        result[s] = (None, None, 0.0, np.nan)

    # registration for the protein runs still unresolved
    print()
    for s, rna in RNA_COUNTERPART.items():
        if s not in result or result[s][0] is not None:
            continue
        if rna not in result or result[rna][0] is None:
            print(f"  {s:24s} cannot register — {rna} unresolved")
            continue
        obs, robs = obs_cache[s], obs_cache[rna]
        core, detail = register_by_shape(obs, robs, result[rna][0], s)
        cov = float(core.notna().mean())
        good = [d for d in detail if d[4]]
        print(f"  {s:24s} shape-match vs {rna}: {len(detail)} clusters, "
              f"{len(good)} assigned -> {core.nunique()} cores, {cov:.1%} of cells")
        for d in sorted(detail, key=lambda d: -d[2]):
            c, k, b, r, ok_ = d[:5]; note = d[5] if len(d) > 5 else ""
            print(f"      cluster {c:>3} -> {str(k):16s} corr {b:.3f} "
                  f"(next {r:.3f}) {'OK' if ok_ else 'rejected'} {note}")
        result[s] = ((core, f"shape:{rna}", cov, np.nan) if cov >= 0.5
                     else (None, None, 0.0, np.nan))

    # Transfer from a run on the same slide that shares a FOV layout. Runs last
    # so it only fills what nothing better resolved.
    for s in [k for k, v in result.items() if v[0] is None]:
        got = try_fov_layout(obs_cache[s], s, result, obs_cache)
        if got:
            other, core, cov, r, offset = got
            result[s] = (core, f"fov-layout:{other}", cov, r)
            print(f"  {s:24s} fov-layout from {other} "
                  f"(median FOV offset {offset:,.0f} px) "
                  f"{core.nunique()} cores {cov:.1%} compact {r:.3f}")

    # Hand assignments win over everything: they come from someone reading the
    # QC figure, which has caught what the automatic methods could not.
    manual = {}
    if args.manual and args.manual.exists():
        for r in csv.DictReader(open(args.manual)):
            core = (r.get("core") or "").strip()
            if core:
                manual.setdefault(r["sample_id"].strip(), {})[
                    str(r["cluster"]).strip().lstrip("?")] = core
    for s, assign in manual.items():
        if s not in obs_cache:
            print(f"  WARNING: manual entry for unknown sample {s}")
            continue
        obs = obs_cache[s]
        prior = result.get(s, (None,))[0]
        # For a partly-resolved sample the clusters offered for hand labelling
        # are those of the cells still unassigned, so existing work is kept.
        scope = obs if prior is None else obs[prior.isna()]
        fov2cl = spatial_clusters(scope, k=FORCE_K.get(s))
        cl = obs["fov"].astype(int).map(fov2cl).astype("Int64").astype(str)
        core = cl.map(assign)
        if prior is not None:
            core = prior.where(prior.notna(), core)
        cov = float(core.notna().mean())
        r = compactness(obs_cache[s][["CenterX_global_px", "CenterY_global_px"]], core)
        result[s] = (core, "manual", cov, r)
        print(f"  {s:24s} MANUAL {core.nunique()} cores  {cov:.1%}  compact {r:.3f}"
              + ("" if r < COMPACT_MAX else "   <-- check: clusters not compact"))

    # Corrections confirmed against other runs on the same slide.
    for s, fix in CORE_LABEL_FIX.items():
        if s in result and result[s][0] is not None:
            core, src, cov, m = result[s]
            result[s] = (core.map(lambda v: fix.get(v, v)), src + "+fix", cov, m)
            print(f"  {s:24s} applied label fix {fix}")

    # Per-FOV corrections, applied last so they beat every other source.
    if args.fov_overrides and args.fov_overrides.exists():
        bys = {}
        for r in csv.DictReader(open(args.fov_overrides)):
            bys.setdefault(r["sample_id"].strip(), {})[int(r["fov"])] = \
                (r.get("core") or "").strip() or None
        for s_, fixes in bys.items():
            if s_ not in result or result[s_][0] is None:
                print(f"  WARNING: FOV override for unresolved sample {s_}")
                continue
            core_, src_, cov_, m_ = result[s_]
            fv = obs_cache[s_]["fov"].astype(int)
            for fov, val in fixes.items():
                core_ = core_.mask(fv == fov, val)
            n = int(fv.isin(fixes).sum())
            result[s_] = (core_, (src_ or "") + "+fov_fix",
                          float(core_.notna().mean()), m_)
            print(f"  {s_:24s} FOV override on {len(fixes)} FOV(s), {n:,} cells: "
                  + ", ".join(f"{k}->{v or 'unassigned'}" for k, v in sorted(fixes.items())))

    # No stray-piece pruning. It assumed a core is spatially contiguous, which
    # is false -- ILE cores split roughly 60/40 and the rule deleted 30% of a
    # sample, and on COL_A1_SJ01 it removed cells Jiwoon confirmed belong to
    # cores 5 and 7. The duplicate-core guard in register_by_shape covers the
    # error this was meant to catch.

    # outputs
    rows = []
    for s, (core, src, cov, metric) in result.items():
        n = 0 if core is None else int(core.nunique())
        rows.append({"sample_id": s, "source": src or "UNRESOLVED",
                     "n_cores": n, "coverage": round(cov, 4),
                     "metric": None if np.isnan(metric) else round(metric, 3)})
        if core is not None and not args.dry_run:
            df = pd.DataFrame({"cell": obs_cache[s].index, "donor_core": core.values})
            if c2p:
                df["donor_patient"] = df["donor_core"].map(
                    lambda c: c2p.get(str(c).replace("-", "_")))
            df.to_csv(args.out_dir / f"{s}.donor_core.csv", index=False)
    pd.DataFrame(rows).sort_values("sample_id").to_csv(
        args.out_dir / "resolution_report.csv", index=False)

    # Template for hand assignment: one row per cluster of every unresolved
    # sample, with the position and size needed to find it on the QC figure.
    tmpl = []
    for s, (core, src, *_ ) in result.items():
        if core is not None and core.isna().sum() < 2000:
            continue
        obs = obs_cache[s]
        scope = obs if core is None else obs[core.isna()]
        fov2cl = spatial_clusters(scope, k=FORCE_K.get(s))
        cl = scope["fov"].astype(int).map(fov2cl)
        obs = scope
        for c, g in obs.groupby(cl):
            tmpl.append({
                "sample_id": s,
                "cluster": f"?{int(c)}",
                "n_cells": len(g),
                "x": int(g.CenterX_global_px.mean()),
                "y": int(g.CenterY_global_px.mean()),
                "core": "",
            })
    if tmpl:
        t = args.out_dir / "manual_template.csv"
        pd.DataFrame(tmpl).sort_values(["sample_id", "n_cells"],
                                       ascending=[True, False]).to_csv(t, index=False)
        print(f"\nhand-assignment template: {t}")
        print("  fill the `core` column from the QC figure, then re-run with "
              "--manual " + str(t))

    # QC figures, grouped by slide so RNA and protein sit side by side
    # Unresolved samples are plotted too, with their spatial clusters shown as
    # anonymous ?1, ?2 ... An absent panel hides the problem; an anonymous one
    # lets the layout be matched against the RNA by eye.
    slides = {}
    for s, (core, *_ ) in result.items():
        if core is None:
            fov2cl = spatial_clusters(obs_cache[s], k=FORCE_K.get(s))
            core = obs_cache[s]["fov"].astype(int).map(
                {k: f"?{v}" for k, v in fov2cl.items()})
        p = s.split("_")
        slides.setdefault(f"{p[2]}_{p[3]}", []).append(
            (s, obs_cache[s][["CenterX_global_px", "CenterY_global_px"]], core))
    backup = args.out_dir / "previous"
    for slide, panels in sorted(slides.items()):
        panels.sort(key=lambda x: ("_CP_" in x[0], x[0]))
        out = args.out_dir / f"cores_{slide}.png"
        # Never clobber a figure silently -- these get annotated by hand, and a
        # re-run once destroyed a set of markings that had to be redone.
        if out.exists():
            backup.mkdir(parents=True, exist_ok=True)
            import shutil as _sh
            _sh.copy2(out, backup / out.name)
        plot_slide(slide, panels, out)
        print(f"  figure {out.name}  ({len(panels)} panels)")

    ok = sum(1 for r in rows if r["source"] != "UNRESOLVED")
    print(f"\n{ok} of {len(rows)} samples resolved")
    print(f"report: {args.out_dir/'resolution_report.csv'}")


if __name__ == "__main__":
    main()
