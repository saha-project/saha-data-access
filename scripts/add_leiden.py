#!/usr/bin/env python3
"""
add_leiden.py
Give every release object at least a clustering: obs['leiden'] on each table that
has neither cell types nor an existing clustering (27 objects on 2026-10-05).
This is a placeholder for real cell typing, not a substitute for it.

Two steps, so every copy of an object carries identical labels:

  compute  one object per task, read from its Zenodo-tier (or v2) store.
           RNA: drop control probes, normalize_total + log1p, 2000 HVGs when the
           panel is larger, scale. Protein: drop controls, log1p, scale.
           Then PCA 30, 15 neighbours, Leiden res 0.5 (igraph). Labels go to a
           sidecar parquet (obs index -> leiden), nothing is written to the store.

  apply    one store per task (Zenodo copy, AWS full-res copy, v2). Writes the
           sidecar labels into obs['leiden'] of a hardlinked copy
           `<out>/<name>.partial`: images/shapes stay shared, while the tables/
           tree and root metadata -- everything a table write touches -- are
           un-shared first, and the source's root metadata is checksummed before
           and after to prove the source was not modified. Read back, check,
           rename. Swapping --out in is a separate step.

Which objects: read from the QC results (`label_kind` per table, written by
qc_release_objects.py), never from a hand-kept list.

Usage (on Cayuga):
    P=~/.venv-sd08/bin/python
    $P scripts/add_leiden.py list    --qc-dir .../release_qc/qc
    $P scripts/add_leiden.py compute --qc-dir ... --labels .../release_qc/leiden --index 0
    $P scripts/add_leiden.py apply   --qc-dir ... --labels ... --out-suffix _leiden --index 0
"""

import argparse
import hashlib
import json
import os
import re
import shutil
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

D = Path("/athena/project-saha/derived")
S3 = D / "release/s3/processed"   # full-res twin of a build/zenodo_tier store (organize_derived.py)
# control probes, isotype controls (spelled with and without a space), and the
# CosMx morphology-stain intensities that some protein tables carry as features
CONTROLS = re.compile(r"^(NegPrb|Negative|FalseCode|SystemControl|BLANK|Blank|NegControl|"
                      r"UnassignedCodeword|DeprecatedCodeword|Intergenic|Ms ?IgG|Rb ?IgG|IgG|Channel-)", re.I)
PARAMS = {"method": "scanpy leiden (igraph)", "resolution": 0.5, "n_pcs": 30, "n_neighbors": 15,
          "n_hvg": 2000, "random_state": 0}


def todo(qc_dir: Path):
    """[(sample, store, [tables])] for objects with no cell type and no clustering.

    Store paths come from the QC records. One record per object: its Zenodo-tier
    store, the shared store (Dulai/Xenium), or the v2 store -- never the AWS twin.
    """
    out = []
    for f in sorted(qc_dir.rglob("*.json")):
        j = json.loads(f.read_text())
        if j.get("tier") not in ("zenodo", "both", "v2"):
            continue
        tabs = [t["table"] for t in j.get("table_checks", [])
                if t.get("label_kind") not in ("cell type", "cluster")]
        if tabs:
            out.append((j["sample_id"], j["path"], tabs))
    return out


def targets(qc_dir: Path):
    """Every store to write: the source plus, for a 4x store, its full-res twin."""
    out = []
    for sid, store, tabs in todo(qc_dir):
        out.append((sid, store, tabs))
        twin = S3 / Path(store).parent.name / Path(store).name
        if "zenodo_tier" in store and twin.exists():
            out.append((sid, str(twin), tabs))
    return out


def cluster(t, assay):
    import scanpy as sc
    keep = ~t.var_names.str.match(CONTROLS)
    a = t[:, keep].copy()
    a.X = a.X.astype("float32")
    if assay == "protein":
        sc.pp.log1p(a)
    else:
        sc.pp.normalize_total(a)
        sc.pp.log1p(a)
        if a.n_vars > PARAMS["n_hvg"]:
            sc.pp.highly_variable_genes(a, n_top_genes=PARAMS["n_hvg"], subset=True)
    sc.pp.scale(a, max_value=10)
    sc.pp.pca(a, n_comps=min(PARAMS["n_pcs"], a.n_vars - 1), random_state=0)
    sc.pp.neighbors(a, n_neighbors=PARAMS["n_neighbors"], random_state=0)
    sc.tl.leiden(a, resolution=PARAMS["resolution"], flavor="igraph", n_iterations=2,
                 random_state=0)
    return a.obs["leiden"], int(keep.sum())


def compute(sid, store, tabs, labels: Path):
    warnings.filterwarnings("ignore")
    from spatialdata import read_zarr
    sd = read_zarr(store)
    for tname in tabs:
        t = sd.tables[tname]
        assay = "protein" if (tname == "protein" or "_CP_" in sid) else "rna"
        lab, n_feat = cluster(t, assay)
        out = labels / f"{sid}__{tname}.parquet"
        pd.DataFrame({"leiden": lab.astype(str).values}, index=t.obs.index.astype(str)).to_parquet(out)
        print(f"{sid} [{tname}, {assay}]: {t.n_obs:,} cells, {n_feat} features -> "
              f"{lab.nunique()} clusters, largest {lab.value_counts(normalize=True).iloc[0]:.0%}")


def md5(p: Path):
    return hashlib.md5(p.read_bytes()).hexdigest() if p.exists() else None


def unshare(p: Path):
    """Give a hardlinked file its own inode, so writing it cannot touch the source."""
    tmp = p.with_name(p.name + ".unshare")
    shutil.copy2(p, tmp)
    os.replace(tmp, p)


def apply(sid, store, tabs, labels: Path, suffix: str):
    warnings.filterwarnings("ignore")
    from spatialdata import read_zarr
    from spatialdata.models import TableModel

    src = Path(store)
    out_dir = src.parent.with_name(src.parent.name + suffix)   # e.g. saha-v1-colon_leiden/
    out_dir.mkdir(exist_ok=True)
    final, tmp = out_dir / src.name, out_dir / (src.name + ".partial")
    if final.exists():
        print(f"{src.parent.name}/{sid}: already done, skipping")
        return
    if tmp.exists():
        shutil.rmtree(tmp)  # our own half-finished copy; its links never reach the source
    root_meta = [n for n in ("zarr.json", ".zmetadata", ".zattrs", ".zgroup") if (src / n).exists()]
    before = {n: md5(src / n) for n in root_meta}

    shutil.copytree(src, tmp, copy_function=os.link)          # hardlinks: no data copied
    for n in root_meta:
        unshare(tmp / n)
    for f in (tmp / "tables").rglob("*"):
        if f.is_file():
            unshare(f)

    sd = read_zarr(tmp)
    new_tables = {}
    for tname in tabs:
        t = sd.tables[tname]
        lab = pd.read_parquet(labels / f"{sid}__{tname}.parquet")["leiden"]
        if not lab.index.equals(pd.Index(t.obs.index.astype(str))):
            sys.exit(f"{src.parent.name}/{sid} [{tname}]: label rows do not match obs -- not writing")
        if "leiden" in t.obs:
            sys.exit(f"{src.parent.name}/{sid} [{tname}]: obs already has 'leiden' -- refusing to overwrite")
        new = t.copy()
        new.obs["leiden"] = pd.Categorical(lab.values,
                                           categories=sorted(lab.unique(), key=int))
        new.uns["leiden_params"] = dict(PARAMS)
        attrs = new.uns.pop("spatialdata_attrs")
        new_tables[tname] = TableModel.parse(new, region=attrs["region"], region_key=attrs["region_key"],
                                             instance_key=attrs["instance_key"])  # validated first
    for tname, new in new_tables.items():
        sd.tables[tname] = new
        sd.delete_element_from_disk(tname)
        sd.write_element(tname)

    after = {n: md5(src / n) for n in root_meta}
    assert before == after, f"source root metadata changed: {before} -> {after}"
    back = read_zarr(tmp)
    for tname in tabs:
        b, t = back.tables[tname], sd.tables[tname]
        assert b.n_obs == t.n_obs and "leiden" in b.obs and b.obs["leiden"].notna().all()
        assert list(b.obs.columns) == list(t.obs.columns)
    assert set(back.images) == set(sd.images) and set(back.shapes) == set(sd.shapes)
    tmp.rename(final)
    print(f"{src.parent.name}/{sid}: leiden written to {', '.join(tabs)}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["list", "compute", "apply"])
    ap.add_argument("--qc-dir", type=Path, required=True)
    ap.add_argument("--labels", type=Path)
    ap.add_argument("--out-suffix", default="_leiden")
    ap.add_argument("--index", type=int)
    a = ap.parse_args()
    if a.mode == "list":
        for i, x in enumerate(todo(a.qc_dir)):
            print("compute", i, *x)
        for i, x in enumerate(targets(a.qc_dir)):
            print("apply", i, *x)
        return
    a.labels.mkdir(parents=True, exist_ok=True)
    if a.mode == "compute":
        compute(*todo(a.qc_dir)[a.index], a.labels)
    else:
        apply(*targets(a.qc_dir)[a.index], a.labels, a.out_suffix)


if __name__ == "__main__":
    main()
