#!/usr/bin/env python3
"""
apply_annotations.py
Write cell-type and related labels from the legacy Seurat objects into the
TAP SpatialData objects.

Unlike apply_donor_core.py this is a PARTIAL join: the RDS objects were
annotated after QC, so they cover 46-100% of the cells in an export and the
row order does not match. Cells with no label get NA rather than a guess.

Join key
--------
CosMx cell ids are `c_<slide>_<fov>_<cell>`. The slide number drifts between
objects -- GI_after_Niche.rds re-keyed each organ (appendix=2, ileum=3,
colon=1) while every flat-file export uses slide 1 -- so the prefix is
stripped and the join runs on `<fov>_<cell>`, which is unique within one
acquisition.

Columns written (only those present in the CSV):
    cell_type           broad or primary label
    cell_type_detailed  finer label where a second tier exists
    spatial_niche       spatial domain, NOT a cell type
    pathology_region    pathologist region call (CRC only)
    pathology_detail    free-text region description (CRC only)
    donor_core          <SLIDE>_<CORE>, joins donors.parquet
    cluster             unnamed cluster id, where no biological label exists

Usage:
    P=~/.venv-sd08/bin/python
    $P scripts/apply_annotations.py --zarr-dir /athena/project-saha/derived/build/zenodo_tier/<record_slug> \
        --annot-dir /athena/masonlab/scratch/users/jip2007/code/annot --dry-run
    $P scripts/apply_annotations.py --zarr-dir ... --annot-dir ...
"""

import argparse
import re
import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

COLS = ["cell_type", "cell_type_detailed", "spatial_niche",
        "pathology_region", "pathology_detail", "donor_core", "cluster"]

# spatialdata owns these: `region` is the TableModel region_key (every row reads
# "cells") and `cell` is the instance_key. Writing either breaks the table's
# wiring to shapes/cells, and `region` in particular looks like a plausible
# annotation name -- hence the explicit guard rather than a comment.
RESERVED = {"region", "cell", "instance_key", "region_key"}
assert not (set(COLS) & RESERVED)

SLIDE_PREFIX = re.compile(r"^c_\d+_")


def join_key(ids):
    """c_<slide>_<fov>_<cell> -> <fov>_<cell>; anything else passes through."""
    return [SLIDE_PREFIX.sub("", str(i)) for i in ids]


def apply_one(zarr_path: Path, csvs: list, dry: bool, overwrite: bool = False):
    import pandas as pd
    import spatialdata as sd

    sdata = sd.read_zarr(zarr_path)
    table = sdata.tables["table"]
    keys = pd.Index(join_key(table.obs.index))
    if keys.has_duplicates:
        raise ValueError(f"{zarr_path.name}: join key is not unique after "
                         f"stripping the slide prefix")

    added = {}
    for csv_path in csvs:                      # later files fill gaps / add tiers
        d = pd.read_csv(csv_path, dtype=str)
        if "join_key" not in d.columns:
            raise ValueError(f"{csv_path.name}: no join_key column")
        d = d.drop_duplicates("join_key").set_index("join_key")
        for c in COLS:
            if c not in d.columns:
                continue
            if c in RESERVED:
                raise ValueError(f"{c} is reserved by spatialdata")
            vals = d[c].reindex(keys)
            vals = vals.where(vals.notna() & (vals != ""), other=None)
            if vals.notna().sum() == 0:
                continue
            if c in table.obs.columns and not overwrite:   # fill gaps, never clobber
                prev = table.obs[c].astype(object)
                vals = pd.Series(prev.to_numpy(), index=keys).fillna(
                    pd.Series(vals.to_numpy(), index=keys))
            table.obs[c] = pd.Categorical(vals.to_numpy())
            n = int(table.obs[c].notna().sum())
            added[c] = (n, 100.0 * n / table.n_obs, int(table.obs[c].nunique()))

    if not added:
        return None
    if not dry:
        # write_element(overwrite=True) refuses to write into its own store, so
        # the element is removed first -- same pattern as apply_donor_core.py.
        sdata.delete_element_from_disk("table")
        sdata.write_element("table")
    return {"n_obs": table.n_obs, "added": added}


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--zarr-dir", type=Path, required=True)
    ap.add_argument("--annot-dir", type=Path, required=True)
    ap.add_argument("--only", nargs="*")
    ap.add_argument("--overwrite", action="store_true",
                    help="replace an existing column instead of only filling its "
                         "gaps; needed when re-running to correct earlier values")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    ok = miss = fail = 0
    for zarr_path in sorted(args.zarr_dir.glob("*.zarr")):
        sid = zarr_path.stem
        if args.only and sid not in args.only:
            continue
        # <sample>.csv first, then <sample>__extra.csv to layer a finer tier
        csvs = [p for p in (args.annot_dir / f"{sid}.csv",
                            args.annot_dir / f"{sid}__extra.csv") if p.exists()]
        if not csvs:
            print(f"{sid:30s} — no annotation source")
            miss += 1
            continue
        try:
            r = apply_one(zarr_path, csvs, args.dry_run, args.overwrite)
        except Exception as e:
            print(f"{sid:30s} FAILED {type(e).__name__}: {e}")
            fail += 1
            continue
        if r is None:
            print(f"{sid:30s} — source present but no usable column")
            miss += 1
            continue
        parts = ", ".join(f"{c}={n:,} ({p:.0f}%, {k} lvl)"
                          for c, (n, p, k) in r["added"].items())
        print(f"{sid:30s} {r['n_obs']:8,d} cells  {parts}")
        ok += 1

    print(f"\nannotated {ok}, no source {miss}, failed {fail}"
          + ("  [dry run — nothing written]" if args.dry_run else ""))
    return 1 if fail else 0


if __name__ == "__main__":
    sys.exit(main())
