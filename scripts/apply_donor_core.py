#!/usr/bin/env python3
"""
apply_donor_core.py
Write per-cell donor identity into the CosMx SpatialData objects.

Takes the resolved assignments from resolve_donor_cores.py and adds to
`tables/table.obs`:

    donor_core     <SLIDE>_<CORE>, e.g. COL_A1_5 -- joins to SampleID in the
                   cohort table of the manuscript, and to donors.parquet
    donor_patient  only where a core->patient grouping exists (PDAC: 28 cores
                   from 14 patients, two cores each, KRAS G12R / G12D)

These slides are tissue microarrays, so donor is a per-CELL property: one
acquisition covers 2-28 donors. A scalar donor_id per sample cannot express
that and is wrong wherever it appears.

Cells with no assignment get NA rather than a guess -- mostly cells the
published h5ads dropped in QC, which is also what `qc_pass` marks.

Usage:
    python scripts/apply_donor_core.py --zarr-dir ~/SAHA_zarr_build \
        --qc-dir zenodo/donor_qc --dry-run

    python scripts/apply_donor_core.py --zarr-dir ~/SAHA_zarr_build \
        --qc-dir zenodo/donor_qc
"""

import argparse
import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

COLS = ["donor_core", "donor_patient"]


def norm(v):
    """Config files spell PDAC cores with hyphens; the release uses snake_case."""
    if v is None:
        return None
    s = str(v).strip()
    return None if s in ("", "nan", "None") else s.replace("-", "_")


def apply_one(zarr_path: Path, csv_path: Path, dry: bool):
    import pandas as pd
    import spatialdata as sd

    sdata = sd.read_zarr(zarr_path)
    table = sdata.tables["table"]
    d = pd.read_csv(csv_path)

    if len(d) != table.n_obs:
        raise ValueError(f"{len(d):,} rows in CSV vs {table.n_obs:,} cells in object")
    if "cell" in d.columns and not (d["cell"].astype(str).to_numpy()
                                    == table.obs.index.to_numpy()).all():
        raise ValueError("cell order in CSV does not match the object")

    added = {}
    for c in COLS:
        if c not in d.columns:
            continue
        vals = d[c].map(norm)
        if vals.notna().sum() == 0:
            continue
        table.obs[c] = pd.Categorical(vals.to_numpy())
        added[c] = (int(vals.notna().sum()), int(vals.nunique()))

    if not added:
        return None
    if not dry:
        # write_element(overwrite=True) refuses to write into its own store, so
        # the element is removed first -- same pattern as create_cosmx_zarr.py.
        sdata.delete_element_from_disk("table")
        sdata.write_element("table")
    return {"n_obs": table.n_obs, "added": added}


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--zarr-dir", type=Path, required=True)
    ap.add_argument("--qc-dir", type=Path, required=True)
    ap.add_argument("--only", nargs="*")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    csvs = sorted(args.qc_dir.glob("*.donor_core.csv"))
    if args.only:
        csvs = [c for c in csvs if c.name.split(".")[0] in args.only]
    if not csvs:
        sys.exit(f"ERROR: no *.donor_core.csv in {args.qc_dir}")

    print(f"{len(csvs)} assignment file(s)\n")
    ok = fail = 0
    for c in csvs:
        sample = c.name.split(".")[0]
        z = args.zarr_dir / f"{sample}.zarr"
        if not z.exists():
            print(f"  {sample:24s} SKIP — no object at {z}")
            continue
        try:
            r = apply_one(z, c, args.dry_run)
        except Exception as e:
            print(f"  {sample:24s} FAILED {type(e).__name__}: {e}")
            fail += 1
            continue
        if r is None:
            print(f"  {sample:24s} nothing to add")
            continue
        bits = ", ".join(f"{k} {v[0]:,}/{r['n_obs']:,} cells, {v[1]} values"
                         for k, v in r["added"].items())
        print(f"  {sample:24s} {bits}")
        ok += 1

    print(f"\n{ok} object(s) updated, {fail} failed"
          + ("   [dry-run — nothing written]" if args.dry_run else ""))
    if fail:
        sys.exit(1)


if __name__ == "__main__":
    main()
