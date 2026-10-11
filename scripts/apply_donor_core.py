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


def fill_by_fov(d):
    """Assign donor_core to cells the published h5ad dropped, via their FOV.

    The assignments were resolved against the published QC-filtered h5ads, so
    cells those h5ads dropped came through with no donor -- 108,480 of them,
    4% of the CosMx cells. They are real cells on a real core, and the core is
    knowable: a FOV sits on exactly one core of the microarray, and the cell id
    encodes its FOV (`c_<slide>_<fov>_<cell>`).

    This is only sound while no FOV straddles two cores, so that is checked
    rather than assumed -- a FOV with two cores is left alone, never
    majority-voted. Cells in FOVs with no assignment at all stay NA.

    Returns (filled, ambiguous_fovs).
    """
    import pandas as pd

    if "cell" not in d.columns or d["donor_core"].isna().sum() == 0:
        return 0, []

    fov = d["cell"].astype(str).str.split("_").str[2]
    known = d.loc[d["donor_core"].notna()]
    known_fov = fov[d["donor_core"].notna()]

    per_fov = known.groupby(known_fov)["donor_core"].unique()
    ambiguous = [f for f, v in per_fov.items() if len(v) > 1]
    mapping = {f: v[0] for f, v in per_fov.items() if len(v) == 1}

    gap = d["donor_core"].isna()
    filled_vals = fov[gap].map(mapping)
    n = int(filled_vals.notna().sum())
    if n:
        d.loc[gap, "donor_core"] = filled_vals
    return n, ambiguous


def apply_one(zarr_path: Path, csv_path: Path, dry: bool):
    import pandas as pd
    import spatialdata as sd

    sdata = sd.read_zarr(zarr_path)
    if not sdata.tables:
        raise ValueError("object has no table")
    table = sdata.tables[list(sdata.tables)[0]]
    d = pd.read_csv(csv_path)

    if len(d) != table.n_obs:
        raise ValueError(f"{len(d):,} rows in CSV vs {table.n_obs:,} cells in object")
    if "cell" in d.columns and not (d["cell"].astype(str).to_numpy()
                                    == table.obs.index.to_numpy()).all():
        raise ValueError("cell order in CSV does not match the object")

    filled, ambiguous = fill_by_fov(d)
    if ambiguous:
        raise ValueError(f"{len(ambiguous)} FOV(s) span more than one donor core "
                         f"({ambiguous[:5]}) -- refusing to infer")

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
    if filled:
        added.setdefault("_filled_by_fov", (filled, 0))
    if not dry:
        # write_element(overwrite=True) refuses to write into its own store, so
        # the element is removed first. That ordering is destructive: if the
        # write raises, or the process is killed between the two calls, the
        # object is left with no table at all. Validate first, so a bad table is
        # rejected while the good one is still on disk.
        from spatialdata.models import TableModel
        try:
            TableModel.parse(table)
        except Exception as e:
            raise ValueError(f"table fails validation, nothing written: {e}")
        attrs = dict(table.uns.get("spatialdata_attrs", {}))
        for k in ("region_key", "instance_key"):
            col = attrs.get(k)
            if col and col not in table.obs.columns:
                raise ValueError(f"{k} '{col}' missing from obs, nothing written")

        name = list(sdata.tables)[0]
        sdata.delete_element_from_disk(name)
        sdata.write_element(name)
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
        bits = ", ".join(
            (f"+{v[0]:,} filled via FOV" if k == "_filled_by_fov"
             else f"{k} {v[0]:,}/{r['n_obs']:,} cells, {v[1]} values")
            for k, v in r["added"].items())
        print(f"  {sample:24s} {bits}")
        ok += 1

    print(f"\n{ok} object(s) updated, {fail} failed"
          + ("   [dry-run — nothing written]" if args.dry_run else ""))
    if fail:
        sys.exit(1)


if __name__ == "__main__":
    main()
