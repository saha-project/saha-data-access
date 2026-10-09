#!/usr/bin/env python3
"""
build_v2_cosmx.py
Build the v2 release copy of a merged CosMx slide object.

Three things happen, in this order, and the order matters:

1. Core identity is written as `donor_core`, v1's convention, NOT as `Core_ID`.
   The public obs whitelist keeps `donor_core` and does not know about
   `Core_ID`, so mapping first is what stops step 2 deleting the cores. The
   value is `<slide>_<n>`, e.g. PROS_A2_17, matching v1's `APE_A9_1` style.
     - the RNA table already carries Core_ID from AtoMx
     - the protein table takes the sidecar produced by transfer_cores_v2.py
       (A2 only; A3 was never annotated at source, so it stays null)

2. The public obs whitelist is applied per table, reusing v1's own keep sets
   via apply_obs_whitelist.keep_set() rather than a second copy of the rules.
   The table decides which base set applies: `rna` -> RNA_KEEP, `protein` ->
   PRT_KEEP. Dropped columns are written out first so the removal is reversible.

3. The object is written to a NEW path and read back. v1 established that a
   SpatialData write can report success and still lose elements.

Also reports the protein panel's variable names, since 69 variables does not
match v1's `IO Protein (64-plex)` and the panel is unidentified.

Usage:
    PY=/home/fs01/jip2007/miniconda3/envs/saha/bin/python
    $PY scripts/build_v2_cosmx.py --object .../export/SAHA_CosMx_PROS_A2.zarr \\
        --slide PROS_A2 --cores-protein .../v2_cores_A2_protein.parquet \\
        --out .../derived/v2_objects/SAHA_CosMx_PROS_A2.zarr \\
        --report-dir .../v2_obs_dropped [--dry-run]
"""

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parent
sys.path.insert(0, str(REPO))


def as_core(values, index):
    """donor_core as a categorical, so 'no core' survives the write as missing.

    An object-dtype column holding None does NOT round-trip through zarr: the
    Nones come back as strings, so `notna()` then counts every row. Measured on
    the first attempt at these objects:

        A2 protein  donor_core non-null 221,028 -> 221,374 after write
        A3 protein  donor_core non-null       0 -> 289,773 after write

    A3 is the one that matters -- a slide with NO core annotation would have
    shipped claiming a core for all 289,773 cells, and any groupby would have
    treated it as real. Categorical stores missing as code -1 and AnnData
    round-trips that faithfully.
    """
    return pd.Categorical(pd.Series(values, index=index, dtype="object"))


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--object", type=Path, required=True)
    ap.add_argument("--slide", required=True, help="e.g. PROS_A2; prefixes donor_core")
    ap.add_argument("--cores-protein", type=Path,
                    help="sidecar from transfer_cores_v2.py for the protein table")
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--report-dir", type=Path, required=True)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    import warnings
    warnings.filterwarnings("ignore")
    from spatialdata import read_zarr
    from apply_obs_whitelist import keep_set

    sd = read_zarr(args.object)
    args.report_dir.mkdir(parents=True, exist_ok=True)
    print(f"=== {args.object.name}  tables={list(sd.tables)}")

    summary = {}
    for tname, t in sd.tables.items():
        n0 = t.obs.shape[1]

        # ---- 1. core identity -> donor_core -------------------------------
        src = None
        if tname == "protein" and args.cores_protein and args.cores_protein.exists():
            sc = pd.read_parquet(args.cores_protein)
            if len(sc) != t.n_obs:
                sys.exit(f"ERROR: sidecar has {len(sc)} rows, protein table has "
                         f"{t.n_obs} -- refusing to attach misaligned labels")
            core = sc["core"].to_numpy()
            ok = sc["assigned"].to_numpy().astype(bool)
            vals = [f"{args.slide}_{c}" if (o and pd.notna(c)) else np.nan
                    for o, c in zip(ok, core)]
            t.obs["donor_core"] = as_core(vals, t.obs.index)
            src = f"transferred from RNA ({ok.sum():,}/{len(ok):,} cells)"
        elif "Core_ID" in t.obs.columns and t.obs["Core_ID"].notna().any():
            cid = t.obs["Core_ID"]
            t.obs["donor_core"] = as_core(
                [f"{args.slide}_{c}" if pd.notna(c) else np.nan for c in cid],
                t.obs.index)
            src = "AtoMx Core_ID"
        else:
            t.obs["donor_core"] = as_core([np.nan] * t.n_obs, t.obs.index)
            src = "NOT AVAILABLE -- never annotated at source"

        ncore = int(t.obs["donor_core"].notna().sum())
        ndist = int(t.obs["donor_core"].dropna().nunique())
        print(f"  [{tname}] donor_core: {src}; {ncore:,}/{t.n_obs:,} cells, "
              f"{ndist} distinct")

        # ---- 2. public obs whitelist --------------------------------------
        # Pretend a sample id so v1's keep_set picks the right base set.
        fake = f"SAHA_{'CP' if tname == 'protein' else 'CR'}_{args.slide}"
        keep = keep_set(fake, set(t.obs.columns))

        # The table's own join keys must survive, whatever they are called.
        # v1's STRUCTURAL list names the keys v1's objects happened to use; these
        # objects were parsed with instance_key="cell_index", which is not in it,
        # and dropping it makes SpatialData.write() die with KeyError at the very
        # end -- after the whole object has been serialised. Take the names from
        # the table instead of guessing.
        attrs = t.uns.get("spatialdata_attrs", {})
        required = {attrs.get("instance_key"), attrs.get("region_key")} - {None}
        missing = required - set(t.obs.columns)
        if missing:
            sys.exit(f"ERROR: {tname} declares join key(s) {sorted(missing)} that are "
                     f"not in obs -- the object is inconsistent, not safe to rewrite")
        keep |= required

        drop = [c for c in t.obs.columns if c not in keep]
        if "donor_core" in drop:
            sys.exit("ERROR: the whitelist would drop donor_core -- aborting")
        rep = args.report_dir / f"obs_dropped_{args.object.stem}_{tname}.csv.gz"
        if not args.dry_run and drop:
            t.obs[drop].to_csv(rep, index=True, compression="gzip")
        t.obs = t.obs[[c for c in t.obs.columns if c in keep]]
        print(f"        obs {n0} -> {t.obs.shape[1]} ({len(drop)} dropped"
              + (f", backed up to {rep.name})" if drop and not args.dry_run else ")"))
        summary[tname] = (ncore, ndist, n0, t.obs.shape[1])

        if tname == "protein":
            v = list(t.var_names)
            print(f"        protein panel: {len(v)} variables")
            print(f"          {sorted(v)}")

    if args.dry_run:
        print("\n[dry-run] nothing written")
        return 0

    args.out.parent.mkdir(parents=True, exist_ok=True)
    print(f"\n  writing {args.out}")
    sd.write(args.out)

    back = read_zarr(args.out)
    assert set(back.tables) == set(sd.tables), "tables changed across write"
    assert set(back.shapes) == set(sd.shapes), "shapes changed across write"
    assert len(back.images) == len(sd.images), "image count changed across write"
    for tn, (ncore, ndist, _, ncols) in summary.items():
        bt = back.tables[tn]
        assert bt.n_obs == sd.tables[tn].n_obs, f"{tn}: cell count changed"
        assert "donor_core" in bt.obs.columns, f"{tn}: donor_core lost"
        got = int(bt.obs["donor_core"].notna().sum())
        assert got == ncore, f"{tn}: donor_core {ncore} -> {got} across write"
        assert bt.obs.shape[1] == ncols, f"{tn}: obs width changed"
    print(f"  verified: {len(back.tables)} tables, {len(back.images)} images, "
          f"{len(back.shapes)} shapes; donor_core preserved "
          + ", ".join(f"{k}={v[0]:,}" for k, v in summary.items()))
    try:
        print(f"  is_self_contained(): {back.is_self_contained()}")
    except Exception:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
