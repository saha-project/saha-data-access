#!/usr/bin/env python3
"""
patch_donor_core.py
Add per-cell donor identity (`donor_core`) to built CosMx SpatialData objects.

These slides are tissue microarrays. One acquisition covers several donor
cores, so donor is a property of a CELL, not of a sample -- a single object
such as SAHA_CR_COL_A1_EC02 spans all seven COL_A1 cores. A scalar donor_id
per sample cannot represent that and is wrong wherever it appears.

The published organ-level h5ads already carry this as `SAHA_Sample`
(`<SLIDE>_<CORE>`, e.g. COL_A1_5), which joins directly to `SampleID` in the
cohort table of the manuscript. This copies it onto the objects, matching cells
the same way qc_pass does -- on the `cell` key, after subsetting the h5ad to
the sample.

Coverage is partial and that is a property of the sources, not of this script:
the protein h5ads carry no core column, and neither do the Xenium objects.
Samples with no available annotation are reported and left alone rather than
given a fabricated value.

Usage:
    python scripts/patch_donor_core.py --zarr-dir ~/SAHA_zarr_build \
        --h5ad-dir "…/h5ad/RNA" --extra-h5ad "…/Analysis/IBD_integration/adata.joint.h5ad" \
        --dry-run

    python scripts/patch_donor_core.py --zarr-dir ~/SAHA_zarr_build \
        --h5ad-dir "…/h5ad/RNA" --extra-h5ad "…/adata.joint.h5ad" --write-csv out/
"""

import argparse
import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

CORE_COL = "SAHA_Sample"          # <SLIDE>_<CORE>, joins to cohort-table SampleID
OUT_COL = "donor_core"


def cell_key(df, what="obs"):
    """CosMx cell_ID restarts per FOV; `cell` (c_slide_fov_cell) is slide-unique."""
    if "cell" in df.columns:
        return df["cell"].astype(str)
    if {"fov", "cell_ID"} <= set(df.columns):
        return "c_1_" + df["fov"].astype(str) + "_" + df["cell_ID"].astype(str)
    raise KeyError(f"{what}: need 'cell', or both 'fov' and 'cell_ID'")


def organ_code(sample_id: str) -> str:
    parts = sample_id.split("_")
    return parts[2] if len(parts) > 2 else ""


def candidate_h5ads(sample_id: str, h5ad_dir: Path, extra: list) -> list:
    """Organ-level h5ad first, then any explicitly supplied files (IBD etc.)."""
    out = []
    if h5ad_dir:
        code = organ_code(sample_id)
        for name in (f"SAHA_{code}_RNA.h5ad", f"SAHA_{code}_PRT.h5ad"):
            p = h5ad_dir / name
            if p.exists():
                out.append(p)
    return out + [p for p in extra if p.exists()]


def core_map(h5ad_path: Path, sample_id: str):
    """Return a Series mapping cell key -> donor core, or None."""
    import anndata as ad

    a = ad.read_h5ad(h5ad_path, backed="r")
    obs = a.obs
    if CORE_COL not in obs.columns:
        a.file.close()
        return None
    for col in ("SAHA_name", "sample_id"):
        if col in obs.columns:
            obs = obs[obs[col].astype(str) == sample_id]
            break
    obs = obs.copy()
    a.file.close()
    if not len(obs):
        return None
    try:
        obs.index = cell_key(obs, h5ad_path.name)
    except KeyError:
        return None
    s = obs[CORE_COL].astype(str)
    s = s[~s.isin(["nan", ""])]
    return s[~s.index.duplicated()]


def patch(zarr_path: Path, h5ad_dir: Path, extra: list, dry: bool, csv_dir: Path):
    import spatialdata as sd

    sample_id = zarr_path.stem
    sdata = sd.read_zarr(zarr_path)
    table = sdata.tables["table"]

    for h in candidate_h5ads(sample_id, h5ad_dir, extra):
        m = core_map(h, sample_id)
        if m is None:
            continue
        mapped = table.obs.index.map(m)
        hit = mapped.notna().mean()
        if hit < 0.5:
            continue

        counts = mapped.value_counts().to_dict()
        print(f"  {sample_id:24s} {hit:6.1%} of {table.n_obs:>8,} cells | "
              f"{len(counts)} cores from {h.name}")
        for k, v in sorted(counts.items()):
            print(f"      {k:16s} {v:>8,}")

        if csv_dir:
            csv_dir.mkdir(parents=True, exist_ok=True)
            import pandas as pd
            pd.DataFrame({"cell": table.obs.index, OUT_COL: mapped}).to_csv(
                csv_dir / f"{sample_id}.donor_core.csv", index=False)

        if not dry:
            table.obs[OUT_COL] = mapped.astype("object")
            sdata.delete_element_from_disk("table")
            sdata.write_element("table")
        return True, len(counts)

    print(f"  {sample_id:24s} no core annotation available")
    return False, 0


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--zarr-dir", type=Path, required=True)
    ap.add_argument("--h5ad-dir", type=Path)
    ap.add_argument("--extra-h5ad", type=Path, action="append", default=[])
    ap.add_argument("--only", nargs="*")
    ap.add_argument("--write-csv", type=Path,
                    help="also write per-sample cell->core CSVs (for patching "
                         "a copy of the objects on another machine)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    stores = sorted(args.zarr_dir.glob("*.zarr"))
    if args.only:
        stores = [z for z in stores if z.stem in args.only]
    if not stores:
        sys.exit(f"ERROR: no .zarr stores in {args.zarr_dir}")

    print(f"{len(stores)} object(s)\n")
    ok = 0
    missing = []
    for z in stores:
        try:
            done, _ = patch(z, args.h5ad_dir, args.extra_h5ad,
                            args.dry_run, args.write_csv)
        except Exception as e:
            print(f"  {z.stem:24s} FAILED {type(e).__name__}: {e}")
            done = False
        ok += bool(done)
        if not done:
            missing.append(z.stem)

    print(f"\n{ok} of {len(stores)} annotated")
    if missing:
        print(f"{len(missing)} without a core annotation in any source:")
        for m in missing:
            print(f"  {m}")
    if args.dry_run:
        print("\n[dry-run] nothing written")


if __name__ == "__main__":
    main()
