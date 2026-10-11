#!/usr/bin/env python3
"""
apply_donor_core_xenium.py
Write donor identity into the Xenium SpatialData objects.

Xenium acquisitions are already split by donor: the trailing number of the
sample id is the core on the slide, so SAHA_XR_APE_A9_MS01_03 images core
APE_A9_3. Donor is therefore a per-SAMPLE property here, unlike CosMx where one
acquisition spans a whole TMA and donor is per-cell. The same `donor_core`
column is written either way so the two platforms join identically.

24 of the 25 v1 Xenium samples carry a trailing number. SAHA_XR_PANC_A1_MS01
does not, and at 701,037 cells against 22k-88k for its siblings it is plainly
not a single core -- it is left unassigned rather than guessed at.

Usage:
    python scripts/apply_donor_core_xenium.py \
        --zarr-dir ".../preprocessed/spatialdata/RNA" --dry-run
"""

import argparse
import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")


def core_of(sample_id: str):
    """SAHA_XR_APE_A9_MS01_03 -> APE_A9_3, or None when there is no core number.
    One rule for all callers: donor_map.xenium_core (suffix rule + XENIUM_CORE_OVERRIDE)."""
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from donor_map import xenium_core
    return xenium_core(sample_id)


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--zarr-dir", type=Path, required=True)
    ap.add_argument("--only", nargs="*")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    import pandas as pd
    import spatialdata as sd

    stores = sorted(args.zarr_dir.glob("*.zarr"))
    if args.only:
        stores = [z for z in stores if z.stem in args.only]
    if not stores:
        sys.exit(f"ERROR: no .zarr stores in {args.zarr_dir}")

    print(f"{len(stores)} Xenium object(s)\n")
    done, skipped = 0, []
    for z in stores:
        core = core_of(z.stem)
        if core is None:
            skipped.append(z.stem)
            print(f"  {z.stem:34s} no core number in the id — left unassigned")
            continue
        try:
            sdata = sd.read_zarr(z)
            table = sdata.tables["table"]
            table.obs["donor_core"] = pd.Categorical([core] * table.n_obs)
            if not args.dry_run:
                sdata.delete_element_from_disk("table")
                sdata.write_element("table")
            print(f"  {z.stem:34s} {table.n_obs:>8,} cells -> {core}")
            done += 1
        except Exception as e:
            print(f"  {z.stem:34s} FAILED {type(e).__name__}: {e}")

    print(f"\n{done} object(s) assigned"
          + (f", {len(skipped)} left unassigned: {', '.join(skipped)}" if skipped else "")
          + ("   [dry-run — nothing written]" if args.dry_run else ""))


if __name__ == "__main__":
    main()
