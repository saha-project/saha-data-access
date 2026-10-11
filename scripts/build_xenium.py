#!/usr/bin/env python3
"""
build_xenium.py
Build a SpatialData object from a standard Xenium output bundle.

The v1 Xenium objects were produced on the workstation before this repo existed;
this covers the acquisitions whose raw bundles sit on the cluster instead --
starting with SAHA_XR_PRCA_UA_MS01, the prostate-cancer run that was uploaded,
recorded in the tracker, and then never reached the manifest.

spatialdata_io.xenium() does the reading. What this adds is the release
vocabulary, so a Xenium object joins the rest of the atlas identically:

    sample_id / platform / modality / assay_type
    organ / condition
    donor_core / donor_patient   -- per cell, as everywhere else in SAHA

Donor is written per cell even when an acquisition covers a single specimen.
A scalar per-sample donor column is what collapsed whole tissue microarrays onto
one fictional person, and the join has to work the same way for every object.

Usage:
    P=~/.venv-sd08/bin/python
    $P scripts/build_xenium.py --raw-dir /athena/project-saha/data/SAHA_XR_PRCA_UA_MS01 \
        --sample SAHA_XR_PRCA_UA_MS01 --donor-core PRCA_UA \
        --organ prostate --condition cancer \
        --out-dir /athena/project-saha/derived/build/zenodo_tier/<record_slug> [--dry-run]
"""

import argparse
import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--raw-dir", type=Path, required=True)
    ap.add_argument("--sample", required=True)
    ap.add_argument("--donor-core", required=True)
    ap.add_argument("--donor-patient")
    ap.add_argument("--organ", required=True)
    ap.add_argument("--condition", required=True, choices=["normal", "cancer", "disease"])
    ap.add_argument("--out-dir", type=Path, required=True)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    import numpy as np
    import pandas as pd
    import spatialdata_io as sdio

    if not (args.raw_dir / "experiment.xenium").exists():
        sys.exit(f"ERROR: no experiment.xenium in {args.raw_dir} -- not a Xenium bundle")

    sdata = sdio.xenium(str(args.raw_dir))
    table = list(sdata.tables.values())[0]

    table.obs["sample_id"] = args.sample
    table.obs["platform"] = "xenium"
    table.obs["assay_type"] = "rna"
    table.obs["modality"] = "rna"
    table.obs["organ"] = args.organ
    table.obs["condition"] = args.condition
    table.obs["donor_core"] = pd.Categorical([args.donor_core] * table.n_obs)
    if args.donor_patient:
        table.obs["donor_patient"] = pd.Categorical([args.donor_patient] * table.n_obs)

    # SpatialData compares obs names case-insensitively and refuses a table that
    # carries two spellings of one name; the Xenium reader and our additions can
    # collide (cell_id / cell_ID).
    seen, drop = {}, []
    for c in table.obs.columns:
        k = str(c).lower()
        if k in seen:
            drop.append(c)
        else:
            seen[k] = c
    if drop:
        print(f"  dropping case-variant duplicate obs column(s): {drop}")
        table.obs = table.obs.drop(columns=drop)

    imgs = sorted(sdata.images.keys())
    shapes = sorted(sdata.shapes.keys())
    pts = sorted(sdata.points.keys())
    print(f"  {args.sample}: {table.n_obs:,} cells x {table.n_vars:,} features")
    print(f"    images {imgs}")
    print(f"    shapes {shapes}")
    print(f"    points {pts}")
    print(f"    donor_core={args.donor_core}"
          + (f" donor_patient={args.donor_patient}" if args.donor_patient else ""))

    if not shapes:
        sys.exit("ERROR: no cell boundaries read -- refusing to write a shapeless object")

    if args.dry_run:
        print("  [dry-run] nothing written")
        return

    out = args.out_dir / f"{args.sample}.zarr"
    out.parent.mkdir(parents=True, exist_ok=True)
    sdata.write(out, overwrite=True)
    print(f"  wrote {out}")


if __name__ == "__main__":
    main()
