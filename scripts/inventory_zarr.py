#!/usr/bin/env python3
"""
inventory_zarr.py
Scan a directory of SpatialData zarr stores and emit one row per object.

Feeds the Zenodo file_manifest.csv and cross-checks the built objects against
samples.parquet.

Usage:
    python scripts/inventory_zarr.py --zarr-dir ~/SAHA_zarr_build \
        --manifest data/samples.parquet --out zenodo/file_manifest.csv
"""

import argparse
import warnings
from pathlib import Path

import pandas as pd

warnings.filterwarnings("ignore")


def dir_bytes(path: Path) -> int:
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())


def scan(zarr_path: Path) -> dict:
    import spatialdata as sd

    sdata = sd.read_zarr(zarr_path)
    table = sdata.tables["table"]
    obs = table.obs

    row = {
        "file": zarr_path.name,
        "sample_id": zarr_path.stem,
        "n_cells": table.n_obs,
        "n_features": table.n_vars,
        "n_transcripts": (len(sdata.points["transcripts"])
                          if "transcripts" in sdata.points else 0),
        "n_shapes": sum(len(v) for v in sdata.shapes.values()),
        "has_images": bool(sdata.images),
        "qc_pass_cells": int(obs["qc_pass"].sum()) if "qc_pass" in obs else None,
        "has_cell_type": "cell_type" in obs.columns,
        "n_obs_columns": obs.shape[1],
        "bytes": dir_bytes(zarr_path),
    }
    row["size_gb"] = round(row["bytes"] / 1e9, 3)
    return row


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--zarr-dir", type=Path, required=True, action="append",
                   help="directory of .zarr stores (repeatable)")
    p.add_argument("--manifest", type=Path, help="samples.parquet to cross-check")
    p.add_argument("--out", type=Path, help="write CSV here")
    args = p.parse_args()

    stores = sorted(z for d in args.zarr_dir for z in d.glob("*.zarr"))
    if not stores:
        raise SystemExit(f"no .zarr stores under {args.zarr_dir}")

    rows = []
    for z in stores:
        try:
            rows.append(scan(z))
            print(f"  scanned {z.name}")
        except Exception as e:
            print(f"  FAILED {z.name}: {type(e).__name__}: {e}")
    df = pd.DataFrame(rows)

    if args.manifest and args.manifest.exists():
        man = pd.read_parquet(args.manifest)[
            ["sample_id", "platform", "assay_type", "organ", "condition",
             "donor_id", "n_cells"]
        ].rename(columns={"n_cells": "manifest_n_cells"})
        df = df.merge(man, on="sample_id", how="left")
        # qc_pass is our reconstruction of the published (QC-filtered) cell set,
        # so it is what should line up with the manifest — not the raw cell count.
        df["matches_manifest"] = (
            df["qc_pass_cells"].notna()
            & df["manifest_n_cells"].notna()
            & ((df["qc_pass_cells"] - df["manifest_n_cells"]).abs() <= 100)
        )

    cols = [c for c in ["file", "sample_id", "platform", "assay_type", "organ",
                        "condition", "donor_id", "n_cells", "qc_pass_cells",
                        "manifest_n_cells", "matches_manifest", "n_features",
                        "n_transcripts", "n_shapes", "has_images",
                        "has_cell_type", "size_gb", "bytes"] if c in df.columns]
    df = df[cols].sort_values("sample_id")

    print(f"\n{df.drop(columns=['bytes', 'file']).to_string(index=False)}")
    print(f"\ntotal: {len(df)} objects, {df['size_gb'].sum():.1f} GB, "
          f"{df['n_cells'].sum():,} cells, {df['n_transcripts'].sum():,} transcripts")
    if "matches_manifest" in df.columns:
        bad = df[~df["matches_manifest"] & df["manifest_n_cells"].notna()]
        if len(bad):
            print(f"\n{len(bad)} object(s) disagree with the manifest:")
            print(bad[["sample_id", "qc_pass_cells", "manifest_n_cells"]]
                  .to_string(index=False))

    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(args.out, index=False)
        print(f"\nwrote {args.out}")


if __name__ == "__main__":
    main()
