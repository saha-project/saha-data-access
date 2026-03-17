#!/usr/bin/env python3
"""
backfill_s3_paths.py
Populate s3_raw_prefix and s3_processed_path in sample_metadata.csv
based on the agreed S3 layout for s3://saha-open-data.

S3 layout assumptions (must match upload_to_s3.py):
  raw/cosmx/{sample_id}/           — CosMx flat files (Wasabi=Y samples)
  processed/h5ad/cosmx/RNA/        — standardized organ-level CosMx RNA h5ads
  processed/h5ad/cosmx/PRT/        — standardized organ-level CosMx PRT h5ads
  processed/h5ad/xenium/           — standardized Xenium sample-level h5ads
  processed/zarr/xenium/           — Xenium SpatialData zarr

Run after completing S3 uploads (Phases 3.2–3.4).

Usage:
    python scripts/backfill_s3_paths.py           # updates sample_metadata.csv
    python scripts/backfill_s3_paths.py --dry-run # print without writing
"""

import argparse
from pathlib import Path
import pandas as pd

BUCKET    = "saha-open-data"
MANIFEST  = Path(__file__).parent.parent / "sample_metadata.csv"

# ---------------------------------------------------------------------------
# Organ code → h5ad filename mapping (CosMx organ-level files)
# ---------------------------------------------------------------------------

# Extract organ code from sample_id token (index 2 in SAHA_{assay}_{organ}_{donor}_{batch})
ORGAN_TO_H5AD_RNA = {
    "APE":  "SAHA_APE_RNA.h5ad",
    "COL":  "SAHA_COL_RNA.h5ad",
    "ILE":  "SAHA_ILE_RNA.h5ad",
    "LN":   "SAHA_LN_RNA.h5ad",
    "PANC": "SAHA_PANC_RNA.h5ad",
    "PROS": "SAHA_PROS_RNA.h5ad",
    "STO":  "SAHA_STO_RNA.h5ad",
    "BM":   "SAHA_BM_RNA.h5ad",
    "LIV":  "SAHA_LIV_RNA_A1.h5ad",   # default; A2 handled below
}

ORGAN_TO_H5AD_PRT = {
    "APE":  "SAHA_APE_PRT.h5ad",
    "COL":  "SAHA_COL_PRT.h5ad",
    "ILE":  "SAHA_ILE_PRT.h5ad",
    "LN":   "SAHA_LN_PRT.h5ad",
    "PANC": "SAHA_PANC_PRT.h5ad",
    "PROS": "SAHA_PROS_PRT.h5ad",
    "STO":  "SAHA_STO_PRT.h5ad",
    "BM":   "SAHA_BM_PRT.h5ad",
    "LIV":  "SAHA_LIV_PRT.h5ad",
}


def organ_code(sample_id: str) -> str:
    """Extract organ token from sample_id (e.g. SAHA_CR_COL_A1_EC02 → COL)."""
    parts = sample_id.split("_")
    return parts[2] if len(parts) >= 3 else ""


def donor_code(sample_id: str) -> str:
    """Extract donor token (e.g. SAHA_CR_LIV_A2_TAP01 → A2)."""
    parts = sample_id.split("_")
    return parts[3] if len(parts) >= 4 else ""


def s3_processed_path(row: pd.Series) -> str:
    sid    = row["Folder Name"].strip()
    assay  = row["Assay"].strip().lower()
    organ  = organ_code(sid)
    donor  = donor_code(sid)

    if assay == "xenium":
        # sample-level h5ad
        h5ad = f"{sid}_RNA.h5ad"
        return f"s3://{BUCKET}/processed/h5ad/xenium/{h5ad}"

    elif "rna" in assay or assay == "cosmx":
        # Use PDAC-specific note if PDAC (no organ-level h5ad yet)
        if organ == "PDAC":
            return ""   # not yet processed into organ h5ad
        if organ == "IBD":
            return ""   # not yet processed
        h5ad = ORGAN_TO_H5AD_RNA.get(organ, "")
        if not h5ad:
            return ""
        # LIV A2 has its own h5ad
        if organ == "LIV" and donor == "A2":
            h5ad = "SAHA_LIV_RNA_A2.h5ad"
        return f"s3://{BUCKET}/processed/h5ad/cosmx/RNA/{h5ad}"

    elif "protein" in assay or "prt" in assay:
        if organ in ("PDAC", "IBD"):
            return ""
        h5ad = ORGAN_TO_H5AD_PRT.get(organ, "")
        return f"s3://{BUCKET}/processed/h5ad/cosmx/PRT/{h5ad}" if h5ad else ""

    return ""


def s3_raw_prefix(row: pd.Series) -> str:
    sid   = row["Folder Name"].strip()
    assay = row["Assay"].strip().lower()
    if str(row["Wasabi"]).strip().upper() != "Y":
        return ""
    platform = "xenium" if "xenium" in assay else "cosmx"
    return f"s3://{BUCKET}/raw/{platform}/{sid}/"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


def main():
    args = parse_args()

    df = pd.read_csv(MANIFEST, dtype=str, keep_default_na=False)

    changed = 0
    for idx, row in df.iterrows():
        if str(row.get("release_status", "")).strip().lower().startswith("excluded"):
            continue
        folder = str(row["Folder Name"]).strip()
        if not folder:
            continue

        raw  = s3_raw_prefix(row)
        proc = s3_processed_path(row)

        if args.dry_run:
            if raw or proc:
                print(f"  {folder}")
                if raw:
                    print(f"    s3_raw_prefix      = {raw}")
                if proc:
                    print(f"    s3_processed_path  = {proc}")
        else:
            df.at[idx, "s3_raw_prefix"]     = raw
            df.at[idx, "s3_processed_path"] = proc
            if raw or proc:
                changed += 1

    if not args.dry_run:
        df.to_csv(MANIFEST, index=False)
        print(f"Updated {changed} rows in {MANIFEST}")


if __name__ == "__main__":
    main()
