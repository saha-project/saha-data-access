#!/usr/bin/env python3
"""
upload_to_s3.py
Upload SAHA processed data and metadata to s3://saha-open-data.

S3 layout
---------
s3://saha-open-data/
  metadata/
    samples.parquet
    donors.parquet
    runs.parquet
    panels.parquet
  processed/
    h5ad/
      cosmx/RNA/   SAHA_*_RNA.h5ad   (organ-level, standardized)
      cosmx/PRT/   SAHA_*_PRT.h5ad
      xenium/      SAHA_XR_*_RNA.h5ad  (sample-level, standardized)
    zarr/
      xenium/      SAHA_XR_*_RNA.zarr/
      cosmx/       SAHA_CR_*_RNA.zarr/ SAHA_CP_*_PRT.zarr/  (Phase 2.7, post raw upload)
  docs/
    README.md  DATA_DICTIONARY.md  QUICK_START.md  ...

Usage:
    # Dry run (lists what would be uploaded, no AWS calls)
    python scripts/upload_to_s3.py --dry-run

    # Upload everything
    python scripts/upload_to_s3.py

    # Upload only metadata Parquet
    python scripts/upload_to_s3.py --only metadata

    # Upload only h5ad files
    python scripts/upload_to_s3.py --only h5ad

    # Upload only Xenium zarr objects
    python scripts/upload_to_s3.py --only zarr

Requirements:
    pip install boto3
    AWS credentials with s3:PutObject on saha-open-data
"""

import argparse
import os
import sys
from pathlib import Path

BUCKET  = "saha-open-data"
REGION  = "us-east-1"

# ---------------------------------------------------------------------------
# Path configuration — adjust if local paths differ
# ---------------------------------------------------------------------------

REPO_ROOT    = Path(__file__).parent.parent
DATA_DIR     = REPO_ROOT / "data"
DOCS_DIR     = REPO_ROOT / "docs"

# Standardized public h5ads (output of scripts/standardize_h5ad.py)
H5AD_PUB_RNA    = DATA_DIR / "h5ad_public" / "RNA"
H5AD_PUB_PRT    = DATA_DIR / "h5ad_public" / "PRT"
H5AD_PUB_XENIUM = DATA_DIR / "h5ad_public" / "xenium"

# Xenium zarr objects (already exist)
ZARR_XENIUM = Path("/Users/jiwoonpark/Dropbox (Personal)/2025- MasonLab/"
                   "2025_SAHA/Data/preprocessed/spatialdata/RNA")

# CosMx zarr objects (created by scripts/create_cosmx_zarr.py)
ZARR_COSMX  = DATA_DIR / "zarr" / "cosmx"

EXCLUDE_NAMES = {
    ".DS_Store", "Thumbs.db",
}
EXCLUDE_SUFFIXES = {".tmp", ".pyc", ".swp"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def should_exclude(path: Path) -> bool:
    return (path.name in EXCLUDE_NAMES or
            path.suffix in EXCLUDE_SUFFIXES or
            path.name.startswith("._") or
            path.name.startswith("temp_"))


def s3_key(prefix: str, relative: Path) -> str:
    return f"{prefix}/{relative}".replace("\\", "/")


def upload_file(s3_client, local_path: Path, bucket: str, key: str,
                dry_run: bool) -> None:
    size_mb = local_path.stat().st_size / 1_048_576
    if dry_run:
        print(f"  [dry-run] s3://{bucket}/{key}  ({size_mb:.1f} MB)")
        return
    print(f"  → s3://{bucket}/{key}  ({size_mb:.1f} MB)")
    s3_client.upload_file(str(local_path), bucket, key)


def upload_dir(s3_client, local_dir: Path, s3_prefix: str,
               bucket: str, dry_run: bool, recursive: bool = True) -> int:
    """Upload all files in local_dir to s3_prefix. Returns file count."""
    count = 0
    if not local_dir.exists():
        print(f"  SKIP (not found): {local_dir}")
        return 0

    iterator = local_dir.rglob("*") if recursive else local_dir.iterdir()
    for p in sorted(iterator):
        if not p.is_file() or should_exclude(p):
            continue
        rel = p.relative_to(local_dir)
        key = s3_key(s3_prefix, rel)
        upload_file(s3_client, p, bucket, key, dry_run)
        count += 1
    return count


# ---------------------------------------------------------------------------
# Upload groups
# ---------------------------------------------------------------------------

def upload_metadata(s3, bucket: str, dry_run: bool) -> None:
    print("\n=== Metadata Parquet ===")
    for name in ("samples.parquet", "donors.parquet",
                 "runs.parquet", "panels.parquet"):
        p = DATA_DIR / name
        if not p.exists():
            print(f"  MISSING: {p} — run ingest scripts first")
            continue
        upload_file(s3, p, bucket, f"metadata/{name}", dry_run)


def upload_h5ad(s3, bucket: str, dry_run: bool) -> None:
    print("\n=== Processed h5ad files ===")
    for local, prefix in [
        (H5AD_PUB_RNA,    "processed/h5ad/cosmx/RNA"),
        (H5AD_PUB_PRT,    "processed/h5ad/cosmx/PRT"),
        (H5AD_PUB_XENIUM, "processed/h5ad/xenium"),
    ]:
        n = upload_dir(s3, local, prefix, bucket, dry_run)
        if n == 0 and not local.exists():
            print(f"  NOTE: {local} not found — run standardize_h5ad.py --batch first")


def upload_zarr(s3, bucket: str, dry_run: bool) -> None:
    print("\n=== SpatialData zarr objects ===")
    # Xenium zarr (already exists)
    n = upload_dir(s3, ZARR_XENIUM, "processed/zarr/xenium", bucket, dry_run)
    if n == 0:
        print(f"  NOTE: No Xenium zarr found at {ZARR_XENIUM}")
    # CosMx zarr (created by create_cosmx_zarr.py after Phase 3 raw upload)
    if ZARR_COSMX.exists():
        upload_dir(s3, ZARR_COSMX, "processed/zarr/cosmx", bucket, dry_run)
    else:
        print(f"  NOTE: CosMx zarr not yet created (run create_cosmx_zarr.py after raw upload)")


def upload_docs(s3, bucket: str, dry_run: bool) -> None:
    print("\n=== Documentation ===")
    upload_dir(s3, DOCS_DIR, "docs", bucket, dry_run, recursive=False)
    # Also upload top-level readme
    readme = REPO_ROOT / "readme.md"
    if readme.exists():
        upload_file(s3, readme, bucket, "README.md", dry_run)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--bucket",   default=BUCKET)
    p.add_argument("--region",   default=REGION)
    p.add_argument("--only",     choices=["metadata", "h5ad", "zarr", "docs"],
                   help="Upload only one category")
    p.add_argument("--dry-run",  action="store_true",
                   help="List uploads without executing")
    return p.parse_args()


def main():
    args = parse_args()

    if args.dry_run:
        s3 = None
        print(f"DRY RUN — would upload to s3://{args.bucket}/\n")
    else:
        try:
            import boto3
            session = boto3.session.Session(region_name=args.region)
            s3 = session.client("s3")
        except ImportError:
            sys.exit("ERROR: boto3 not installed. pip install boto3")

    groups = {
        "metadata": lambda: upload_metadata(s3, args.bucket, args.dry_run),
        "h5ad":     lambda: upload_h5ad(s3, args.bucket, args.dry_run),
        "zarr":     lambda: upload_zarr(s3, args.bucket, args.dry_run),
        "docs":     lambda: upload_docs(s3, args.bucket, args.dry_run),
    }

    if args.only:
        groups[args.only]()
    else:
        for fn in groups.values():
            fn()

    print("\nDone." if not args.dry_run else "\n[dry-run complete]")


if __name__ == "__main__":
    main()
