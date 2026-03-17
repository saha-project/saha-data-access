#!/usr/bin/env python3
"""
wasabi_to_s3.py
Migrate CosMx raw flat files from Wasabi → s3://saha-open-data/raw/cosmx/.

For each sample with Wasabi=Y in sample_metadata.csv, this script syncs
the raw CosMx output directory from the Wasabi bucket to the S3 bucket,
placing each sample under:
    s3://saha-open-data/raw/cosmx/{sample_id}/

After a successful sync it can backfill s3_raw_prefix in sample_metadata.csv
(use --backfill).

Usage:
    # Dry run (show what would be synced)
    python scripts/wasabi_to_s3.py \
        --wasabi-bucket  <wasabi-bucket-name> \
        --wasabi-prefix  cosmx_raw \
        --dry-run

    # Full sync
    python scripts/wasabi_to_s3.py \
        --wasabi-bucket  <wasabi-bucket-name> \
        --wasabi-prefix  cosmx_raw \
        --backfill

    # Sync a single sample
    python scripts/wasabi_to_s3.py \
        --sample SAHA_CR_COL_A1_EC02 \
        --wasabi-bucket <wasabi-bucket-name> \
        --wasabi-prefix cosmx_raw

Requirements:
    pip install boto3
    AWS credentials for both Wasabi (source) and AWS S3 (destination).
    Wasabi credentials: set WASABI_ACCESS_KEY / WASABI_SECRET_KEY env vars,
    or configure a named profile 'wasabi' in ~/.aws/credentials.
"""

import argparse
import os
import sys
from pathlib import Path

import pandas as pd

DEST_BUCKET   = "saha-open-data"
DEST_REGION   = "us-east-1"
WASABI_REGION = "us-east-1"
WASABI_ENDPOINT = "https://s3.wasabisys.com"

MANIFEST_PATH = Path(__file__).parent.parent / "sample_metadata.csv"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_wasabi_client():
    import boto3
    access_key = os.environ.get("WASABI_ACCESS_KEY")
    secret_key = os.environ.get("WASABI_SECRET_KEY")
    if access_key and secret_key:
        return boto3.client(
            "s3",
            endpoint_url=WASABI_ENDPOINT,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=WASABI_REGION,
        )
    # Fall back to named profile
    import boto3.session
    session = boto3.session.Session(profile_name="wasabi",
                                    region_name=WASABI_REGION)
    return session.client("s3", endpoint_url=WASABI_ENDPOINT)


def get_s3_client():
    import boto3
    return boto3.client("s3", region_name=DEST_REGION)


def list_wasabi_objects(wasabi, bucket: str, prefix: str) -> list[dict]:
    """List all objects under prefix in Wasabi bucket."""
    objects = []
    paginator = wasabi.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objects.extend(page.get("Contents", []))
    return objects


def copy_object(wasabi, s3, wasabi_bucket: str, wasabi_key: str,
                dest_bucket: str, dest_key: str, dry_run: bool,
                size_bytes: int = 0) -> None:
    size_mb = size_bytes / 1_048_576
    if dry_run:
        print(f"    [dry-run] wasabi://{wasabi_bucket}/{wasabi_key}")
        print(f"           → s3://{dest_bucket}/{dest_key}  ({size_mb:.1f} MB)")
        return

    # Stream from Wasabi → S3
    response = wasabi.get_object(Bucket=wasabi_bucket, Key=wasabi_key)
    body = response["Body"]
    s3.upload_fileobj(body, dest_bucket, dest_key)
    print(f"    ✓ {dest_key}  ({size_mb:.1f} MB)")


# ---------------------------------------------------------------------------
# Per-sample sync
# ---------------------------------------------------------------------------

def sync_sample(sample_id: str, wasabi_bucket: str, wasabi_prefix: str,
                wasabi, s3, dry_run: bool) -> bool:
    """
    Sync one sample's raw flat files from Wasabi to S3.
    Returns True on success.
    """
    src_prefix  = f"{wasabi_prefix}/{sample_id}/".lstrip("/")
    dest_prefix = f"raw/cosmx/{sample_id}"

    objects = list_wasabi_objects(wasabi, wasabi_bucket, src_prefix)
    if not objects:
        # Try without trailing slash / without sample subfolder
        src_prefix = f"{wasabi_prefix}/{sample_id}".lstrip("/")
        objects = list_wasabi_objects(wasabi, wasabi_bucket, src_prefix)

    if not objects:
        print(f"  WARNING: no objects found for {sample_id} under "
              f"wasabi://{wasabi_bucket}/{src_prefix}")
        return False

    # Exclude .DS_Store and temp files
    objects = [o for o in objects
               if not os.path.basename(o["Key"]).startswith(".")
               and not os.path.basename(o["Key"]).startswith("temp_")]

    print(f"  {sample_id}: {len(objects)} objects")
    for obj in objects:
        rel_key = obj["Key"][len(src_prefix):].lstrip("/")
        dest_key = f"{dest_prefix}/{rel_key}"
        copy_object(wasabi, s3, wasabi_bucket, obj["Key"],
                    DEST_BUCKET, dest_key, dry_run, obj.get("Size", 0))
    return True


# ---------------------------------------------------------------------------
# Backfill s3_raw_prefix into sample_metadata.csv
# ---------------------------------------------------------------------------

def backfill_manifest(synced_samples: list[str]) -> None:
    df = pd.read_csv(MANIFEST_PATH, dtype=str, keep_default_na=False)
    for idx, row in df.iterrows():
        sid = row["Folder Name"].strip()
        if sid in synced_samples:
            df.at[idx, "s3_raw_prefix"] = f"s3://{DEST_BUCKET}/raw/cosmx/{sid}/"
    df.to_csv(MANIFEST_PATH, index=False)
    print(f"\nBackfilled s3_raw_prefix for {len(synced_samples)} samples in {MANIFEST_PATH}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--wasabi-bucket", required=True,
                   help="Wasabi source bucket name")
    p.add_argument("--wasabi-prefix", default="",
                   help="Key prefix inside Wasabi bucket (e.g. 'cosmx_raw')")
    p.add_argument("--sample",
                   help="Sync a single sample_id only")
    p.add_argument("--backfill", action="store_true",
                   help="Update s3_raw_prefix in sample_metadata.csv after sync")
    p.add_argument("--dry-run",  action="store_true")
    return p.parse_args()


def main():
    args = parse_args()

    df = pd.read_csv(MANIFEST_PATH, dtype=str, keep_default_na=False)
    active = df[~df["release_status"].str.startswith("excluded", na=False)]
    wasabi_samples = active[
        active["Wasabi"].str.strip().str.upper() == "Y"
    ]["Folder Name"].str.strip().tolist()

    if args.sample:
        if args.sample not in wasabi_samples:
            sys.exit(f"ERROR: {args.sample} not in active Wasabi=Y samples")
        wasabi_samples = [args.sample]

    print(f"Samples to sync: {len(wasabi_samples)}")
    if args.dry_run:
        print("[DRY RUN]\n")

    try:
        wasabi = get_wasabi_client()
        s3     = get_s3_client() if not args.dry_run else None
    except Exception as e:
        sys.exit(f"ERROR: failed to initialise S3 clients: {e}")

    synced = []
    for sid in wasabi_samples:
        ok = sync_sample(sid, args.wasabi_bucket, args.wasabi_prefix,
                         wasabi, s3, dry_run=args.dry_run)
        if ok:
            synced.append(sid)

    print(f"\nSynced {len(synced)}/{len(wasabi_samples)} samples.")

    if args.backfill and synced and not args.dry_run:
        backfill_manifest(synced)


if __name__ == "__main__":
    main()
