#!/usr/bin/env python3
"""
validate_release.py
Pre-release validation suite for SAHA open data.

Runs two classes of checks:
  Local  — parquet referential integrity and path derivation (no AWS needed)
  S3     — confirm every expected S3 object exists; test anonymous access;
           stream-test the large all-organ h5ad (requires --s3 flag)

Usage:
    # Local checks only (fast, no AWS credentials needed)
    python scripts/validate_release.py

    # Full suite including S3 checks (requires AWS credentials + uploads complete)
    python scripts/validate_release.py --s3 \
        --bucket saha-open-data \
        --region us-east-1

    # Skip the large-file stream test (saves time)
    python scripts/validate_release.py --s3 --skip-stream-test

Requirements:
    pip install pandas pyarrow boto3 anndata
"""

import argparse
import sys
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

REPO = Path(__file__).parent.parent
DATA = REPO / "data"
BUCKET = "saha-open-data"
REGION = "us-east-1"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_passed = 0
_failed = 0


def ok(msg: str) -> None:
    global _passed
    _passed += 1
    print(f"  PASS  {msg}")


def fail(msg: str) -> None:
    global _failed
    _failed += 1
    print(f"  FAIL  {msg}")


def section(title: str) -> None:
    print(f"\n{'='*55}")
    print(f"  {title}")
    print(f"{'='*55}")


# ---------------------------------------------------------------------------
# Local checks
# ---------------------------------------------------------------------------

def check_parquet_files() -> dict:
    """Load all 4 parquets; return as dict of DataFrames."""
    section("Local — Parquet files exist and load")
    tables = {}
    for name in ("samples", "donors", "runs", "panels"):
        p = DATA / f"{name}.parquet"
        if not p.exists():
            fail(f"{name}.parquet missing at {p}")
            tables[name] = pd.DataFrame()
        else:
            df = pq.read_table(p).to_pandas()
            ok(f"{name}.parquet  ({len(df)} rows)")
            tables[name] = df
    return tables


def check_referential_integrity(tables: dict, require_s3_paths: bool = False) -> None:
    section("Local — Referential integrity")
    samples = tables["samples"]
    runs    = tables["runs"]
    donors  = tables["donors"]
    panels  = tables["panels"]

    if samples.empty:
        fail("samples table empty — skipping integrity checks")
        return

    # 6.2  run_id
    sample_run_ids = set(samples["run_id"].dropna())
    run_ids        = set(runs["run_id"].dropna())
    missing        = sample_run_ids - run_ids
    if missing:
        fail(f"run_ids in samples missing from runs.parquet: {missing}")
    else:
        ok("All run_ids in samples exist in runs.parquet")

    # 6.3  donor_id
    sample_donor_ids = set(samples["donor_id"].dropna())
    donor_ids        = set(donors["donor_id"].dropna())
    missing          = sample_donor_ids - donor_ids
    if missing:
        fail(f"donor_ids in samples missing from donors.parquet: {missing}")
    else:
        ok("All donor_ids in samples exist in donors.parquet")

    # panel_name
    sample_panels = set(samples["panel_name"].dropna())
    panel_names   = set(panels["panel_name"].dropna())
    missing       = sample_panels - panel_names
    if missing:
        fail(f"panel_names in samples missing from panels.parquet: {missing}")
    else:
        ok("All panel_names in samples exist in panels.parquet")

    # n_cells coverage
    n_null = samples["n_cells"].isna().sum()
    n_total = len(samples)
    if n_null == 0:
        ok(f"n_cells populated for all {n_total} samples")
    elif n_null <= 4:
        ok(f"n_cells: {n_total - n_null}/{n_total} populated "
           f"({n_null} null — expected for samples without h5ad yet)")
    else:
        fail(f"n_cells null for {n_null}/{n_total} samples")

    # s3_processed_path non-null for non-disease samples
    # (only a hard failure after backfill_s3_paths.py has been run, i.e. post-upload)
    non_disease = samples[~samples["condition"].isin(["cancer", "disease"])]
    missing_paths = non_disease["s3_processed_path"].isna() | (non_disease["s3_processed_path"] == "")
    n_missing = int(missing_paths.sum())
    if n_missing == 0:
        ok(f"s3_processed_path set for all {len(non_disease)} normal samples")
    elif require_s3_paths:
        ids = non_disease.loc[missing_paths, "sample_id"].tolist()
        fail(f"s3_processed_path missing for {n_missing} normal samples: {ids[:5]}{'...' if n_missing > 5 else ''}")
    else:
        print(f"  WARN  s3_processed_path empty for {n_missing}/{len(non_disease)} samples "
              f"— run backfill_s3_paths.py after upload (Phase 3.6)")


def check_schema_enums(tables: dict) -> None:
    section("Local — Enum value validation")
    samples = tables["samples"]
    if samples.empty:
        return

    valid_platforms = {"cosmx", "xenium", "g4x", "geomx", "rnascope"}
    valid_organs    = {
        "appendix", "bone_marrow", "colon", "ileum", "kidney", "liver",
        "lung", "lymph_node", "pancreas", "prostate", "spleen",
        "stomach", "thymus", "other"
    }
    valid_conditions = {"normal", "cancer", "disease", "other"}
    valid_institutions = {"cornell", "wcm", "st_jude", "msk", "broad", "other"}

    for col, valid in [
        ("platform",              valid_platforms),
        ("organ",                 valid_organs),
        ("condition",             valid_conditions),
        ("generation_institution", valid_institutions),
    ]:
        bad = samples[~samples[col].isin(valid)][col].unique().tolist()
        if bad:
            fail(f"Invalid {col} values: {bad}")
        else:
            ok(f"All {col} values are valid enums")


# ---------------------------------------------------------------------------
# S3 checks
# ---------------------------------------------------------------------------

def check_s3_objects(s3, samples: pd.DataFrame, bucket: str) -> None:
    section("S3 — Expected objects exist (6.1)")
    missing = []
    for _, row in samples.iterrows():
        for col in ("s3_processed_path", "s3_raw_prefix"):
            path = str(row.get(col, "") or "").strip()
            if not path:
                continue
            key = path.replace(f"s3://{bucket}/", "").rstrip("/")
            # For directories (raw prefix), just check the prefix exists via list
            if path.endswith("/"):
                resp = s3.list_objects_v2(Bucket=bucket, Prefix=key, MaxKeys=1)
                if resp.get("KeyCount", 0) == 0:
                    missing.append(path)
            else:
                # Single file
                try:
                    s3.head_object(Bucket=bucket, Key=key)
                except Exception:
                    missing.append(path)

    if missing:
        fail(f"{len(missing)} expected S3 objects not found:")
        for m in missing[:10]:
            print(f"    {m}")
        if len(missing) > 10:
            print(f"    ... ({len(missing)} total)")
    else:
        ok(f"All expected S3 objects exist ({len(samples)} samples checked)")


def check_metadata_on_s3(s3, bucket: str) -> None:
    section("S3 — Metadata Parquet files exist")
    for name in ("samples", "donors", "runs", "panels"):
        key = f"metadata/{name}.parquet"
        try:
            s3.head_object(Bucket=bucket, Key=key)
            ok(f"s3://{bucket}/{key}")
        except Exception:
            fail(f"s3://{bucket}/{key} not found")


def check_anonymous_access(bucket: str, region: str) -> None:
    """6.5 — Test that public prefixes are accessible without credentials."""
    section("S3 — Anonymous access (6.5)")
    try:
        import boto3
        from botocore import UNSIGNED
        from botocore.config import Config

        anon = boto3.client(
            "s3",
            region_name=region,
            config=Config(signature_version=UNSIGNED),
        )
        for prefix in ("metadata/", "processed/", "zarr/", "docs/"):
            resp = anon.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
            if resp.get("KeyCount", 0) > 0:
                ok(f"Anonymous list: s3://{bucket}/{prefix}")
            else:
                fail(f"Anonymous list returned 0 objects for s3://{bucket}/{prefix}")
    except Exception as e:
        fail(f"Anonymous access test error: {e}")


def check_stream_large_file(s3, bucket: str) -> None:
    """6.6 — Stream the large all-organ h5ad with backed='r'."""
    section("S3 — Large file streaming (6.6)")
    try:
        import anndata as ad
        import s3fs
    except ImportError:
        fail("anndata or s3fs not installed — skipping stream test")
        return

    key = "processed/h5ad/cosmx/RNA/SAHA_All_RNA.h5ad"
    uri = f"s3://{bucket}/{key}"
    try:
        fs = s3fs.S3FileSystem(anon=False)
        if not fs.exists(uri):
            fail(f"{uri} does not exist — skipping")
            return
        adata = ad.read_h5ad(uri, backed="r")
        ok(f"Streamed {uri}: shape={adata.shape}")
        adata.file.close()
    except Exception as e:
        fail(f"Stream test failed: {e}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--s3",    action="store_true",
                   help="Run S3 checks (requires AWS credentials and completed uploads)")
    p.add_argument("--bucket",  default=BUCKET)
    p.add_argument("--region",  default=REGION)
    p.add_argument("--skip-stream-test", action="store_true",
                   help="Skip the large h5ad streaming test (6.6)")
    return p.parse_args()


def main():
    args = parse_args()

    tables = check_parquet_files()
    check_referential_integrity(tables, require_s3_paths=args.s3)
    check_schema_enums(tables)

    if args.s3:
        try:
            import boto3
            s3 = boto3.client("s3", region_name=args.region)
        except ImportError:
            sys.exit("ERROR: boto3 not installed. pip install boto3")

        check_s3_objects(s3, tables.get("samples", pd.DataFrame()), args.bucket)
        check_metadata_on_s3(s3, args.bucket)
        check_anonymous_access(args.bucket, args.region)
        if not args.skip_stream_test:
            check_stream_large_file(s3, args.bucket)
    else:
        print("\n  (S3 checks skipped — pass --s3 after uploads are complete)")

    section("Summary")
    print(f"  {_passed} passed,  {_failed} failed")
    if _failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
