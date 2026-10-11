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

sys.path.insert(0, str(Path(__file__).resolve().parent))
import donor_map

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

    # 6.3  donor identity
    #
    # This check used to compare samples.donor_id against donors.donor_id and
    # report PASS. It passed while 36 of 47 samples pointed at a donor from a
    # different organ: the ids existed, they were simply the wrong people. An
    # existence test cannot catch a re-pointed key, so the check now follows the
    # cores through and verifies the organs agree.
    if "donor_id" in samples.columns:
        fail("samples.parquet still carries a scalar donor_id — donor is per-cell "
             "(obs donor_core); a single id per sample cannot describe a TMA")
    else:
        ok("samples.parquet carries no scalar donor_id")

    cores = donor_map.sample_cores()
    by_core = donors.set_index("donor_core") if not donors.empty else None

    # donors.parquet also documents cohort members with no data yet. They must
    # never be referenced by a sample -- if one is, either the flag is stale or
    # an acquisition was attributed to tissue that was never imaged.
    if "in_release" in donors.columns:
        awaiting = set(donors.loc[~donors.in_release, "donor_core"])
        referenced = {c for s_id in samples["sample_id"]
                      for c in donor_map.cores_for(s_id, cores)}
        wrong = sorted(awaiting & referenced)
        if wrong:
            fail(f"{len(wrong)} core(s) flagged as awaiting data are referenced by a "
                 f"sample: {wrong[:5]}")
        else:
            ok(f"{len(awaiting)} core(s) awaiting acquisition, none referenced by a sample")

    unknown, mismatched, uncovered = [], [], []
    for _, r in samples.iterrows():
        sample_cores = donor_map.cores_for(r["sample_id"], cores)
        if not sample_cores:
            uncovered.append(r["sample_id"])
            continue
        for c in sample_cores:
            if by_core is None or c not in by_core.index:
                unknown.append((r["sample_id"], c))
                continue
            donor_organ = by_core.loc[c, "organ"]
            if donor_organ and r["organ"] and donor_organ != r["organ"]:
                mismatched.append((r["sample_id"], c, r["organ"], donor_organ))

    if uncovered:
        fail(f"{len(uncovered)} sample(s) with no donor core: {uncovered[:5]}")
    else:
        ok(f"All {len(samples)} samples resolve to at least one donor core")

    if unknown:
        fail(f"{len(unknown)} donor core(s) referenced by samples but absent from "
             f"donors.parquet: {unknown[:5]}")
    else:
        ok("Every donor core referenced by a sample exists in donors.parquet")

    if mismatched:
        fail(f"{len(mismatched)} sample/donor organ mismatch(es) — "
             f"e.g. {mismatched[0][0]} is {mismatched[0][2]} but core "
             f"{mismatched[0][1]} is {mismatched[0][3]}")
    else:
        ok("Every sample's donor cores come from the same organ as the sample")

    # n_donors must agree with the cores actually assigned
    if "n_donors" in samples.columns:
        wrong = [(r["sample_id"], r["n_donors"],
                  len(donor_map.cores_for(r["sample_id"], cores)))
                 for _, r in samples.iterrows()
                 if r["n_donors"] != len(donor_map.cores_for(r["sample_id"], cores))]
        if wrong:
            fail(f"n_donors disagrees with the donor map for {len(wrong)} sample(s): "
                 f"{wrong[:5]}")
        else:
            ok("n_donors matches the resolved donor cores for every sample")

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

    # Read the allowed values from schemas/samples.json rather than keeping a
    # second copy here. The duplicate lists went stale the moment the release
    # gained a rectum organ and a Northwestern site: the schema accepted the
    # rows, this check rejected them, and neither was wrong about the data.
    import json
    schema = json.loads((REPO / "schemas" / "samples.json").read_text())
    props = schema.get("properties", {})

    for col in ("platform", "organ", "condition", "generation_institution"):
        enum = props.get(col, {}).get("enum")
        if not enum:
            fail(f"{col} has no enum in schemas/samples.json")
            continue
        valid = {v for v in enum if v is not None}
        bad = sorted(set(samples[col].dropna()) - valid)
        if bad:
            fail(f"Invalid {col} values: {bad} (allowed: {sorted(valid)})")
        else:
            ok(f"All {col} values are valid enums ({len(valid)} allowed)")


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
        for prefix in ("metadata/", "processed/", "docs/"):
            resp = anon.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
            if resp.get("KeyCount", 0) > 0:
                ok(f"Anonymous list: s3://{bucket}/{prefix}")
            else:
                fail(f"Anonymous list returned 0 objects for s3://{bucket}/{prefix}")
    except Exception as e:
        fail(f"Anonymous access test error: {e}")


def check_stream_large_file(s3, bucket: str) -> None:
    """6.6 — Open the largest SpatialData object anonymously, straight from S3."""
    section("S3 — Remote read of the largest object (6.6)")
    try:
        import spatialdata
        from package_for_zenodo import record_slug
    except ImportError:
        fail("spatialdata not installed — skipping stream test")
        return

    s = pd.read_parquet(DATA / "samples.parquet")
    row = s[s.platform == "cosmx"].sort_values("n_cells").iloc[-1]   # no _RNA suffix on CosMx
    uri = f"s3://{bucket}/processed/{record_slug(row)}/{row.sample_id}.zarr"
    try:
        sd = spatialdata.read_zarr(uri, storage_options={"anon": True})
        n = sum(t.n_obs for t in sd.tables.values())
        if n == row.n_cells:
            ok(f"Opened {uri} anonymously: {n:,} cells")
        else:
            fail(f"Opened {uri} but it holds {n:,} cells, samples.parquet says {row.n_cells:,}")
    except Exception as e:
        fail(f"Remote read failed for {uri}: {e}")


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
