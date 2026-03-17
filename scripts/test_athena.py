#!/usr/bin/env python3
"""
test_athena.py
Run a suite of validation queries against the SAHA Glue/Athena catalog
to confirm that the metadata Parquet files are correctly registered and
readable after running setup_glue_catalog.py.

Usage:
    # Dry-run: print queries without executing
    python scripts/test_athena.py --dry-run

    # Run against real Athena (requires AWS credentials + Athena output bucket)
    python scripts/test_athena.py \
        --output-bucket saha-open-data \
        --output-prefix athena-results \
        --database saha \
        --region us-east-1

Requirements:
    pip install boto3
    AWS credentials with athena:StartQueryExecution, athena:GetQueryResults,
    s3:PutObject on the output bucket.
"""

import argparse
import sys
import time

DATABASE = "saha"
REGION   = "us-east-1"

# ---------------------------------------------------------------------------
# Test queries — each is (name, sql, expected_min_rows)
# ---------------------------------------------------------------------------

TESTS = [
    (
        "samples_count",
        "SELECT COUNT(*) AS n FROM {db}.samples",
        1,
    ),
    (
        "samples_by_organ",
        "SELECT organ, COUNT(*) AS n FROM {db}.samples GROUP BY organ ORDER BY n DESC",
        1,
    ),
    (
        "samples_by_platform",
        "SELECT platform, COUNT(*) AS n FROM {db}.samples GROUP BY platform",
        1,
    ),
    (
        "donors_count",
        "SELECT COUNT(*) AS n FROM {db}.donors",
        1,
    ),
    (
        "donors_all",
        "SELECT donor_id, age_group, sex, consent_level FROM {db}.donors ORDER BY donor_id",
        1,
    ),
    (
        "runs_count",
        "SELECT COUNT(*) AS n FROM {db}.runs",
        1,
    ),
    (
        "panels_count",
        "SELECT COUNT(*) AS n FROM {db}.panels",
        1,
    ),
    (
        "join_samples_donors",
        (
            "SELECT s.sample_id, s.organ, s.condition, d.consent_level "
            "FROM {db}.samples s "
            "JOIN {db}.donors d ON s.donor_id = d.donor_id "
            "ORDER BY s.sample_id LIMIT 10"
        ),
        1,
    ),
    (
        "qc_pass_filter",
        "SELECT COUNT(*) AS n FROM {db}.samples WHERE qc_pass = true",
        1,
    ),
    (
        "s3_paths_non_null",
        (
            "SELECT sample_id, s3_processed_path "
            "FROM {db}.samples "
            "WHERE s3_processed_path IS NOT NULL AND s3_processed_path <> '' "
            "LIMIT 5"
        ),
        1,
    ),
    (
        "runs_protocol_fields",
        (
            "SELECT run_id, fiducial_concentration_pct, hybridization_duration_h, "
            "cell_segmentation_profile FROM {db}.runs LIMIT 5"
        ),
        1,
    ),
    (
        "panel_plex_check",
        "SELECT panel_name, plex FROM {db}.panels ORDER BY plex DESC",
        1,
    ),
]


# ---------------------------------------------------------------------------
# Athena helpers
# ---------------------------------------------------------------------------

def run_query(athena, sql: str, output_location: str, database: str) -> dict:
    resp = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": output_location},
    )
    qid = resp["QueryExecutionId"]

    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)
        state = status["QueryExecution"]["QueryExecutionStatus"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)

    if state != "SUCCEEDED":
        reason = status["QueryExecution"]["QueryExecutionStatus"].get("StateChangeReason", "")
        raise RuntimeError(f"Query {qid} {state}: {reason}")

    results = athena.get_query_results(QueryExecutionId=qid)
    return results


def count_rows(results: dict) -> int:
    rows = results.get("ResultSet", {}).get("Rows", [])
    return max(0, len(rows) - 1)  # subtract header


def print_results(results: dict, max_rows: int = 5) -> None:
    rows = results.get("ResultSet", {}).get("Rows", [])
    if not rows:
        print("    (no rows)")
        return
    header = [c.get("VarCharValue", "") for c in rows[0]["Data"]]
    print("    " + " | ".join(header))
    print("    " + "-" * (sum(len(h) for h in header) + 3 * len(header)))
    for row in rows[1 : max_rows + 1]:
        vals = [c.get("VarCharValue", "") for c in row["Data"]]
        print("    " + " | ".join(vals))
    if len(rows) - 1 > max_rows:
        print(f"    ... ({len(rows)-1} rows total)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--output-bucket", default="saha-open-data",
                   help="S3 bucket for Athena query results")
    p.add_argument("--output-prefix", default="athena-results",
                   help="S3 prefix for Athena query results")
    p.add_argument("--database", default=DATABASE)
    p.add_argument("--region",   default=REGION)
    p.add_argument("--dry-run",  action="store_true",
                   help="Print queries without executing")
    return p.parse_args()


def main():
    args = parse_args()
    output_location = f"s3://{args.output_bucket}/{args.output_prefix}/"

    if not args.dry_run:
        try:
            import boto3
            athena = boto3.client("athena", region_name=args.region)
        except ImportError:
            sys.exit("ERROR: boto3 not installed. Run: pip install boto3")

    passed = 0
    failed = 0

    for name, sql_template, min_rows in TESTS:
        sql = sql_template.format(db=args.database)
        print(f"\n[{name}]")
        print(f"  SQL: {sql}")

        if args.dry_run:
            print("  [dry-run] skipped")
            continue

        try:
            results = run_query(athena, sql, output_location, args.database)
            nrows = count_rows(results)
            if nrows >= min_rows:
                print(f"  PASS  ({nrows} rows returned)")
                print_results(results)
                passed += 1
            else:
                print(f"  FAIL  expected >= {min_rows} rows, got {nrows}")
                failed += 1
        except Exception as e:
            print(f"  ERROR  {e}")
            failed += 1

    if not args.dry_run:
        print(f"\n{'='*40}")
        print(f"Results: {passed} passed, {failed} failed out of {passed+failed} tests")
        if failed:
            sys.exit(1)


if __name__ == "__main__":
    main()
