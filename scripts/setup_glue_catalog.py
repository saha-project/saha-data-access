#!/usr/bin/env python3
"""
setup_glue_catalog.py
Register SAHA Parquet metadata tables in AWS Glue Data Catalog so they
are queryable via Amazon Athena without a dedicated database server.

What this script does:
  1. Creates (or updates) a Glue database named `saha`.
  2. For each of the four metadata tables (samples, donors, runs, panels)
     it creates or replaces a Glue table pointing at the corresponding
     Parquet prefix on S3.
  3. Prints the Athena DDL you can run to verify the tables.

Usage:
    # Dry-run (print what would be created, no AWS calls)
    python scripts/setup_glue_catalog.py --dry-run

    # Real run (requires AWS credentials with glue:* permissions)
    python scripts/setup_glue_catalog.py \
        --bucket saha-open-data \
        --prefix metadata \
        --region us-east-1 \
        --database saha

Requirements:
    pip install boto3
    AWS credentials configured (e.g. via ~/.aws/credentials or IAM role)
"""

import argparse
import json
import sys

# ---------------------------------------------------------------------------
# Glue column-type definitions derived from schemas
# ---------------------------------------------------------------------------

TABLES = {
    "samples": {
        "description": "One row per tissue sample. Partitioned by organ and platform.",
        "partition_keys": [
            {"Name": "organ",    "Type": "string"},
            {"Name": "platform", "Type": "string"},
        ],
        "columns": [
            {"Name": "sample_id",                  "Type": "string"},
            {"Name": "run_id",                     "Type": "string"},
            {"Name": "assay_type",                 "Type": "string"},
            {"Name": "generation_institution",     "Type": "string"},
            {"Name": "tissue_source_institution",  "Type": "string"},
            {"Name": "condition",                  "Type": "string"},
            {"Name": "anatomical_region",          "Type": "string"},
            {"Name": "donor_id",                   "Type": "string"},
            {"Name": "slide_id",                   "Type": "string"},
            {"Name": "panel_name",                 "Type": "string"},
            {"Name": "panel_plex",                 "Type": "int"},
            {"Name": "n_cells",                    "Type": "bigint"},
            {"Name": "n_fovs",                     "Type": "int"},
            {"Name": "qc_pass",                    "Type": "boolean"},
            {"Name": "processing_date",            "Type": "string"},
            {"Name": "slide_prepared_date",        "Type": "string"},
            {"Name": "s3_raw_prefix",              "Type": "string"},
            {"Name": "s3_processed_path",          "Type": "string"},
            {"Name": "on_wasabi",                  "Type": "boolean"},
        ],
    },
    "donors": {
        "description": "One row per de-identified tissue donor.",
        "partition_keys": [],
        "columns": [
            {"Name": "donor_id",                  "Type": "string"},
            {"Name": "age",                       "Type": "int"},
            {"Name": "age_group",                 "Type": "string"},
            {"Name": "sex",                       "Type": "string"},
            {"Name": "ethnicity",                 "Type": "string"},
            {"Name": "comorbidities",             "Type": "array<string>"},
            {"Name": "tissue_source_institution", "Type": "string"},
            {"Name": "tissue_source_country",     "Type": "string"},
            {"Name": "consent_level",             "Type": "string"},
        ],
    },
    "runs": {
        "description": "One row per instrument acquisition run (batch).",
        "partition_keys": [],
        "columns": [
            {"Name": "run_id",                      "Type": "string"},
            {"Name": "platform",                    "Type": "string"},
            {"Name": "instrument_id",               "Type": "string"},
            {"Name": "generation_institution",      "Type": "string"},
            {"Name": "run_date",                    "Type": "string"},
            {"Name": "panel_name",                  "Type": "string"},
            {"Name": "panel_version",               "Type": "string"},
            {"Name": "n_samples",                   "Type": "int"},
            {"Name": "instrument_software_version", "Type": "string"},
            {"Name": "slide_prep_manual",           "Type": "string"},
            {"Name": "instrument_manual",           "Type": "string"},
            {"Name": "qc_summary",                  "Type": "string"},
            {"Name": "s3_raw_prefix",               "Type": "string"},
        ],
    },
    "panels": {
        "description": "One row per panel configuration.",
        "partition_keys": [],
        "columns": [
            {"Name": "panel_name",     "Type": "string"},
            {"Name": "platform",       "Type": "string"},
            {"Name": "assay_type",     "Type": "string"},
            {"Name": "plex",           "Type": "int"},
            {"Name": "version",        "Type": "string"},
            {"Name": "gene_list",      "Type": "array<string>"},
            {"Name": "protein_list",   "Type": "array<string>"},
            {"Name": "catalog_number", "Type": "string"},
            {"Name": "notes",          "Type": "string"},
        ],
    },
}


# ---------------------------------------------------------------------------
# Glue helpers
# ---------------------------------------------------------------------------

def build_storage_descriptor(bucket: str, prefix: str, table: str,
                              columns: list[dict]) -> dict:
    s3_location = f"s3://{bucket}/{prefix}/{table}/"
    return {
        "Location": s3_location,
        "InputFormat":  "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
        "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
        "SerdeInfo": {
            "SerializationLibrary":
                "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
            "Parameters": {"serialization.format": "1"},
        },
        "Columns": columns,
        "Compressed": False,
        "NumberOfBuckets": -1,
        "StoredAsSubDirectories": False,
    }


def ensure_database(glue, database: str, dry_run: bool) -> None:
    if dry_run:
        print(f"[dry-run] Would create Glue database '{database}'")
        return
    try:
        glue.get_database(Name=database)
        print(f"  Database '{database}' already exists.")
    except glue.exceptions.EntityNotFoundException:
        glue.create_database(
            DatabaseInput={
                "Name": database,
                "Description": "SAHA spatial omics consortium metadata (AWS Open Data)",
            }
        )
        print(f"  Created Glue database '{database}'.")


def upsert_table(glue, database: str, table_name: str, table_def: dict,
                 bucket: str, prefix: str, dry_run: bool) -> None:
    storage = build_storage_descriptor(
        bucket, prefix, table_name, table_def["columns"]
    )
    table_input = {
        "Name": table_name,
        "Description": table_def["description"],
        "StorageDescriptor": storage,
        "PartitionKeys": table_def["partition_keys"],
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            "classification": "parquet",
            "compressionType": "snappy",
            "EXTERNAL": "TRUE",
        },
    }

    if dry_run:
        print(f"[dry-run] Would upsert table '{database}.{table_name}'")
        print(f"          S3 location: {storage['Location']}")
        return

    try:
        glue.get_table(DatabaseName=database, Name=table_name)
        glue.update_table(DatabaseName=database, TableInput=table_input)
        print(f"  Updated table '{database}.{table_name}'.")
    except glue.exceptions.EntityNotFoundException:
        glue.create_table(DatabaseName=database, TableInput=table_input)
        print(f"  Created table '{database}.{table_name}'.")


# ---------------------------------------------------------------------------
# Athena verification DDL
# ---------------------------------------------------------------------------

def print_athena_queries(database: str) -> None:
    print("\n--- Athena verification queries ---")
    for table in TABLES:
        print(f"SELECT * FROM {database}.{table} LIMIT 5;")
    print()
    print("-- Cross-table join example:")
    print(f"SELECT s.sample_id, s.organ, s.condition, d.sex, d.age_group")
    print(f"FROM {database}.samples s")
    print(f"JOIN {database}.donors d ON s.donor_id = d.donor_id")
    print(f"WHERE s.qc_pass = true AND s.organ = 'colon'")
    print(f"LIMIT 20;")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--bucket",   default="saha-open-data",
                   help="S3 bucket name (default: saha-open-data)")
    p.add_argument("--prefix",   default="metadata",
                   help="S3 key prefix for metadata Parquet files (default: metadata)")
    p.add_argument("--region",   default="us-east-1")
    p.add_argument("--database", default="saha",
                   help="Glue database name (default: saha)")
    p.add_argument("--dry-run",  action="store_true",
                   help="Print what would be done without making AWS API calls")
    return p.parse_args()


def main():
    args = parse_args()

    if not args.dry_run:
        try:
            import boto3
        except ImportError:
            sys.exit("ERROR: boto3 not installed. Run: pip install boto3")

        glue = boto3.client("glue", region_name=args.region)
    else:
        glue = None

    print(f"Setting up Glue catalog: database='{args.database}', "
          f"bucket='s3://{args.bucket}/{args.prefix}/'")

    ensure_database(glue, args.database, args.dry_run)

    for table_name, table_def in TABLES.items():
        upsert_table(
            glue, args.database, table_name, table_def,
            args.bucket, args.prefix, args.dry_run,
        )

    print_athena_queries(args.database)
    print("Done.")


if __name__ == "__main__":
    main()
