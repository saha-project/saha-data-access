#!/usr/bin/env python3
"""
ingest_runs.py
Derive runs.parquet from sample_metadata.csv (by collapsing on Batch/run_id),
or accept a dedicated run_metadata.csv.  Validates against schemas/runs.json.

Usage — derive from existing samples manifest:
    python scripts/ingest_runs.py \
        --input  sample_metadata.csv \
        --output data/runs.parquet \
        --schema schemas/runs.json

Usage — from a dedicated runs CSV:
    python scripts/ingest_runs.py \
        --input  run_metadata.csv \
        --mode   direct \
        --output data/runs.parquet

Requirements:
    pip install pandas pyarrow jsonschema
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from jsonschema import Draft202012Validator

# ---------------------------------------------------------------------------
# Same lookup tables as ingest_samples.py
# ---------------------------------------------------------------------------

PLATFORM_MAP = {
    "cosmx rna":     "cosmx",
    "cosmx protein": "cosmx",
    "xenium":        "xenium",
    "g4x":           "g4x",
    "geomx":         "geomx",
    "rnascope":      "rnascope",
}

INSTITUTION_MAP = {
    "tap":      "cornell",
    "wcm":      "wcm",
    "st.jude":  "st_jude",
    "msk-sail": "msk",
    "msk":      "msk",
    "broad":    "broad",
}


def parse_date(value: object) -> "str | None":
    if value is None:
        return None
    s = str(value).strip()
    if s in ("", "N/A", "nan", "NaN"):
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def safe_int(value: object) -> "int | None":
    if value is None:
        return None
    s = str(value).strip()
    if s in ("", "N/A", "nan", "NaN"):
        return None
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Derive mode: collapse sample manifest → one row per Batch/run
# ---------------------------------------------------------------------------

def transform_derived(df: pd.DataFrame) -> list[dict]:
    """Aggregate sample_metadata.csv rows by Batch to produce run records."""
    records = []

    # Group by batch (run_id)
    batch_col = "Batch"
    if batch_col not in df.columns:
        print("WARNING: 'Batch' column not found — cannot derive runs.")
        return records

    for run_id, group in df.groupby(batch_col, dropna=False):
        run_id = str(run_id).strip()
        if not run_id or run_id in ("nan", "N/A"):
            continue

        # Platform: most common assay in the group
        assay_raw = group["Assay"].mode()[0] if "Assay" in group.columns else ""
        platform = PLATFORM_MAP.get(str(assay_raw).strip().lower())

        # Institution: most common
        inst_raw = group["Run location"].mode()[0] if "Run location" in group.columns else ""
        generation_institution = INSTITUTION_MAP.get(str(inst_raw).strip().lower(), "other")

        # Earliest run date in group
        run_date = None
        if "Run start date" in group.columns:
            dates = [parse_date(v) for v in group["Run start date"] if parse_date(v)]
            run_date = min(dates) if dates else None

        # Panel: most common
        panel_name = None
        if "Panel" in group.columns:
            panel_name = str(group["Panel"].mode()[0]).strip() or None

        record = {
            "run_id":                      run_id,
            "platform":                    platform,
            "instrument_id":               None,
            "generation_institution":      generation_institution,
            "run_date":                    run_date,
            "panel_name":                  panel_name,
            "panel_version":               None,
            "n_samples":                   len(group),
            "instrument_software_version": None,
            "slide_prep_manual":           None,
            "instrument_manual":           None,
            "qc_summary":                  None,
            "s3_raw_prefix":               None,
        }
        records.append(record)

    return records


# ---------------------------------------------------------------------------
# Direct mode: read a dedicated runs CSV (columns match schema)
# ---------------------------------------------------------------------------

def transform_direct(df: pd.DataFrame) -> list[dict]:
    """Map a runs-specific CSV to run records."""
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    records = []

    for _, row in df.iterrows():
        run_id = str(row.get("run_id", "")).strip()
        if not run_id or run_id in ("nan", "N/A"):
            continue

        platform_raw = str(row.get("platform", "")).strip().lower()
        platform = PLATFORM_MAP.get(platform_raw, platform_raw or None)

        inst_raw = str(row.get("generation_institution", "")).strip().lower()
        generation_institution = INSTITUTION_MAP.get(inst_raw, inst_raw or "other")

        record = {
            "run_id":                      run_id,
            "platform":                    platform,
            "instrument_id":               str(row.get("instrument_id", "")).strip() or None,
            "generation_institution":      generation_institution,
            "run_date":                    parse_date(row.get("run_date")),
            "panel_name":                  str(row.get("panel_name", "")).strip() or None,
            "panel_version":               str(row.get("panel_version", "")).strip() or None,
            "n_samples":                   safe_int(row.get("n_samples")),
            "instrument_software_version": str(row.get("instrument_software_version", "")).strip() or None,
            "slide_prep_manual":           str(row.get("slide_prep_manual", "")).strip() or None,
            "instrument_manual":           str(row.get("instrument_manual", "")).strip() or None,
            "qc_summary":                  str(row.get("qc_summary", "")).strip() or None,
            "s3_raw_prefix":               str(row.get("s3_raw_prefix", "")).strip() or None,
        }
        records.append(record)

    return records


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_records(records: list[dict], schema: dict) -> list[tuple[int, str]]:
    validator = Draft202012Validator(schema)
    errors = []
    for i, rec in enumerate(records):
        for err in validator.iter_errors(rec):
            errors.append((i, f"{err.json_path}: {err.message}"))
    return errors


# ---------------------------------------------------------------------------
# Parquet output
# ---------------------------------------------------------------------------

PARQUET_SCHEMA = pa.schema([
    ("run_id",                      pa.string()),
    ("platform",                    pa.string()),
    ("instrument_id",               pa.string()),
    ("generation_institution",      pa.string()),
    ("run_date",                    pa.string()),
    ("panel_name",                  pa.string()),
    ("panel_version",               pa.string()),
    ("n_samples",                   pa.int32()),
    ("instrument_software_version", pa.string()),
    ("slide_prep_manual",           pa.string()),
    ("instrument_manual",           pa.string()),
    ("qc_summary",                  pa.string()),
    ("s3_raw_prefix",               pa.string()),
])


def write_parquet(records: list[dict], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(records)

    for field in PARQUET_SCHEMA:
        if field.name not in df.columns:
            df[field.name] = None

    table = pa.Table.from_pandas(
        df[[f.name for f in PARQUET_SCHEMA]],
        schema=PARQUET_SCHEMA,
        preserve_index=False,
    )
    pq.write_table(table, output_path, compression="snappy")
    print(f"Wrote {len(records)} runs to {output_path}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--input",  default="sample_metadata.csv")
    p.add_argument("--output", default="data/runs.parquet")
    p.add_argument("--schema", default="schemas/runs.json")
    p.add_argument("--mode",   choices=["derived", "direct"], default="derived",
                   help="'derived' collapses sample manifest; 'direct' reads a runs CSV")
    p.add_argument("--no-validate", action="store_true")
    p.add_argument("--strict",      action="store_true")
    return p.parse_args()


def main():
    args = parse_args()

    csv_path = Path(args.input)
    if not csv_path.exists():
        sys.exit(f"ERROR: input file not found: {csv_path}")

    schema_path = Path(args.schema)
    if not schema_path.exists():
        sys.exit(f"ERROR: schema file not found: {schema_path}")

    print(f"Reading {csv_path} (mode={args.mode}) …")
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    df.replace({"N/A": None, "": None}, inplace=True)

    if args.mode == "derived":
        records = transform_derived(df)
    else:
        records = transform_direct(df)

    print(f"  {len(records)} run records produced")

    if not args.no_validate:
        schema = json.loads(schema_path.read_text())
        errors = validate_records(records, schema)
        if errors:
            print(f"\nValidation warnings ({len(errors)} issues):")
            for idx, msg in errors[:50]:
                print(f"  row {idx} ({records[idx].get('run_id', '?')}): {msg}")
            if len(errors) > 50:
                print(f"  … and {len(errors) - 50} more")
            if args.strict:
                sys.exit(1)
        else:
            print("  All records valid.")

    write_parquet(records, Path(args.output))


if __name__ == "__main__":
    main()
