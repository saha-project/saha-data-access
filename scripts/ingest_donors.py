#!/usr/bin/env python3
"""
ingest_donors.py
Map donor_metadata.csv → donors.parquet, validating against schemas/donors.json.

Usage:
    python scripts/ingest_donors.py \
        --input  donor_metadata.csv \
        --output data/donors.parquet \
        --schema schemas/donors.json

Expected input CSV columns (case-insensitive):
    donor_id, age, sex, ethnicity, comorbidities,
    tissue_source_institution, tissue_source_country, consent_level

Requirements:
    pip install pandas pyarrow jsonschema
"""

import argparse
import json
import sys
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from jsonschema import Draft202012Validator

# ---------------------------------------------------------------------------
# Lookup tables
# ---------------------------------------------------------------------------

SEX_MAP = {
    "m": "M", "male": "M",
    "f": "F", "female": "F",
    "unknown": "unknown", "": "unknown",
}

AGE_GROUP_MAP = [
    (0,   17,  "pediatric"),
    (18,  64,  "adult"),
    (65,  120, "elderly"),
]

CONSENT_LEVELS = {"open", "registered", "controlled"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

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


def parse_age_group(age: "int | None") -> "str | None":
    if age is None:
        return None
    for lo, hi, label in AGE_GROUP_MAP:
        if lo <= age <= hi:
            return label
    return None


def parse_comorbidities(value: object) -> "list[str] | None":
    """Accept semicolon- or comma-separated string or JSON array string."""
    if value is None:
        return None
    s = str(value).strip()
    if s in ("", "N/A", "nan", "NaN", "[]"):
        return None
    # Try JSON array first
    if s.startswith("["):
        try:
            parsed = json.loads(s)
            return [str(x).strip() for x in parsed if str(x).strip()]
        except json.JSONDecodeError:
            pass
    # Semicolon-separated
    if ";" in s:
        return [x.strip() for x in s.split(";") if x.strip()]
    # Comma-separated
    return [x.strip() for x in s.split(",") if x.strip()]


def normalize_country(value: object) -> "str | None":
    """Uppercase 2-letter country code; return None if invalid."""
    if value is None:
        return None
    s = str(value).strip().upper()
    if s in ("", "N/A", "NAN"):
        return None
    if len(s) == 2 and s.isalpha():
        return s
    return None


def normalize_consent(value: object) -> "str | None":
    if value is None:
        return None
    s = str(value).strip().lower()
    return s if s in CONSENT_LEVELS else None


# ---------------------------------------------------------------------------
# Core transform
# ---------------------------------------------------------------------------

def transform(df: pd.DataFrame) -> list[dict]:
    """Map raw donor CSV columns to the donors schema."""
    # Normalize column names to lower-snake_case
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    records = []
    for _, row in df.iterrows():
        donor_id = str(row.get("donor_id", "")).strip()
        if not donor_id or donor_id in ("nan", "N/A"):
            continue

        age = safe_int(row.get("age"))

        record = {
            "donor_id":                  donor_id,
            "age":                       age,
            "age_group":                 parse_age_group(age),
            "sex":                       SEX_MAP.get(
                                             str(row.get("sex", "")).strip().lower(),
                                             "unknown"
                                         ),
            "ethnicity":                 str(row.get("ethnicity", "")).strip() or None,
            "comorbidities":             parse_comorbidities(row.get("comorbidities")),
            "tissue_source_institution": str(row.get("tissue_source_institution", "")).strip() or None,
            "tissue_source_country":     normalize_country(row.get("tissue_source_country")),
            "consent_level":             normalize_consent(row.get("consent_level")),
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
    ("donor_id",                  pa.string()),
    ("age",                       pa.int32()),
    ("age_group",                 pa.string()),
    ("sex",                       pa.string()),
    ("ethnicity",                 pa.string()),
    ("comorbidities",             pa.list_(pa.string())),
    ("tissue_source_institution", pa.string()),
    ("tissue_source_country",     pa.string()),
    ("consent_level",             pa.string()),
])


def write_parquet(records: list[dict], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(records)

    for field in PARQUET_SCHEMA:
        if field.name not in df.columns:
            df[field.name] = None

    # list columns need special handling for pyarrow
    table = pa.Table.from_pandas(
        df[[f.name for f in PARQUET_SCHEMA]],
        schema=PARQUET_SCHEMA,
        preserve_index=False,
    )
    pq.write_table(table, output_path, compression="snappy")
    print(f"Wrote {len(records)} donors to {output_path}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--input",  default="donor_metadata.csv")
    p.add_argument("--output", default="data/donors.parquet")
    p.add_argument("--schema", default="schemas/donors.json")
    p.add_argument("--no-validate", action="store_true")
    p.add_argument("--strict",      action="store_true",
                   help="Exit non-zero if any validation errors are found")
    return p.parse_args()


def main():
    args = parse_args()

    csv_path = Path(args.input)
    if not csv_path.exists():
        sys.exit(f"ERROR: input file not found: {csv_path}")

    schema_path = Path(args.schema)
    if not schema_path.exists():
        sys.exit(f"ERROR: schema file not found: {schema_path}")

    print(f"Reading {csv_path} …")
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    df.replace({"N/A": None, "": None}, inplace=True)

    print(f"Transforming {len(df)} rows …")
    records = transform(df)
    print(f"  {len(records)} donor records produced")

    if not args.no_validate:
        schema = json.loads(schema_path.read_text())
        errors = validate_records(records, schema)
        if errors:
            print(f"\nValidation warnings ({len(errors)} issues):")
            for idx, msg in errors[:50]:
                print(f"  row {idx} ({records[idx].get('donor_id', '?')}): {msg}")
            if len(errors) > 50:
                print(f"  … and {len(errors) - 50} more")
            if args.strict:
                sys.exit(1)
        else:
            print("  All records valid.")

    write_parquet(records, Path(args.output))


if __name__ == "__main__":
    main()
