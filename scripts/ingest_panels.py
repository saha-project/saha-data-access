#!/usr/bin/env python3
"""
ingest_panels.py
Write panels.parquet from a panels CSV or from the built-in SAHA panel seed data.

Usage — from CSV:
    python scripts/ingest_panels.py \
        --input  panel_metadata.csv \
        --output data/panels.parquet \
        --schema schemas/panels.json

Usage — seed built-in SAHA panels (no CSV required):
    python scripts/ingest_panels.py --seed --output data/panels.parquet

Expected CSV columns:
    panel_name, platform, assay_type, plex, version,
    gene_list (semicolon-separated), protein_list (semicolon-separated),
    catalog_number, notes

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
# Built-in seed data — extend as panels are finalized
# ---------------------------------------------------------------------------

SEED_PANELS = [
    {
        "panel_name":     "UCC 1K",
        "platform":       "cosmx",
        "assay_type":     "rna",
        "plex":           1000,
        "version":        None,
        "gene_list":      None,   # populate with actual gene list when available
        "protein_list":   None,
        "catalog_number": None,
        "notes":          "CosMx Universal Cell Characterization 1000-plex RNA panel",
    },
    {
        "panel_name":     "6K Discovery",
        "platform":       "cosmx",
        "assay_type":     "rna",
        "plex":           6000,
        "version":        None,
        "gene_list":      None,
        "protein_list":   None,
        "catalog_number": None,
        "notes":          "CosMx 6000-plex RNA discovery panel",
    },
    {
        "panel_name":     "19K",
        "platform":       "cosmx",
        "assay_type":     "rna",
        "plex":           19000,
        "version":        None,
        "gene_list":      None,
        "protein_list":   None,
        "catalog_number": None,
        "notes":          "CosMx whole-transcriptome 19000-plex RNA panel",
    },
    {
        "panel_name":     "Xenium Human Multi-Tissue",
        "platform":       "xenium",
        "assay_type":     "rna",
        "plex":           377,
        "version":        None,
        "gene_list":      None,
        "protein_list":   None,
        "catalog_number": None,
        "notes":          "10x Genomics Xenium Human Multi-Tissue and Cancer Panel",
    },
    {
        "panel_name":     "G4X",
        "platform":       "g4x",
        "assay_type":     "rna",
        "plex":           None,  # plex set per-run; update when finalized
        "version":        None,
        "gene_list":      None,
        "protein_list":   None,
        "catalog_number": None,
        "notes":          "G4X spatial transcriptomics panel",
    },
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def parse_list_column(value: object) -> "list[str] | None":
    """Accept semicolon-, comma-, or newline-separated string or JSON array."""
    if value is None:
        return None
    s = str(value).strip()
    if s in ("", "N/A", "nan", "NaN", "[]"):
        return None
    if s.startswith("["):
        try:
            parsed = json.loads(s)
            return [str(x).strip() for x in parsed if str(x).strip()]
        except json.JSONDecodeError:
            pass
    if ";" in s:
        return [x.strip() for x in s.split(";") if x.strip()]
    if "\n" in s:
        return [x.strip() for x in s.split("\n") if x.strip()]
    return [x.strip() for x in s.split(",") if x.strip()]


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
# Core transform
# ---------------------------------------------------------------------------

def transform_csv(df: pd.DataFrame) -> list[dict]:
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    records = []

    for _, row in df.iterrows():
        panel_name = str(row.get("panel_name", "")).strip()
        if not panel_name or panel_name in ("nan", "N/A"):
            continue

        platform = str(row.get("platform", "")).strip().lower() or None
        assay_type = str(row.get("assay_type", "")).strip().lower() or None
        if assay_type not in ("rna", "protein", "multiome"):
            assay_type = None

        record = {
            "panel_name":     panel_name,
            "platform":       platform,
            "assay_type":     assay_type,
            "plex":           safe_int(row.get("plex")),
            "version":        str(row.get("version", "")).strip() or None,
            "gene_list":      parse_list_column(row.get("gene_list")),
            "protein_list":   parse_list_column(row.get("protein_list")),
            "catalog_number": str(row.get("catalog_number", "")).strip() or None,
            "notes":          str(row.get("notes", "")).strip() or None,
        }
        records.append(record)

    return records


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_records(records: list[dict], schema: dict) -> list[tuple[int, str]]:
    # panels with plex=None fail schema minimum:1; skip those for seed data
    validator = Draft202012Validator(schema)
    errors = []
    for i, rec in enumerate(records):
        if rec.get("plex") is None:
            continue  # seed placeholder — allow
        for err in validator.iter_errors(rec):
            errors.append((i, f"{err.json_path}: {err.message}"))
    return errors


# ---------------------------------------------------------------------------
# Parquet output
# ---------------------------------------------------------------------------

PARQUET_SCHEMA = pa.schema([
    ("panel_name",     pa.string()),
    ("platform",       pa.string()),
    ("assay_type",     pa.string()),
    ("plex",           pa.int32()),
    ("version",        pa.string()),
    ("gene_list",      pa.list_(pa.string())),
    ("protein_list",   pa.list_(pa.string())),
    ("catalog_number", pa.string()),
    ("notes",          pa.string()),
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
    print(f"Wrote {len(records)} panels to {output_path}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--input",       default="panel_metadata.csv",
                   help="CSV input (ignored when --seed is set)")
    p.add_argument("--output",      default="data/panels.parquet")
    p.add_argument("--schema",      default="schemas/panels.json")
    p.add_argument("--seed",        action="store_true",
                   help="Write built-in SAHA seed panels instead of reading a CSV")
    p.add_argument("--no-validate", action="store_true")
    p.add_argument("--strict",      action="store_true")
    return p.parse_args()


def main():
    args = parse_args()

    schema_path = Path(args.schema)
    if not schema_path.exists():
        sys.exit(f"ERROR: schema file not found: {schema_path}")

    if args.seed:
        print("Using built-in SAHA seed panels …")
        records = list(SEED_PANELS)
    else:
        csv_path = Path(args.input)
        if not csv_path.exists():
            sys.exit(f"ERROR: input file not found: {csv_path}\n"
                     "  Use --seed to write built-in SAHA panels instead.")
        print(f"Reading {csv_path} …")
        df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
        df.replace({"N/A": None, "": None}, inplace=True)
        records = transform_csv(df)

    print(f"  {len(records)} panel records")

    if not args.no_validate:
        schema = json.loads(schema_path.read_text())
        errors = validate_records(records, schema)
        if errors:
            print(f"\nValidation warnings ({len(errors)} issues):")
            for idx, msg in errors[:50]:
                print(f"  row {idx} ({records[idx].get('panel_name', '?')}): {msg}")
            if len(errors) > 50:
                print(f"  … and {len(errors) - 50} more")
            if args.strict:
                sys.exit(1)
        else:
            print("  All records valid.")

    write_parquet(records, Path(args.output))


if __name__ == "__main__":
    main()
