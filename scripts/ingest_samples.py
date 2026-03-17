#!/usr/bin/env python3
"""
ingest_samples.py
Map sample_metadata.csv → samples.parquet, validating against schemas/samples.json.

Usage:
    python scripts/ingest_samples.py \
        --input  sample_metadata.csv \
        --output data/samples.parquet \
        --schema schemas/samples.json

Requirements:
    pip install pandas pyarrow jsonschema
"""

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from jsonschema import Draft202012Validator, ValidationError

# ---------------------------------------------------------------------------
# Lookup tables derived from the SAHA manifest conventions
# ---------------------------------------------------------------------------

# Keys are lowercase — the callers do .strip().lower() before lookup.
PLATFORM_MAP = {
    "cosmx rna":     ("cosmx", "rna"),
    "cosmx protein": ("cosmx", "protein"),
    "cosmx":         ("cosmx", "rna"),       # bare "cosmx" fallback
    "xenium":        ("xenium", "rna"),
    "g4x":           ("g4x", "rna"),
    "geomx":         ("geomx", "rna"),
    "rnascope":      ("rnascope", "rna"),
}

INSTITUTION_MAP = {
    "tap":      "cornell",
    "wcm":      "wcm",
    "st.jude":  "st_jude",
    "msk-sail": "msk",
    "msk":      "msk",
    "broad":    "broad",
}

ORGAN_MAP = {
    "appendix":    "appendix",
    "bone marrow": "bone_marrow",
    "colon":       "colon",
    "ileum":       "ileum",
    "kidney":      "kidney",
    "liver":       "liver",
    "lung":        "lung",
    "lymph node":  "lymph_node",
    "pancreas":    "pancreas",
    "prostate":    "prostate",
    "spleen":      "spleen",
    "stomach":     "stomach",
    "thymus":      "thymus",
}

CONDITION_MAP = {
    "normal":  "normal",
    "cancer":  "cancer",
    "disease": "disease",
}

# Slide ID suffix → donor_id.
# Slide ID format: {ORGAN}_{DONOR_CODE} (e.g. APE_A9, PANC_CA, IBD_A1).
# Map only the part after the first underscore (or the full value for IBD slides).
SLIDE_ID_DONOR_MAP = {
    "A1":     "SAHA_D001",
    "A9":     "SAHA_D002",
    "CA":     "SAHA_D003",   # PANC_CA → PDAC patient
    "IBD_A1": "SAHA_D004",
    "IBD_A2": "SAHA_D005",
}


def donor_from_slide_id(slide_id: object) -> "str | None":
    """Return donor_id from a Slide ID string (e.g. 'APE_A9' → 'SAHA_D002')."""
    if slide_id is None:
        return None
    s = str(slide_id).strip()
    if not s or s in ("", "nan", "N/A"):
        return None
    parts = s.split("_", 1)
    if len(parts) < 2:
        return None
    organ, suffix = parts[0], parts[1]
    if organ == "IBD":
        return SLIDE_ID_DONOR_MAP.get(f"IBD_{suffix}")
    return SLIDE_ID_DONOR_MAP.get(suffix)


# Panel plex is extracted from the canonical panel name.
# Extend this dict when new panels are added.
PANEL_PLEX_MAP = {
    "ucc 1k":    1000,
    "6k discovery": 6000,
    "19k":       19000,
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def parse_date(value: object) -> "str | None":
    """Convert MM/DD/YYYY strings (and variants) to ISO 8601 YYYY-MM-DD."""
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


def normalize_str(value: object, mapping: dict) -> "str | None":
    """Lower-strip lookup in a mapping dict; returns None on miss."""
    if value is None:
        return None
    key = str(value).strip().lower()
    return mapping.get(key)


def parse_panel_plex(panel_name: object) -> "int | None":
    """Extract plex count from a panel name string."""
    if panel_name is None:
        return None
    name = str(panel_name).strip()

    # Exact/prefix match in lookup table first
    key = name.lower()
    if key in PANEL_PLEX_MAP:
        return PANEL_PLEX_MAP[key]
    for k, v in PANEL_PLEX_MAP.items():
        if k in key:
            return v

    # Pattern: "(377-plex)" or "377-plex"
    m = re.search(r"\((\d+)-plex\)", name)
    if m:
        return int(m.group(1))
    m = re.search(r"(\d+)-plex", name)
    if m:
        return int(m.group(1))

    # Pattern: "19K" or "6K"
    m = re.search(r"(\d+)[Kk]", name)
    if m:
        return int(m.group(1)) * 1000

    return None


def safe_int(value: object) -> "int | None":
    """Cast to int, returning None for blanks / 'N/A'."""
    if value is None:
        return None
    s = str(value).strip()
    if s in ("", "N/A", "nan", "NaN"):
        return None
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return None


def safe_bool(value: object) -> "bool | None":
    """Interpret 'Y'/'y' as True, blank/NaN as None, anything else as False."""
    if value is None:
        return None
    s = str(value).strip()
    if s in ("", "nan", "NaN"):
        return None
    return s.upper() == "Y"


# ---------------------------------------------------------------------------
# Core transform
# ---------------------------------------------------------------------------

def transform(df: pd.DataFrame) -> list[dict]:
    """Map raw manifest columns to the samples schema."""
    records = []

    for _, row in df.iterrows():
        folder_name = str(row.get("Folder Name", "")).strip()
        if not folder_name:
            continue  # skip blank rows

        # Skip samples not cleared for public release
        release_status = str(row.get("release_status", "")).strip().lower()
        if release_status.startswith("excluded"):
            continue

        assay_raw = str(row.get("Assay", "")).strip()
        platform_key = assay_raw.lower()
        platform, assay_type = PLATFORM_MAP.get(platform_key, (None, None))

        organ_raw = str(row.get("Organ", "")).strip()
        organ = normalize_str(organ_raw, ORGAN_MAP) or "other"

        condition_raw = str(row.get("State", "")).strip()
        condition = normalize_str(condition_raw, CONDITION_MAP) or "other"

        institution_raw = str(row.get("Run location", "")).strip()
        generation_institution = (
            INSTITUTION_MAP.get(institution_raw.lower()) or "other"
        )

        panel_name = str(row.get("Panel", "")).strip() or None
        panel_plex = parse_panel_plex(panel_name)

        record = {
            "sample_id":               folder_name,
            "run_id":                  str(row.get("Batch", "")).strip() or None,
            "platform":                platform,
            "assay_type":              assay_type,
            "generation_institution":  generation_institution,
            "tissue_source_institution": None,         # not captured in source CSV
            "organ":                   organ,
            "condition":               condition,
            "anatomical_region":       str(row.get("Tissue type", "")).strip() or None,
            "donor_id":                donor_from_slide_id(row.get("Slide ID")),
            "slide_id":                str(row.get("Slide ID", "")).strip() or None,
            "panel_name":              panel_name,
            "panel_plex":              panel_plex,
            "n_cells":                 None,            # not in source CSV
            "n_fovs":                  safe_int(row.get("#")),
            "qc_pass":                 safe_bool(row.get("Wasabi")),
            "processing_date":         parse_date(row.get("Run start date")),
            "slide_prepared_date":     parse_date(row.get("Slide prepared")),
            "s3_raw_prefix":           None,
            "s3_processed_path":       None,
            "on_wasabi":               safe_bool(row.get("Wasabi")),
        }
        records.append(record)

    return records


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_records(records: list[dict], schema: dict) -> list[tuple[int, str]]:
    """Run JSON Schema validation; return list of (row_index, error_message)."""
    validator = Draft202012Validator(schema)
    errors = []
    for i, rec in enumerate(records):
        for err in validator.iter_errors(rec):
            errors.append((i, f"{err.json_path}: {err.message}"))
    return errors


# ---------------------------------------------------------------------------
# Parquet output
# ---------------------------------------------------------------------------

# Explicit PyArrow schema keeps column order and types deterministic.
PARQUET_SCHEMA = pa.schema([
    ("sample_id",               pa.string()),
    ("run_id",                  pa.string()),
    ("platform",                pa.string()),
    ("assay_type",              pa.string()),
    ("generation_institution",  pa.string()),
    ("tissue_source_institution", pa.string()),
    ("organ",                   pa.string()),
    ("condition",               pa.string()),
    ("anatomical_region",       pa.string()),
    ("donor_id",                pa.string()),
    ("slide_id",                pa.string()),
    ("panel_name",              pa.string()),
    ("panel_plex",              pa.int32()),
    ("n_cells",                 pa.int64()),
    ("n_fovs",                  pa.int32()),
    ("qc_pass",                 pa.bool_()),
    ("processing_date",         pa.string()),
    ("slide_prepared_date",     pa.string()),
    ("s3_raw_prefix",           pa.string()),
    ("s3_processed_path",       pa.string()),
    ("on_wasabi",               pa.bool_()),
])


def write_parquet(records: list[dict], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(records)

    # Ensure all schema columns are present (fill missing with None)
    for field in PARQUET_SCHEMA:
        if field.name not in df.columns:
            df[field.name] = None

    table = pa.Table.from_pandas(
        df[[f.name for f in PARQUET_SCHEMA]],
        schema=PARQUET_SCHEMA,
        preserve_index=False,
    )

    # write_to_dataset supports Hive-style partitioning (organ=.../platform=...);
    # write_table writes a single flat file — choose based on output_path type.
    if output_path.suffix == ".parquet":
        pq.write_table(table, output_path, compression="snappy")
    else:
        # Treat output_path as a directory root for partitioned dataset
        import pyarrow.dataset as ds
        ds.write_dataset(
            table,
            base_dir=str(output_path),
            format="parquet",
            partitioning=ds.partitioning(
                pa.schema([("organ", pa.string()), ("platform", pa.string())]),
                flavor="hive",
            ),
            file_options=ds.ParquetFileFormat().make_write_options(compression="snappy"),
            existing_data_behavior="overwrite_or_ignore",
        )
    print(f"Wrote {len(records)} rows to {output_path}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--input",  default="sample_metadata.csv",
                   help="Path to source CSV (default: sample_metadata.csv)")
    p.add_argument("--output", default="data/samples.parquet",
                   help="Output Parquet path (default: data/samples.parquet)")
    p.add_argument("--schema", default="schemas/samples.json",
                   help="JSON Schema path (default: schemas/samples.json)")
    p.add_argument("--no-validate", action="store_true",
                   help="Skip JSON Schema validation step")
    p.add_argument("--strict", action="store_true",
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
    print(f"  {len(records)} records produced (skipped {len(df) - len(records)} blank rows)")

    if not args.no_validate:
        schema = json.loads(schema_path.read_text())
        errors = validate_records(records, schema)
        if errors:
            print(f"\nValidation warnings ({len(errors)} issues):")
            for idx, msg in errors[:50]:
                print(f"  row {idx} ({records[idx].get('sample_id', '?')}): {msg}")
            if len(errors) > 50:
                print(f"  … and {len(errors) - 50} more")
            if args.strict:
                sys.exit(1)
        else:
            print("  All records valid.")

    write_parquet(records, Path(args.output))


if __name__ == "__main__":
    main()
