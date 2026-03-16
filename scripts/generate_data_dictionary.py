#!/usr/bin/env python3
"""
generate_data_dictionary.py
Auto-generate docs/DATA_DICTIONARY.md from the four JSON Schema files.

Usage:
    python scripts/generate_data_dictionary.py \
        --schemas-dir schemas/ \
        --output     docs/DATA_DICTIONARY.md
"""

import argparse
import json
from pathlib import Path
from datetime import date

TABLE_ORDER = ["samples", "donors", "runs", "panels"]

# Which tables correspond to Parquet partitioning
PARTITION_INFO = {
    "samples": "Partitioned by `organ` and `platform`.",
}

PREAMBLE = """\
# DATA_DICTIONARY.md

> Auto-generated from `schemas/*.json` on {date}. Edit the JSON Schema
> files to update this document, then re-run
> `python scripts/generate_data_dictionary.py`.

This document describes the four Parquet metadata tables that form the
SAHA spatial omics open-data catalog.  All column names use `snake_case`;
dates follow ISO 8601 (`YYYY-MM-DD`); institution codes and platform names
are lowercase per SAHA conventions.

"""

TABLE_INTRO = {
    "samples": (
        "One row per tissue sample.  "
        "Joins to `donors.parquet` on `donor_id`, to `runs.parquet` on "
        "`run_id`, and to `panels.parquet` on `panel_name`."
    ),
    "donors": (
        "One row per de-identified tissue donor.  "
        "Controlled-access columns (age, ethnicity, comorbidities) are "
        "omitted from the open tier."
    ),
    "runs": (
        "One row per instrument acquisition run (batch).  "
        "Links to `samples.parquet` via `run_id`."
    ),
    "panels": (
        "One row per panel configuration.  "
        "`gene_list` and `protein_list` are stored as Parquet list columns."
    ),
}


def type_label(prop: dict) -> str:
    """Return a compact human-readable type string."""
    t = prop.get("type")
    if isinstance(t, list):
        non_null = [x for x in t if x != "null"]
        nullable = "null" in t
        base = non_null[0] if non_null else "any"
    else:
        base = t or "any"
        nullable = False

    if base == "array":
        items_type = prop.get("items", {}).get("type", "any")
        label = f"array[{items_type}]"
    else:
        label = base

    if nullable:
        label += " | null"
    return f"`{label}`"


def format_constraints(prop: dict) -> str:
    """Produce a compact constraint string (enum values, min/max, pattern)."""
    parts = []
    if "enum" in prop:
        vals = [v for v in prop["enum"] if v is not None]
        if vals:
            parts.append("enum: " + ", ".join(f"`{v}`" for v in vals))
    if "minimum" in prop:
        parts.append(f"min={prop['minimum']}")
    if "maximum" in prop:
        parts.append(f"max={prop['maximum']}")
    if "pattern" in prop:
        parts.append(f"pattern: `{prop['pattern']}`")
    if "format" in prop:
        parts.append(f"format: {prop['format']}")
    return "; ".join(parts)


def render_table(schema: dict) -> str:
    """Render a Markdown table for one JSON Schema."""
    lines = []
    props = schema.get("properties", {})
    required = set(schema.get("required", []))

    lines.append("| Column | Type | Required | Constraints | Description |")
    lines.append("|--------|------|:--------:|-------------|-------------|")

    for name, prop in props.items():
        req = "yes" if name in required else ""
        constraints = format_constraints(prop)
        description = prop.get("description", "")
        lines.append(
            f"| `{name}` | {type_label(prop)} | {req} | {constraints} | {description} |"
        )

    return "\n".join(lines)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--schemas-dir", default="schemas",
                   help="Directory containing JSON Schema files")
    p.add_argument("--output", default="docs/DATA_DICTIONARY.md",
                   help="Output Markdown file path")
    args = p.parse_args()

    schemas_dir = Path(args.schemas_dir)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    sections = [PREAMBLE.format(date=date.today().isoformat())]

    # Table of contents
    sections.append("## Tables\n")
    for name in TABLE_ORDER:
        sections.append(f"- [{name}.parquet](#{name}parquet)\n")
    sections.append("\n---\n")

    for name in TABLE_ORDER:
        schema_path = schemas_dir / f"{name}.json"
        if not schema_path.exists():
            print(f"WARNING: {schema_path} not found — skipping")
            continue
        schema = json.loads(schema_path.read_text())

        sections.append(f"## {name}.parquet\n")
        sections.append(TABLE_INTRO.get(name, "") + "\n")

        if name in PARTITION_INFO:
            sections.append(f"\n> **Partitioning:** {PARTITION_INFO[name]}\n")

        sections.append("\n" + render_table(schema) + "\n")
        sections.append("\n---\n")

    output_path.write_text("\n".join(sections))
    print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()
