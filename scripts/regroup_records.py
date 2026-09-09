#!/usr/bin/env python3
"""
regroup_records.py
Reorganise already-built .zarr.zip archives into per-organ Zenodo records.

The v1 records are organ-based and span both platforms, so a record's files can
be split across two machines: CosMx archives live on Cayuga, Xenium archives on
the workstation. Rather than moving ~70 GB over a link that has proved
unreliable, this arranges each machine's own files into the record directories
and writes identical documentation on both. Each half is then uploaded to the
same Zenodo draft from wherever it already sits.

Pancreas is the one organ split by platform: combined it is 64.2 GB, over
Zenodo's 50 GB per-record cap, and a normal/PDAC split does not rescue it
(normal alone is 58.5 GB).

Archives are hard-linked, not copied, so regrouping costs no disk and leaves
the originals untouched.

Usage:
    # what would move, nothing written
    python scripts/regroup_records.py --manifest zenodo/combined_manifest.csv \
        --location local --src-dir ~/SAHA_zenodo --out-dir ~/SAHA_zenodo_v1 --dry-run

    python scripts/regroup_records.py --manifest zenodo/combined_manifest.csv \
        --location local --src-dir ~/SAHA_zenodo --out-dir ~/SAHA_zenodo_v1
"""

import argparse
import csv
import os
import sys
from collections import defaultdict
from pathlib import Path

# Verified by reading the stores directly. These four disease samples have no
# published h5ad to reconcile against, so samples.parquet carries no n_cells --
# see DESCRIPTION.md on qc_pass.
KNOWN_CELL_COUNTS = {
    "SAHA_CR_IBD_A1_EC04": 386896,
    "SAHA_CR_IBD_A2_EC04": 54003,
    "SAHA_CP_PDAC_RC_EC02": 293365,
    "SAHA_CR_PDAC_RC_EC02": 106718,
}


def load(manifest: Path) -> list:
    rows = list(csv.DictReader(open(manifest)))
    for r in rows:
        if not (r.get("n_cells") or "").strip():
            n = KNOWN_CELL_COUNTS.get(r["sample_id"])
            if n:
                r["n_cells"] = str(n)
    return rows


def readme(record: str, rows: list) -> str:
    gb = sum(int(r["size_bytes"]) for r in rows) / 1e9
    cells = sum(float(r["n_cells"] or 0) for r in rows)
    plats = sorted({r["platform"] for r in rows})
    organs = sorted({r["organ"] for r in rows})
    assays = sorted({r["assay_type"] for r in rows})

    lines = [
        f"# SAHA v1.0 — {record}",
        "",
        f"{len(rows)} SpatialData objects, {gb:.1f} GB, {int(cells):,} cells.",
        f"Organ: {', '.join(organs)}. Platform: {', '.join(plats)}. "
        f"Assay: {', '.join(assays)}.",
        "",
        "Part of the SAHA v1.0 SpatialData release. See the parent collection",
        "record for metadata tables, full documentation, and citation details.",
        "",
        "## Contents",
        "",
        "| file | sample | platform | assay | condition | cells | GB |",
        "|------|--------|----------|-------|-----------|-------|-----|",
    ]
    for r in sorted(rows, key=lambda x: x["file"]):
        lines.append(
            f"| `{r['file']}` | {r['sample_id']} | {r['platform']} | "
            f"{r['assay_type']} | {r['condition']} | "
            f"{int(float(r['n_cells'] or 0)):,} | "
            f"{int(r['size_bytes']) / 1e9:.2f} |"
        )

    lines += [
        "",
        "## Reading the data",
        "",
        "Each `.zip` contains one SpatialData (OME-NGFF / Zarr v3) store.",
        "Archives are uncompressed containers — zarr chunks are already compressed.",
        "",
        "```bash",
        f"unzip {sorted(r['file'] for r in rows)[0]}",
        "```",
        "",
        "```python",
        "import spatialdata as sd",
        f"sdata = sd.read_zarr(\"{sorted(r['file'] for r in rows)[0][:-4]}\")",
        "adata = sdata.tables[\"table\"]",
        "```",
        "",
        "Written with spatialdata 0.8.0 / zarr 3.3.0. Verify downloads against",
        "`MD5SUMS.txt`; per-sample detail is in `file_manifest.csv`.",
        "",
    ]
    if "cosmx" in plats:
        lines += [
            "## CosMx objects in this record",
            "",
            "Morphology images are 5-channel immunofluorescence, downsampled 4x",
            "to 0.48 um/px so the record fits Zenodo's size limit. Cell and",
            "compartment label masks are at full resolution. Full-resolution",
            "morphology (0.12 um/px) is published via AWS Open Data.",
            "",
            "`labels/cell_labels` holds a globally unique integer per cell; the",
            "obs column `label_id` carries the same value, so table rows and",
            "label pixels can be matched:",
            "",
            "```python",
            "adata.obs[\"label_id\"]        # == fov * 100000 + fov_cell_id",
            "```",
            "",
            "The label image is a superset of the table: a small fraction of",
            "labels (~0.6%) are cells the instrument segmented but dropped from",
            "the expression export, so they have no matching table row.",
            "",
            "Objects contain every segmented cell. The obs column `qc_pass`",
            "marks cells present in the published, QC-filtered h5ad — that",
            "subset is what the manuscript cell counts refer to. Samples with no",
            "published h5ad carry no `qc_pass` column.",
            "",
        ]
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--manifest", type=Path, required=True)
    ap.add_argument("--location", required=True, choices=["local", "cluster"],
                    help="which machine's files to arrange")
    ap.add_argument("--src-dir", type=Path, action="append", required=True,
                    help="directory holding the existing .zarr.zip files (repeatable)")
    ap.add_argument("--out-dir", type=Path, required=True)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    rows = load(args.manifest)
    by_record = defaultdict(list)
    for r in rows:
        by_record[r["record"]].append(r)

    # index every archive present on this machine
    have = {}
    for d in args.src_dir:
        for p in d.rglob("*.zarr.zip"):
            have[p.name] = p

    print(f"{len(rows)} archives in the manifest, {len(have)} present here\n")

    missing_here = []
    for record in sorted(by_record):
        rec_rows = by_record[record]
        mine = [r for r in rec_rows if r["location"] == args.location]
        gb = sum(int(r["size_bytes"]) for r in rec_rows) / 1e9
        found = [r for r in mine if r["file"] in have]
        missing_here += [r["file"] for r in mine if r["file"] not in have]
        other = len(rec_rows) - len(mine)
        print(f"  {record:22s} {len(rec_rows):2d} files {gb:5.1f} GB | "
              f"here {len(found)}/{len(mine)}"
              + (f", {other} on the other machine" if other else ""))

    if missing_here:
        print(f"\nWARNING: {len(missing_here)} archive(s) expected here but not found:")
        for f in missing_here[:10]:
            print(f"  {f}")

    if args.dry_run:
        print("\n[dry-run] nothing written")
        return

    args.out_dir.mkdir(parents=True, exist_ok=True)
    for record in sorted(by_record):
        rec_rows = sorted(by_record[record], key=lambda r: r["file"])
        rec_dir = args.out_dir / record
        mine = [r for r in rec_rows if r["location"] == args.location
                and r["file"] in have]
        if not mine:
            continue                      # this record has nothing on this machine
        rec_dir.mkdir(parents=True, exist_ok=True)

        for r in mine:
            dst = rec_dir / r["file"]
            if dst.exists():
                continue
            try:
                os.link(have[r["file"]], dst)          # hard link: no extra disk
            except OSError:
                dst.symlink_to(have[r["file"]])

        # Documentation describes the whole record, including the half that
        # lives on the other machine, so both copies are identical.
        (rec_dir / "README.md").write_text(readme(record, rec_rows))
        with open(rec_dir / "file_manifest.csv", "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=[
                "file", "sample_id", "platform", "assay_type", "organ",
                "condition", "donor_id", "n_cells", "size_bytes", "md5"])
            w.writeheader()
            for r in rec_rows:
                w.writerow({k: r[k] for k in w.fieldnames})
        with open(rec_dir / "MD5SUMS.txt", "w") as fh:
            for r in rec_rows:
                fh.write(f"{r['md5']}  {r['file']}\n")

        print(f"  {record:22s} {len(mine)} archive(s) linked, docs written")

    print(f"\nwrote {args.out_dir}")


if __name__ == "__main__":
    main()
