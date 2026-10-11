#!/usr/bin/env python3
"""
build_raw_manifest.py
Generate aws/raw_manifest.csv -- the reviewed control file for raw upload.

Why a manifest and not a directory walk
---------------------------------------
Raw lives in three unrelated trees with three different shapes, and nothing
derives `sample_id` from their paths reliably:

    /athena/project-saha/data/<sample_id>/       one dir per acquisition -- resolvable
    /athena/project-saha/data_TAP/<organ>/...    by organ, depth varies per run
    /athena/project-saha/data_IBD/<panel>/       by panel; RDS only, no raw exists

A walk would therefore invent sample ids for the TAP tree, which is the same
irregularity that made stitch_tap_images.py necessary. So every row is emitted
with `include` FALSE unless its source directory name matches a sample_id in the
release manifest exactly. Rows a human has not confirmed do not upload.

Sizes are not re-measured: they come from the per-component survey already run
on the cluster (slurm 3391556, 21 min over 3.61 TB / 2.67M files). Re-walking
lustre to recompute numbers that exist is waste.

What is in scope, per the decisions recorded in docs/FILE_FORMATS.md:
  flatFiles/   yes -- small, directly useful
  RawFiles/    yes -- the bulk; note that publishing it duplicates the pixels the
               stitched morphology in the objects was derived from
  TileDB/      yes -- AtoMx vendor store, only readable with NanoString's stack;
               included for completeness, flagged in the note
  _rootfiles   yes -- H&E .tif and Seurat .rds sitting at the acquisition root

Usage:
    python scripts/build_raw_manifest.py --sizes raw_sizes.tsv [--out aws/raw_manifest.csv]
"""

import argparse
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parent.parent

# Components to publish, and what to say about each in the manifest.
COMPONENT_NOTE = {
    "flatFiles": "expression, metadata, polygons, transcripts",
    "RawFiles": "per-FOV TIFFs; the stitched morphology in the objects derives "
                "from these, so both publish the same pixels",
    "TileDB": "AtoMx vendor store; readable only with NanoString's stack",
    "_rootfiles": "H&E .tif and/or Seurat .rds at the acquisition root",
    "CellStatsDir": "per-FOV cell statistics",
    "AnalysisResults": "vendor analysis outputs",
}


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sizes", type=Path, required=True,
                    help="raw_sizes.tsv from the cluster survey")
    ap.add_argument("--samples", type=Path, default=REPO / "data/samples.parquet")
    ap.add_argument("--out", type=Path, default=REPO / "aws/raw_manifest.csv")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    sizes = pd.read_csv(args.sizes, sep="\t")
    man = pd.read_parquet(args.samples)

    # run_id and institution come from the manifest, never from the path.
    meta = man.set_index("sample_id")[
        ["platform", "generation_institution", "run_id"]].to_dict("index")

    rows = []
    for r in sizes.itertuples(index=False):
        acq, comp = r.acquisition, r.component
        info = meta.get(acq)
        if info is not None:
            prefix = (f"raw/{info['platform']}/{info['generation_institution']}"
                      f"/{info['run_id']}/{acq}/"
                      + ("" if comp == "_rootfiles" else f"{comp}/"))
            include, note = True, COMPONENT_NOTE.get(comp, "")
        else:
            # Not a release sample_id. The TAP tree is organised by organ, so its
            # directory names are organ codes, not acquisitions; resolving them
            # needs the FOV_Metadata reconciliation, not a path parse.
            prefix = ""
            include = False
            note = ("UNRESOLVED: directory name is not a sample_id in "
                    "samples.parquet. TAP is organised by organ; map to "
                    "acquisitions before including."
                    if r.tree == "data_TAP" else
                    "no raw exists for this cohort (Seurat RDS and H&E only)"
                    if r.tree == "data_IBD" else
                    "UNRESOLVED: not in samples.parquet")
        rows.append({
            "sample_id": acq if info is not None else "",
            "source_tree": r.tree,
            "source_path": f"/athena/project-saha/{r.tree}/{acq}"
                           + ("" if comp == "_rootfiles" else f"/{comp}"),
            "component": comp,
            "s3_prefix": prefix,
            "bytes": int(r.bytes),
            "n_files": int(r.n_files),
            "include": include,
            "note": note,
        })

    out = pd.DataFrame(rows).sort_values(
        ["include", "source_tree", "sample_id", "component"],
        ascending=[False, True, True, True])

    inc = out[out.include]
    print(f"  {len(out)} rows, {len(inc)} included, {len(out) - len(inc)} held back")
    print(f"  included: {inc.bytes.sum()/1e12:.2f} TB, {inc.n_files.sum():,} files, "
          f"{inc.sample_id.nunique()} acquisitions")
    held = out[~out.include]
    if len(held):
        print(f"  held back: {held.bytes.sum()/1e12:.2f} TB, {held.n_files.sum():,} files")
        for t, g in held.groupby("source_tree"):
            print(f"      {t}: {len(g)} rows, {g.bytes.sum()/1e12:.2f} TB")
    print("\n  by component (included):")
    for c, g in inc.groupby("component"):
        print(f"      {c:14s} {g.bytes.sum()/1e9:9.1f} GB  {g.n_files.sum():>9,} files")

    # A manifest that silently includes nothing is worse than no manifest.
    assert len(inc) > 0, "nothing resolved -- check that --samples matches the survey"

    if args.dry_run:
        print("\n[dry-run] nothing written")
        return 0
    args.out.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out, index=False)
    print(f"\nwrote {args.out}")
    print("  REVIEW `include` before uploading; rows default to False when the "
          "source directory could not be resolved to a release sample.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
