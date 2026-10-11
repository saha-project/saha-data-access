#!/usr/bin/env python3
"""
release_tally.py
Write zenodo/release_breakdown.csv -- the one tally of what the release contains.

There used to be three: release_breakdown.csv (v1 only), tap_release_breakdown.csv
(TAP only) and v1_combined_breakdown.csv (both). They drifted, as parallel
hand-maintained tallies do -- the combined one carried 27,298 cells for
SAHA_CP_COL_A1_TAP01 against the object's real 101,618, and its release total was
74,320 cells short. This derives the tally from samples.parquet instead, so there
is one place to be wrong and the validator can check it.

Cell counts come from samples.parquet, which is measured off the built objects:

    n_cells          every cell the instrument segmented -- what is IN the file
    n_cells_qc_pass  the subset the published h5ads kept, matching obs qc_pass

Those differ by up to 25,203 cells on a single CosMx sample. Reporting the
qc_pass number as the file's cell count, which the old manifests did, means a
downloader's count never matches the documentation.

Sizes are read from the packaged archives when they exist; rerun after
package_for_zenodo.py to pick up real sizes.

Usage:
    python scripts/release_tally.py
    python scripts/release_tally.py --sizes zenodo/combined_manifest.csv
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import donor_map
from package_for_zenodo import record_slug

REPO = Path(__file__).resolve().parent.parent
ZENODO_CAP_GB = 50


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--samples", type=Path, default=REPO / "data/samples.parquet")
    ap.add_argument("--sizes", type=Path,
                    help="CSV with sample_id + size_bytes (a packaging manifest)")
    ap.add_argument("--out", type=Path, default=REPO / "zenodo/release_breakdown.csv")
    ap.add_argument("--record-metadata", type=Path,
                    default=REPO / "zenodo/record_metadata.json",
                    help="also refresh the _proposed_split block in this file")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    s = pd.read_parquet(args.samples)
    cores = donor_map.sample_cores()

    s["record"] = s.apply(record_slug, axis=1)
    s["donor_cores"] = s.sample_id.map(
        lambda x: ";".join(donor_map.cores_for(x, cores)))

    gb = {}
    for src in filter(None, [args.sizes]):
        if src.exists():
            d = pd.read_csv(src)
            col = "size_bytes" if "size_bytes" in d.columns else None
            if col:
                gb = dict(zip(d.sample_id, d[col] / 1e9))
    s["gb"] = s.sample_id.map(gb)

    cols = ["record", "sample_id", "platform", "assay_type", "organ", "condition",
            "n_cells", "n_cells_qc_pass", "n_donors", "donor_cores", "n_fovs", "gb"]
    out = s[cols].sort_values(["record", "sample_id"])

    by = out.groupby("record").agg(
        acquisitions=("sample_id", "size"),
        cells=("n_cells", "sum"),
        donors=("donor_cores", lambda x: len({c for v in x for c in v.split(";") if c})),
        gb=("gb", "sum"))
    by["over_cap"] = by.gb > ZENODO_CAP_GB

    print(by.to_string())
    all_cores = {c for v in out.donor_cores for c in v.split(";") if c}
    print(f"\n{len(out)} acquisitions, {int(out.n_cells.sum()):,} cells, "
          f"{len(all_cores)} donor cores, {out.gb.sum():.1f} GB, "
          f"{out.record.nunique()} records")
    qc = out.n_cells_qc_pass.notna()
    if qc.any():
        print(f"{int(qc.sum())} sample(s) carry a QC-filtered subset: "
              f"{int(out.loc[qc, 'n_cells_qc_pass'].sum()):,} of "
              f"{int(out.loc[qc, 'n_cells'].sum()):,} cells")
    if by.over_cap.any():
        print(f"\nWARNING: over Zenodo's {ZENODO_CAP_GB} GB cap: "
              f"{by[by.over_cap].index.tolist()}")

    if args.dry_run:
        print("\n[dry-run] nothing written")
        return
    out.to_csv(args.out, index=False)
    print(f"\nwrote {args.out}")

    if args.record_metadata and args.record_metadata.exists():
        update_record_metadata(args.record_metadata, out, by)
        print(f"wrote {args.record_metadata} (_proposed_split)")


def title_for(slug: str) -> str:
    name = slug.replace("saha-v1-", "").replace("-", " ")
    name = {"lymph node": "lymph node", "bone marrow": "bone marrow",
            "crc": "colorectal cancer",
            "pancreas cosmx": "pancreas (CosMx)",
            "pancreas xenium": "pancreas (Xenium)"}.get(name, name)
    return f"Spatial Atlas of Human Anatomy (SAHA): {name}, v1.0"


def update_record_metadata(path: Path, out: pd.DataFrame, by: pd.DataFrame) -> None:
    """Regenerate _proposed_split from the tally rather than maintaining it by hand.

    The hand-written block described 7 records and 9,324,897 cells long after
    the release became 11 records; each number in it was a separate chance to
    publish something untrue.
    """
    import json
    d = json.loads(path.read_text())
    children = []
    for slug, g in out.groupby("record"):
        # CosMx archives live on Cayuga, Xenium on the workstation.
        on = g.platform.value_counts().to_dict()
        children.append({
            "slug": slug,
            "platform": "+".join(sorted(g.platform.unique())),
            "organ_group": sorted(g.organ.unique())[0],
            "n_samples": int(len(g)),
            "measured_gb": round(float(g.gb.sum()), 1),
            "n_cells": int(g.n_cells.sum()),
            "n_donor_cores": len({c for v in g.donor_cores for c in v.split(";") if c}),
            "has_images": True,
            "files_on": {k: int(v) for k, v in
                         {"cluster": on.get("cosmx", 0),
                          "local": on.get("xenium", 0)}.items() if v},
            "title": title_for(slug),
        })
    d["_proposed_split"]["children"] = children
    d["_proposed_split"]["totals"] = {
        "n_records": int(out.record.nunique()),
        "n_samples": int(len(out)),
        "measured_gb": round(float(out.gb.sum()), 1),
        "n_cells": int(out.n_cells.sum()),
        "n_donor_cores": len({c for v in out.donor_cores for c in v.split(";") if c}),
        "cosmx_images_zenodo_gb": 52.0,
        "cosmx_images_awsfull_gb": 623.0,
    }
    d["_proposed_split"]["_generated_by"] = (
        "scripts/release_tally.py -- do not hand-edit; rerun after repackaging")
    path.write_text(json.dumps(d, indent=2, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
