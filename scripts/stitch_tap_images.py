#!/usr/bin/env python3
"""
stitch_tap_images.py
Drive stitch_cosmx_images.py across the TAP acquisitions.

data_TAP is not laid out like /athena/project-saha/data: there is no
<sample_id>/ directory, the flat files and the raw morphology live in separate
subtrees (Protein/<organ>/flatFiles/... vs Protein/RawData/<organ>/...), and the
nesting depth varies per run. So stitch_cosmx_images.py cannot discover anything
here and needs both --metadata and --cellstats handed to it explicitly.

The mapping is already recorded in zenodo/tap_audit/tap_crosswalk.csv, which was
built when the acquisitions were first matched to their exports:

    raw_morphology_dir  -> the CellStatsDir holding the per-FOV tiles
    run_tissue_name     -> names the flat-file metadata, <name>_metadata_file.csv.gz

Two acquisitions are absent from that column on purpose and are skipped here:

    SAHA_CP_APE_A9_TAP03   protein half was never flat-file exported; the object
                           was rebuilt from RDS, and the APE CellStatsDir belongs
                           to the RNA run
    SAHA_CP_COL_A1_TAP01   data_TAP/COL/CellStatsDir exists but contains no
                           Morphology2D; the raw imagery for this run is not
                           usable, which matches its being the one object in the
                           release holding background-subtracted intensities
                           rather than raw counts

Usage:
    P=~/.venv-sd08/bin/python
    $P scripts/stitch_tap_images.py --downsample 4 \\
        --out-dir /athena/project-saha/derived/build/images/d4/tap --dry-run
    $P scripts/stitch_tap_images.py --downsample 1 \\
        --out-dir /athena/project-saha/derived/build/images/full/tap
"""

import argparse
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
CROSSWALK = REPO / "zenodo" / "tap_audit" / "tap_crosswalk.csv"
DATA_TAP = Path("/athena/project-saha/data_TAP")
STITCH = Path(__file__).resolve().parent / "stitch_cosmx_images.py"


def resolve(root: Path, row) -> "tuple[Path, Path] | None":
    """(metadata, cellstats) for one acquisition, or None if either is missing.

    `raw_morphology_dir` records the run directory, not the CellStatsDir inside
    it. Descend when that subdirectory is present: both tile layouts live below
    it -- `Morphology2D/*_F###.TIF` on newer exports, `FOV###/` on older ones --
    and pointing a level too high finds the FOV offsets but no morphology, which
    fails only after stitching every FOV into an empty canvas.
    """
    cellstats = root / str(row.raw_morphology_dir)
    if (cellstats / "CellStatsDir").is_dir():
        cellstats = cellstats / "CellStatsDir"
    if not cellstats.is_dir():
        print(f"    no CellStatsDir at {cellstats}")
        return None
    # Three tile layouts occur across these runs, and all three are valid:
    #   Morphology2D/*_F###.TIF   newer exports
    #   FOV###/                   older per-FOV directories
    #   *_F###.TIF                flat, straight in CellStatsDir (the runs the
    #                             crosswalk marks morphology_layout=flat)
    if not ((cellstats / "Morphology2D").is_dir()
            or any(cellstats.glob("FOV*"))
            or any(cellstats.glob("*.TIF"))):
        print(f"    no morphology tiles under {cellstats}")
        return None
    name = str(row.run_tissue_name)
    hits = sorted(root.rglob(f"{name}_metadata_file.csv.gz"))
    if not hits:
        print(f"    no {name}_metadata_file.csv.gz under {root}")
        return None
    if len(hits) > 1:
        # Several runs can share a tissue name; prefer the one under the same
        # top-level subtree as the morphology, so protein and RNA do not cross.
        top = str(row.raw_morphology_dir).split("/")[0]
        same = [h for h in hits if top in str(h)]
        hits = same or hits
    return hits[0], cellstats


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--crosswalk", type=Path, default=CROSSWALK)
    ap.add_argument("--root", type=Path, default=DATA_TAP)
    ap.add_argument("--out-dir", type=Path, required=True)
    ap.add_argument("--downsample", type=int, default=4)
    ap.add_argument("--python", default=sys.executable)
    ap.add_argument("--only", nargs="*")
    ap.add_argument("--skip-existing", action="store_true", default=True)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    import pandas as pd
    d = pd.read_csv(args.crosswalk).drop_duplicates("sample_id")
    d = d[d.raw_morphology_dir.notna()]
    if args.only:
        d = d[d.sample_id.isin(args.only)]
    print(f"{len(d)} acquisition(s) with recorded morphology\n")

    ok = skipped = failed = 0
    for row in d.itertuples():
        out = args.out_dir / f"{row.sample_id}.zarr"
        if args.skip_existing and out.exists():
            print(f"  {row.sample_id:26s} SKIP (already stitched)")
            skipped += 1
            continue
        print(f"  {row.sample_id}")
        paths = resolve(args.root, row)
        if paths is None:
            failed += 1
            continue
        metadata, cellstats = paths
        cmd = [args.python, "-u", str(STITCH),
               "--sample", row.sample_id,
               "--metadata", str(metadata),
               "--cellstats", str(cellstats),
               "--out-dir", str(args.out_dir),
               "--downsample", str(args.downsample),
               "--no-labels"]          # no CellLabels anywhere in data_TAP
        if args.dry_run:
            print(f"    metadata  {metadata}")
            print(f"    cellstats {cellstats}")
            continue
        r = subprocess.run(cmd, capture_output=True, text=True)
        tail = [l for l in r.stdout.splitlines()
                if l.strip() and "no parent found" not in l][-2:]
        for l in tail:
            print(f"    {l.strip()}")
        if r.returncode != 0:
            print(f"    FAILED rc={r.returncode}: "
                  f"{(r.stderr or r.stdout).strip().splitlines()[-1][:160]}")
            failed += 1
        else:
            ok += 1

    print(f"\n{ok} stitched, {skipped} skipped, {failed} failed"
          + ("   [dry-run]" if args.dry_run else ""))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
