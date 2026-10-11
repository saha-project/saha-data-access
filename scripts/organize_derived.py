#!/usr/bin/env python3
"""
organize_derived.py
Reorganise /athena/project-saha/derived so the release is a directory you can
read, and upload, as is.

The directory grew one name at a time (zenodo_objects, tap_aws, xenium_zarr, ...),
so it read as a history of the work: nine sibling folders held the 130 release
objects, and as many again held debris. Target layout, grouped by what a thing IS:

    derived/
      release/                 exactly what gets published
        s3/                    = s3://saha-open-data/ (v1)
          processed/<record_slug>/<store>.zarr/    126 objects, full resolution
          raw/<platform>/<institution>/<run>/<sample>/   symlinks into data/
        s3_v2/                 = the v2 queue, same shape
          processed/saha-v2-<organ>/<store>.zarr/
          raw/...              symlinks into 2026_SAHA_data/
        zenodo/                the 13 Zenodo records (zips + manifests)
      build/                   inputs, not published as they are
        zenodo_tier/<record_slug>/<store>.zarr     49 4x CosMx objects; the 77
                               shared Dulai/Xenium objects are symlinks into
                               release/s3 so this tree is the complete Zenodo set
        images/{d4,full}/{v1_cosmx,tap,prca}       stitched morphology inputs
        provenance/            donor_qc, obs_dropped, v2_obs_dropped
      _archive/                superseded trees and debris, kept until release
      logs/

`release/s3/` mirrors the bucket key for key, so the upload is
`aws s3 sync release/s3/ s3://saha-open-data/` (raw/ symlinks are followed).
metadata/ and docs/ are not mirrored: they are generated from the repo at
upload time by upload_to_s3.py.

Everything is a rename within one filesystem -- instant, and reversible -- or a
new symlink. Nothing is deleted except a source directory left EMPTY by its
own objects moving out (os.rmdir refuses anything else). Membership comes from
samples.parquet + record_slug() and from the raw manifests, never from parsing
directory names.

Usage (on Cayuga):
    SAHA_REF=.../release_qc/ref python scripts/organize_derived.py --dry-run
    SAHA_REF=.../release_qc/ref python scripts/organize_derived.py
"""

import argparse
import os
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from package_for_zenodo import record_slug, sample_id_of  # noqa: E402

REPO = Path(os.environ.get("SAHA_REF", Path(__file__).resolve().parent.parent))
ROOT = Path("/athena/project-saha/derived")

AWS_TIER = ["aws_objects", "tap_aws", "prca_aws"]          # full-res CosMx
ZENODO_TIER = ["zenodo_objects", "tap_zarr", "prca_zarr"]  # 4x CosMx
SHARED = ["dulai_zarr", "xenium_zarr"]                     # one store, both tiers

WHOLE = {   # old dir -> new path; moved as a unit
    "v2_objects": "release/s3_v2/processed/saha-v2-prostate",
    "zenodo_upload_v2": "release/zenodo",
    "images_d4": "build/images/d4/v1_cosmx", "tap_images_d4": "build/images/d4/tap",
    "prca_images_d4": "build/images/d4/prca", "images_full": "build/images/full/v1_cosmx",
    "tap_images_full": "build/images/full/tap", "prca_images_full": "build/images/full/prca",
    "donor_qc": "build/provenance/donor_qc", "obs_dropped": "build/provenance/obs_dropped",
    "v2_obs_dropped": "build/provenance/v2_obs_dropped",
    "_replaced": "_archive/replaced_2026-10", "_replaced_4x": "_archive/replaced_4x_elements",
    "zenodo_upload": "_archive/zenodo_upload_2026-08",
    "zenodo_upload_v1": "_archive/zenodo_upload_v1_2026-09",
    "zenodo_upload_v2_staging": "_archive/staging/zenodo_upload_v2_staging",
    "zenodo_upload_v2_staging_leiden": "_archive/staging/zenodo_upload_v2_staging_leiden",
    "zenodo_upload_v2_staging_rectum": "_archive/staging/zenodo_upload_v2_staging_rectum",
    "scratch": "_archive/scratch", "dulai_export": "_archive/empty/dulai_export",
    "dulai_zarr_fixed": "_archive/empty/dulai_zarr_fixed",
    "v2_objects_fixed": "_archive/empty/v2_objects_fixed",
}
KEEP = {"logs", "release", "build", "_archive"}


def plan():
    s = pd.read_parquet(REPO / "data/samples.parquet")
    slug = dict(zip(s.sample_id, s.apply(record_slug, axis=1)))
    v2 = pd.read_parquet(REPO / "data/v2/samples.parquet")
    assert set(v2.organ) == {"prostate"}, "v2 slug below assumes one organ"

    moves, links, problems = [], [], []

    def stores(dirs):
        out = {}
        for d in dirs:
            for z in sorted((ROOT / d).glob("*.zarr")):
                sid = sample_id_of(z)
                if sid in out:
                    problems.append(f"{sid}: in {out[sid].parent.name} and {d}")
                out[sid] = z
        return out

    aws, zen, shared = stores(AWS_TIER), stores(ZENODO_TIER), stores(SHARED)
    for sid in sorted(slug):
        a = aws.get(sid) or shared.get(sid)
        if a is None:
            problems.append(f"{sid}: no release store")
            continue
        moves.append((a, ROOT / "release/s3/processed" / slug[sid] / a.name))
        if sid in zen:
            moves.append((zen[sid], ROOT / "build/zenodo_tier" / slug[sid] / zen[sid].name))
        elif sid in shared:
            link = ROOT / "build/zenodo_tier" / slug[sid] / a.name
            links.append((link, Path("../../../release/s3/processed") / slug[sid] / a.name))
        else:
            problems.append(f"{sid}: no Zenodo-tier store")
    extra = (set(aws) | set(zen) | set(shared)) - set(slug)
    problems += [f"{x}: store exists but not in samples.parquet" for x in sorted(extra)]

    for old, new in WHOLE.items():
        if (ROOT / old).exists():
            moves.append((ROOT / old, ROOT / new))

    # raw: one symlink per manifest row, at the S3 key it will be uploaded to
    for man, tree in (("aws/raw_manifest.csv", "release/s3"), ("aws/raw_manifest_v2.csv", "release/s3_v2")):
        r = pd.read_csv(REPO / man)
        for row in r[r.include].itertuples():
            key, src = row.s3_prefix.rstrip("/"), Path(row.source_path)
            if row.component == "_rootfiles":     # the loose files at the top of the sample dir
                for f in sorted(p for p in src.iterdir() if p.is_file()):
                    links.append((ROOT / tree / key / f.name, f))
            else:
                links.append((ROOT / tree / key, src))
    return moves, links, problems


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    moves, links, problems = plan()
    if problems:
        print("PROBLEMS -- nothing moved:\n  " + "\n  ".join(problems))
        return 1
    clash = [d for _, d in moves if d.exists()] + [l for l, _ in links if l.exists() or l.is_symlink()]
    if clash:
        print("destinations already exist -- nothing moved:\n  " + "\n  ".join(map(str, clash[:20])))
        return 1

    obj = [m for m in moves if m[0].suffix == ".zarr"]
    print(f"{len(obj)} object moves, {len(moves) - len(obj)} directory moves, {len(links)} symlinks")
    for src, dst in moves:
        if src.suffix != ".zarr":
            print(f"  {src.name:34s} -> {dst.relative_to(ROOT)}")
    by_dst = pd.Series([str(d.relative_to(ROOT).parent) for _, d in obj]).value_counts().sort_index()
    print("\n  objects by destination:\n" + "\n".join(f"    {k:48s} {v:>3}" for k, v in by_dst.items()))
    by_link = pd.Series([str(l.relative_to(ROOT)).split("/")[0] + "/" + str(l.relative_to(ROOT)).split("/")[1]
                         for l, _ in links]).value_counts()
    print("\n  symlinks:\n" + "\n".join(f"    {k:48s} {v:>4}" for k, v in by_link.items()))
    srcs = set(AWS_TIER + ZENODO_TIER + SHARED)
    unknown = sorted(p.name for p in ROOT.iterdir() if p.is_dir()
                     and p.name not in WHOLE and p.name not in KEEP and p.name not in srcs)
    if unknown:
        print(f"\n  not in the plan, left where they are: {unknown}")
    if a.dry_run:
        print("\n[dry-run] nothing moved")
        return 0

    for src, dst in moves:
        dst.parent.mkdir(parents=True, exist_ok=True)
        src.rename(dst)
    for link, target in links:
        link.parent.mkdir(parents=True, exist_ok=True)
        link.symlink_to(target)
        assert link.exists(), f"dangling symlink {link} -> {target}"
    for d in srcs:              # now-empty source dirs only; rmdir refuses anything else
        p = ROOT / d
        if p.exists():
            try:
                p.rmdir()
            except OSError:
                p.rename(ROOT / "_archive" / f"leftover_{d}")
                print(f"  {d} was not empty after the moves -> _archive/leftover_{d}")
    print("done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
