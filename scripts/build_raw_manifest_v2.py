#!/usr/bin/env python3
"""
build_raw_manifest_v2.py
Write aws/raw_manifest_v2.csv -- the raw upload plan for v2, same columns as the
v1 aws/raw_manifest.csv and the same S3 shape:

    raw/<platform>/<institution>/<run_id>/<sample_id>/<component>/

Why a hand-written mapping and not a directory walk: the v2 source tree cannot be
read by name. `20260616_SAHA_CR_PROS_A2_EC05/` holds BOTH slides, for both RNA
and protein; the Xenium bundles are named by region (ML108208 = A2, ML108728 =
A3, confirmed against the vendor QC workbook -- see data/v2/STATUS.md). Six
acquisitions, so the mapping is spelled out below where it can be reviewed.

Per-slide pieces (flatFiles, DecodedFiles, Seurat RDS, Xenium bundle) go under
the sample. Pieces that cover a whole run and cannot be split by slide (AtoMx
TileDB, the export md5 lists, the Xenium QC workbooks) go under
`raw/<platform>/<institution>/<run_id>/_run/`.

Usage (on Cayuga; walks 693 GB, 158k files, ~8 min):
    python scripts/build_raw_manifest_v2.py --out aws/raw_manifest_v2.csv
"""

import argparse
import csv
import re
from pathlib import Path

ROOT = Path("/athena/masonlab/scratch/users/jip2007/2026_SAHA_data")
XR = ROOT / "20260427_SAHA_XR_PROS_A2_UP01"
CX = ROOT / "20260616_SAHA_CR_PROS_A2_EC05"
RUN = {"rna": CX / "ECJP7881_Multiomic_RNA_29_06_2026_16_49_19_8",
       "protein": CX / "EC_JP_7881_Multiomic_PRO_29_06_2026_16_50_18_246"}
TAG = {"rna": "RNA", "protein": "PRT"}


def plan():
    rows = []
    for sid, region in (("SAHA_XR_PROS_A2_UP01", "ML108208"), ("SAHA_XR_PROS_A3_UP01", "ML108728")):
        (src,) = XR.glob(f"output-*__{region}__*")
        rows.append((sid, src, "xenium_bundle", f"raw/xenium/upitt/UP01/{sid}/{src.name}/",
                     "10x output bundle for this region (region id -> slide per vendor QC workbook)"))
    for f in sorted(XR.glob("*.xlsx")):
        key = re.sub(r"[^a-z0-9.]+", "_", f.name.lower()).replace("_.", ".").strip("_")   # snake_case S3 key
        rows.append(("_run", f, "run_qc", f"raw/xenium/upitt/UP01/_run/{key}",
                     "vendor QC workbook, covers both slides"))
    for assay, run in RUN.items():
        code = "CR" if assay == "rna" else "CP"
        for slide in ("A2", "A3"):
            sid, tag = f"SAHA_{code}_PROS_{slide}_EC05", f"EC_JP_7881_PROS_{slide}_{TAG[assay]}"
            for comp in ("flatFiles", "DecodedFiles"):
                rows.append((sid, run / comp / tag, comp,
                             f"raw/cosmx/wcm/EC05/{sid}/{comp}/", "AtoMx export, this slide only"))
            rds = run / f"seuratObject_{tag}.RDS"
            rows.append((sid, rds, "seurat_rds", f"raw/cosmx/wcm/EC05/{sid}/{rds.name}",
                         "AtoMx Seurat export, this slide only"))
        for comp in ("TileDB", "md5sum"):
            rows.append(("_run", run / comp, comp, f"raw/cosmx/wcm/EC05/_run/{assay}/{comp}/",
                         "covers both slides; AtoMx does not split it"))
    return rows


def measure(p: Path):
    if p.is_file():
        return p.stat().st_size, 1
    n = b = 0
    for f in p.rglob("*"):
        if f.is_file():
            b += f.stat().st_size
            n += 1
    return b, n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", type=Path, default=Path("aws/raw_manifest_v2.csv"))
    a = ap.parse_args()
    rows = plan()
    missing = [str(r[1]) for r in rows if not r[1].exists()]
    if missing:
        raise SystemExit(f"source paths missing: {missing}")
    with open(a.out, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["sample_id", "source_tree", "source_path", "component", "s3_prefix",
                     "bytes", "n_files", "include", "note"])
        for sid, src, comp, prefix, note in rows:
            b, n = measure(src)
            w.writerow([sid, "2026_SAHA_data", src, comp, prefix, b, n, True, note])
    print(f"{len(rows)} rows -> {a.out}")


if __name__ == "__main__":
    main()
