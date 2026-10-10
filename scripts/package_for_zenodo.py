#!/usr/bin/env python3
"""
package_for_zenodo.py
Package SpatialData zarr stores into per-record Zenodo upload directories.

Zenodo stores files, not directories, so each .zarr becomes one .zip. Archives
use ZIP_STORED (no compression): zarr chunks are already compressed, so deflate
costs hours of CPU for ~1% gain.

Record split, derived from samples.parquet -- see record_slug() and the
"Release Layout" section of docs/FILE_FORMATS.md:
    one record per organ, spanning both platforms and both assays
    pancreas split by platform  (both together are 64 GB, over the cap)
    colorectal cancer its own record rather than inside normal colon
    plus a parent collection record holding the metadata tables and docs.

Every child stays under Zenodo's 50 GB default quota, so no quota-increase
request is needed. Re-check that after any change to what the release contains:
the record sizes are a property of the data, not a standing guarantee.

Usage:
    # see the plan without writing anything
    python scripts/package_for_zenodo.py --zarr-dir ~/SAHA_zarr_build \
        --zarr-dir "/path/to/spatialdata/RNA" \
        --manifest data/samples.parquet --out-dir ~/SAHA_zenodo --dry-run

    # package everything
    python scripts/package_for_zenodo.py --zarr-dir ~/SAHA_zarr_build \
        --zarr-dir "/path/to/spatialdata/RNA" \
        --manifest data/samples.parquet --out-dir ~/SAHA_zenodo

    # one record only, resumable
    python scripts/package_for_zenodo.py ... --only cosmx-protein --skip-existing
"""

import argparse
import hashlib
import shutil
import sys
import zipfile
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parent.parent

# Files macOS scatters through directories; never archive them.
JUNK_NAMES = {".DS_Store", "Icon\r"}
JUNK_PREFIX = "._"

# Copied into the parent collection record. Flat single files, not the
# Hive-partitioned directories AWS carries: a Zenodo record is a flat list of
# files and someone downloading one wants one file.
PARENT_FILES = [
    "data/samples.parquet",
    "data/donors.parquet",
    "data/runs.parquet",
    "data/panels.parquet",
    # The two tables that answer "which cells came from whom". Without these the
    # parent record describes samples and donors but not the crossing between
    # them, which is the unit the objects are actually organised by.
    "zenodo/measurements.csv",
    "zenodo/cohort_coverage.csv",
    "LICENSE-DATA",
]


def is_junk(p: Path) -> bool:
    return p.name.startswith(JUNK_PREFIX) or p.name in JUNK_NAMES


def sample_id_of(zarr_path: Path) -> str:
    """SAHA_XR_APE_A9_MS01_01_RNA.zarr -> SAHA_XR_APE_A9_MS01_01

    Xenium stores carry an assay suffix that the manifest sample_id does not.
    """
    stem = zarr_path.stem
    for suffix in ("_RNA", "_PRT"):
        if stem.endswith(suffix):
            return stem[: -len(suffix)]
    return stem


def record_slug(row) -> str:
    """One record per organ, spanning both platforms and both assays.

    Keeping matched RNA and protein for the same tissue in one record is the
    point -- that pairing is what the atlas is for, and a user studying one
    organ should not have to assemble it from several downloads.

    Pancreas is the exception: it is the most deeply sampled organ and the two
    platforms together come to 64.2 GB, over Zenodo's 50 GB per-record cap.
    Splitting it by condition does not help (normal alone is 58.5 GB), so
    pancreas alone is split by platform.

    Colorectal cancer gets its own record rather than sitting inside colon:
    it is a distinct cohort on a pre-commercial instrument, and burying three
    cancer slides in the normal-colon record would misrepresent both.

    Prostate cancer does NOT get the same treatment, and the asymmetry is
    deliberate. CRC is three slides against a large, heavily-used normal-colon
    record, so a reader looking for cancer would otherwise have to find it
    inside a record named for healthy tissue. Prostate is two normal
    acquisitions totalling 1.1 GB; splitting it would leave two records too
    small to be useful, and PRCA is a single specimen (PCa_9224) that is not in
    Table 1, so it ships with null demographics and reads as an appendix to the
    organ rather than a cohort of its own. It therefore sits inside
    saha-v1-prostate, as PDAC sits inside pancreas, distinguished by
    `condition`. No code is needed for this -- the default branch below already
    routes it -- but the reasoning is recorded so it is not re-argued later.
    """
    organ = row["organ"].replace("_", "-")
    if row["organ"] == "pancreas":
        return f"saha-v1-pancreas-{row['platform']}"
    if row["organ"] == "colon" and row.get("condition") == "cancer":
        return "saha-v1-crc"
    return f"saha-v1-{organ}"


def md5(path: Path, blocksize: int = 8 << 20) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(blocksize), b""):
            h.update(block)
    return h.hexdigest()


def zip_store(zarr_path: Path, out_zip: Path) -> tuple:
    """Archive one zarr store. Returns (n_files, n_skipped)."""
    members = [p for p in sorted(zarr_path.rglob("*")) if p.is_file()]
    keep = [p for p in members if not is_junk(p)]
    skipped = len(members) - len(keep)

    tmp = out_zip.with_suffix(".zip.partial")
    with zipfile.ZipFile(tmp, "w", zipfile.ZIP_STORED, allowZip64=True) as zf:
        for i, p in enumerate(keep, 1):
            # Root the archive at <sample>.zarr so unzip reproduces the store.
            zf.write(p, arcname=str(Path(zarr_path.name) / p.relative_to(zarr_path)))
            if i % 500 == 0:
                print(f"      {i:,}/{len(keep):,} files", end="\r", flush=True)
    tmp.rename(out_zip)
    print(f"      {len(keep):,} files archived"
          + (f", {skipped} junk skipped" if skipped else "") + "      ")
    return len(keep), skipped


def write_record_docs(rec_dir: Path, slug: str, rows: pd.DataFrame,
                      checksums: dict) -> None:
    files = pd.DataFrame(
        [
            {
                "file": f"{r.zarr_name}.zip",
                "sample_id": r.sample_id,
                "platform": r.platform,
                "assay_type": r.assay_type,
                "organ": r.organ,
                "condition": r.condition,
                # donor is per-cell (obs donor_core); a scalar donor_id here
                # collapsed whole tissue microarrays onto one fictional person
                "n_donors": r.n_donors,
                "n_cells": r.n_cells,
                "n_cells_qc_pass": r.n_cells_qc_pass,
                "size_bytes": checksums[f"{r.zarr_name}.zip"][1],
                "md5": checksums[f"{r.zarr_name}.zip"][0],
            }
            for r in rows.itertuples()
            if f"{r.zarr_name}.zip" in checksums
        ]
    )
    files.to_csv(rec_dir / "file_manifest.csv", index=False)

    with open(rec_dir / "MD5SUMS.txt", "w") as f:
        for name, (digest, _) in sorted(checksums.items()):
            f.write(f"{digest}  {name}\n")

    total_gb = files["size_bytes"].sum() / 1e9
    lines = [
        f"# SAHA v1.0 — {slug}",
        "",
        f"{len(files)} SpatialData objects, {total_gb:.1f} GB.",
        "",
        "Part of the SAHA v1.0 SpatialData release. See the parent collection",
        "record for metadata tables, full documentation, and citation details.",
        "",
        "## Reading the data",
        "",
        "Each `.zip` contains one SpatialData (OME-NGFF / Zarr v3) store.",
        "Archives are uncompressed containers — zarr chunks are already compressed.",
        "",
        "```bash",
        f"unzip {files['file'].iloc[0] if len(files) else 'SAMPLE.zarr.zip'}",
        "```",
        "",
        "```python",
        "import spatialdata as sd",
        f"sdata = sd.read_zarr(\"{files['sample_id'].iloc[0] if len(files) else 'SAMPLE'}"
        f"{'_RNA' if 'xenium' in slug else ''}.zarr\")",
        "adata = sdata.tables[\"table\"]",
        "```",
        "",
        "Verify downloads against `MD5SUMS.txt`; per-sample detail is in",
        "`file_manifest.csv`.",
        "",
    ]
    if slug.startswith("cosmx"):
        lines += [
            "## Note on images",
            "",
            "CosMx objects contain **no morphology images** — the flat-file exports",
            "they were built from do not include image data. Expression tables,",
            "cell boundary polygons, and (for RNA) transcript locations are complete.",
            "",
            "## Note on cell filtering",
            "",
            "Objects contain every segmented cell. The obs column `qc_pass` marks",
            "cells present in the published, QC-filtered organ-level h5ad — that",
            "subset is what the manuscript cell counts refer to.",
            "",
            "```python",
            "adata[adata.obs[\"qc_pass\"]]   # published cell set",
            "```",
            "",
            "Samples without a published h5ad carry no `qc_pass` column; their",
            "tables are the raw segmented cell set.",
            "",
        ]
    (rec_dir / "README.md").write_text("\n".join(lines))


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--zarr-dir", type=Path, action="append", required=True,
                   help="directory containing .zarr stores (repeatable)")
    p.add_argument("--manifest", type=Path, default=REPO / "data/samples.parquet")
    p.add_argument("--out-dir", type=Path, required=True)
    p.add_argument("--only", nargs="*", help="restrict to these record slugs")
    p.add_argument("--skip-existing", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    stores = sorted({z for d in args.zarr_dir for z in d.glob("*.zarr")})
    if not stores:
        sys.exit(f"ERROR: no .zarr stores found under {args.zarr_dir}")

    man = pd.read_parquet(args.manifest)
    found = pd.DataFrame([{"zarr_path": z, "zarr_name": z.name,
                           "sample_id": sample_id_of(z)} for z in stores])

    df = found.merge(man, on="sample_id", how="left")
    orphans = df[df["platform"].isna()]
    if len(orphans):
        print(f"WARNING: {len(orphans)} store(s) not in the manifest, excluded: "
              f"{', '.join(orphans['sample_id'])}\n")
        df = df[df["platform"].notna()]

    df["record"] = df.apply(record_slug, axis=1)
    missing = set(man.loc[man["platform"].isin(["cosmx", "xenium"]), "sample_id"]) \
        - set(df["sample_id"])
    if missing:
        print(f"NOTE: {len(missing)} manifest sample(s) have no built zarr: "
              f"{', '.join(sorted(missing))}\n")

    records = sorted(df["record"].unique())
    if args.only:
        records = [r for r in records if r in args.only]

    print(f"{len(df)} object(s) -> {len(records)} record(s)\n")
    for slug in records:
        rows = df[df["record"] == slug]
        gb = sum(sum(f.stat().st_size for f in z.rglob("*") if f.is_file())
                 for z in rows["zarr_path"]) / 1e9
        print(f"  {slug:22s} {len(rows):2d} objects  {gb:6.1f} GB"
              + ("  OVER 50 GB — needs a quota request" if gb > 50 else ""))
    print()

    if args.dry_run:
        print("[dry-run] nothing written")
        return

    args.out_dir.mkdir(parents=True, exist_ok=True)

    for slug in records:
        rows = df[df["record"] == slug]
        rec_dir = args.out_dir / slug
        rec_dir.mkdir(parents=True, exist_ok=True)
        print(f"\n=== {slug} ===")

        checksums = {}
        for r in rows.itertuples():
            out_zip = rec_dir / f"{r.zarr_name}.zip"
            if args.skip_existing and out_zip.exists():
                print(f"  [skip] {out_zip.name}")
                checksums[out_zip.name] = (md5(out_zip), out_zip.stat().st_size)
                continue
            print(f"  {r.zarr_name} …")
            zip_store(Path(r.zarr_path), out_zip)
            checksums[out_zip.name] = (md5(out_zip), out_zip.stat().st_size)
            print(f"      {out_zip.stat().st_size / 1e9:.2f} GB  "
                  f"md5 {checksums[out_zip.name][0][:12]}…")

        write_record_docs(rec_dir, slug, rows, checksums)
        print(f"  wrote README.md, file_manifest.csv, MD5SUMS.txt")

    # Parent collection record.
    parent = args.out_dir / "parent-collection"
    parent.mkdir(parents=True, exist_ok=True)
    copied = []
    for rel in PARENT_FILES:
        src = REPO / rel
        if src.exists():
            shutil.copy2(src, parent / src.name)
            copied.append(src.name)
        else:
            print(f"  WARNING: {rel} not found — not copied to parent record")
    for extra in ("zenodo/DESCRIPTION.md", "zenodo/file_manifest_cosmx.csv"):
        src = REPO / extra
        if src.exists():
            shutil.copy2(src, parent / src.name)
            copied.append(src.name)

    all_files = df.assign(zip_name=df["zarr_name"] + ".zip")[
        ["zip_name", "sample_id", "record", "platform", "assay_type", "organ",
         "condition", "n_donors", "n_cells", "n_cells_qc_pass"]
    ]
    all_files.to_csv(parent / "file_manifest.csv", index=False)
    print(f"\n=== parent-collection ===\n  {', '.join(copied)}, file_manifest.csv")

    total = sum(f.stat().st_size for f in args.out_dir.rglob("*") if f.is_file())
    print(f"\n{'=' * 60}\npackaged {total / 1e9:.1f} GB into {args.out_dir}")


if __name__ == "__main__":
    main()
