#!/usr/bin/env python3
"""
verify_zenodo_package.py
Prove each packaged Zenodo record is what a downloader will need: the right
objects, intact zips, and zip contents that byte-for-byte equal the store that
QC looked at.

For one record directory:
  membership   zips == samples.parquet rows with record_slug() == this record
  md5          every file in MD5SUMS.txt recomputes; every zip is listed;
               file_manifest.csv agrees on md5, size and n_cells
  integrity    ZipFile.testzip() -- every member decompresses to its CRC
  freshness    member set and per-file CRC32 equal the live source store. A
               store edited after packaging (e.g. a later in-place fix) fails
               here while md5 still passes, because MD5SUMS was written from
               the stale zip.

Usage (on Cayuga, one record per slurm array task):
    P=~/.venv-sd08/bin/python
    $P scripts/verify_zenodo_package.py --record saha-v1-prostate --out OUT
    $P scripts/verify_zenodo_package.py --summarize --out OUT
"""

import argparse
import hashlib
import json
import os
import sys
import zipfile
import zlib
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from package_for_zenodo import record_slug, sample_id_of  # noqa: E402

# Reference tables (samples.parquet, measurements.csv). SAHA_REF points at a
# fresh copy when the repo checkout on the cluster lags the local one.
REPO = Path(os.environ.get("SAHA_REF", Path(__file__).resolve().parent.parent))
PKG = Path("/athena/project-saha/derived/release/zenodo")
# the 126 stores the zips were made from, one dir per record (see organize_derived.py)
ZENODO_TIER = Path("/athena/project-saha/derived/build/zenodo_tier")


def md5(p, bs=16 << 20):
    h = hashlib.md5()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(bs), b""):
            h.update(b)
    return h.hexdigest()


def crc(p, bs=16 << 20):
    c = 0
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(bs), b""):
            c = zlib.crc32(b, c)
    return c & 0xFFFFFFFF


def verify(record):
    rdir = PKG / record
    P = []
    s = pd.read_parquet(REPO / "data/samples.parquet")
    s["slug"] = s.apply(record_slug, axis=1)
    want = set(s.loc[s.slug == record, "sample_id"])
    have = {sample_id_of(Path(p.stem)) for p in rdir.glob("*.zarr.zip")}
    if want - have:
        P.append(f"missing zips: {sorted(want - have)}")
    if have - want:
        P.append(f"zips not in this record per samples.parquet: {sorted(have - want)}")

    sums = dict(reversed(l.split(None, 1)) for l in (rdir / "MD5SUMS.txt").read_text().splitlines() if l.strip())
    sums = {k.strip(): v for k, v in sums.items()}
    man = pd.read_csv(rdir / "file_manifest.csv").set_index("file")
    zips = {}
    for z in sorted(rdir.glob("*.zarr.zip")):
        store = z.name.removesuffix(".zip")          # e.g. SAHA_XR_..._01_RNA.zarr
        sid = sample_id_of(Path(store))
        r = {"zip": z.name, "size": z.stat().st_size}
        r["md5"] = m = md5(z)
        if sums.get(z.name) != m:
            P.append(f"{z.name}: md5 {m} != MD5SUMS {sums.get(z.name)}")
        if z.name not in man.index:
            P.append(f"{z.name}: not in file_manifest.csv")
        else:
            row = man.loc[z.name]
            if row.md5 != m or int(row.size_bytes) != r["size"]:
                P.append(f"{z.name}: file_manifest.csv md5/size disagree with the file")
            exp = s.set_index("sample_id").n_cells.get(sid)
            if exp is not None and int(row.n_cells) != int(exp):
                P.append(f"{z.name}: manifest n_cells {row.n_cells} != samples.parquet {exp}")

        with zipfile.ZipFile(z) as zf:
            badm = zf.testzip()
            if badm:
                P.append(f"{z.name}: corrupt member {badm}")
            infos = {i.filename.split("/", 1)[1]: i for i in zf.infolist()
                     if not i.is_dir() and "/" in i.filename}
            tops = {i.filename.split("/", 1)[0] for i in zf.infolist()}
        if tops != {store}:
            P.append(f"{z.name}: top-level entries {sorted(tops)[:3]}, expected {store}/")

        src = ZENODO_TIER / record / store
        src = src if src.exists() else None
        r["source"] = str(src)
        if src is None:
            P.append(f"{z.name}: no source store found")
        else:
            files = {str(p.relative_to(src)): p for p in src.rglob("*") if p.is_file()}
            only_src, only_zip = files.keys() - infos.keys(), infos.keys() - files.keys()
            if only_src or only_zip:
                P.append(f"{z.name}: differs from its source store -- {len(only_src)} files "
                         f"only in store (e.g. {sorted(only_src)[:2]}), {len(only_zip)} only in zip")
            changed = [k for k in files.keys() & infos.keys()
                       if files[k].stat().st_size != infos[k].file_size or crc(files[k]) != infos[k].CRC]
            if changed:
                P.append(f"{z.name}: {len(changed)} files changed since packaging (e.g. {sorted(changed)[:3]})")
            r["n_files"] = len(files)
        zips[z.name] = r
    extra = set(sums) - {p.name for p in rdir.iterdir()}
    if extra:
        P.append(f"MD5SUMS lists files not present: {sorted(extra)}")
    return {"record": record, "n_zips": len(have), "n_expected": len(want),
            "bytes": sum(r["size"] for r in zips.values()), "problems": P, "zips": zips}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--record")
    ap.add_argument("--index", type=int, help="slurm array index into the sorted record list")
    ap.add_argument("--summarize", action="store_true")
    ap.add_argument("--out", default="/athena/masonlab/scratch/users/jip2007/release_qc")
    a = ap.parse_args()
    out = Path(a.out) / "zips"
    out.mkdir(parents=True, exist_ok=True)
    records = sorted(p.name for p in PKG.iterdir() if p.is_dir() and p.name.startswith("saha-v1-"))
    if a.summarize:
        for r in records:
            f = out / f"{r}.json"
            j = json.loads(f.read_text()) if f.exists() else {"record": r, "problems": ["no result"]}
            print(f"{r:28s} {j.get('n_zips', '?'):>3}/{j.get('n_expected', '?'):<3} "
                  f"{j.get('bytes', 0) / 1e9:6.1f} GB  {'OK' if not j['problems'] else ''}")
            for p in j["problems"]:
                print("    -", p)
        return
    rec = a.record or records[a.index]
    j = verify(rec)
    (out / f"{rec}.json").write_text(json.dumps(j, indent=1))
    print(rec, "OK" if not j["problems"] else j["problems"])


if __name__ == "__main__":
    sys.exit(main())
