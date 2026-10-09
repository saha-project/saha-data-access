#!/usr/bin/env python3
"""
fix_he_chunks.py
Re-tile H&E image elements written with 3-px-wide chunks.

attach_he_xenium_v2.py (and attach_he.py before 2026-10-08) passed
chunks=(2048, 2048, 3) for a (y, x, c) image; spatialdata stores (c, y, x) and
applied the tuple in that order, so every H&E was cut into chunks 3 px wide --
tens of thousands of files per element. Pixels are correct; only the tiling is
wrong.

Works on zarr arrays, not through spatialdata (whose delete_element_from_disk
refuses while the element is lazily backed by its own files -- the first
version of this script failed that way). Per element:
  1. build <tmp>/<name>/ : the element-group zarr.json copied byte for byte
     (multiscales + transforms unchanged by construction), each sN array
     rewritten with chunk_shape (3, 1024, 1024) and the same codecs/attrs;
  2. compare EVERY pixel of every level against the original;
  3. swap: rename the old element dir to <backup>, rename the new one in
     (same lustre mount, so both are renames).
Re-running skips elements whose s0 chunks are already >= 256 px.

Usage (on Cayuga):
    P=~/.venv-sd08/bin/python
    $P scripts/fix_he_chunks.py --store .../SAHA_XR_PROS_A2.zarr --work .../v2_retile
"""

import argparse
import json
import shutil
import sys
from pathlib import Path

import numpy as np
import zarr

CHUNK = [3, 1024, 1024]


def chunk_shape(arr_dir: Path):
    return json.loads((arr_dir / "zarr.json").read_text())["chunk_grid"]["configuration"]["chunk_shape"]


def fix(store: Path, work: Path):
    done = []
    for el in sorted((store / "images").glob("he_*")):
        if min(chunk_shape(el / "s0")[1:]) >= 256:
            print(f"{store.name}/{el.name}: chunks already {chunk_shape(el / 's0')} -- skip")
            continue
        tmp = work / "new" / store.name / el.name
        bak = work / "backup" / store.name / el.name
        assert not bak.exists(), f"backup exists, refusing to overwrite: {bak}"
        if tmp.exists():
            shutil.rmtree(tmp)
        tmp.mkdir(parents=True)
        shutil.copy2(el / "zarr.json", tmp / "zarr.json")
        levels = sorted(p.name for p in el.iterdir() if p.is_dir())
        for lv in levels:
            meta = json.loads((el / lv / "zarr.json").read_text())
            meta["chunk_grid"]["configuration"]["chunk_shape"] = CHUNK
            (tmp / lv).mkdir()
            (tmp / lv / "zarr.json").write_text(json.dumps(meta, indent=2))
            src = zarr.open_array(el / lv, mode="r")
            dst = zarr.open_array(tmp / lv, mode="r+")
            dst[...] = src[...]
            assert np.array_equal(zarr.open_array(tmp / lv, mode="r")[...], src[...]), f"{lv} pixels differ"
        n_old = sum(1 for _ in el.rglob("*") if _.is_file())
        n_new = sum(1 for _ in tmp.rglob("*") if _.is_file())
        bak.parent.mkdir(parents=True, exist_ok=True)
        el.rename(bak)
        tmp.rename(el)
        print(f"{store.name}/{el.name}: {len(levels)} levels re-tiled to {CHUNK}, every pixel identical; "
              f"files {n_old} -> {n_new}; old element at {bak}")
        done.append(el.name)
    if not done:
        return
    # root zarr.json caches every array's metadata (incl. chunk_shape): a stale copy
    # makes readers decode the new chunks with the old grid. Re-consolidate.
    shutil.copy2(store / "zarr.json", work / "backup" / store.name / "root_zarr.json")
    zarr.consolidate_metadata(str(store))
    cm = json.loads((store / "zarr.json").read_text())["consolidated_metadata"]["metadata"]
    for name in done:
        assert cm[f"images/{name}/s0"]["chunk_grid"]["configuration"]["chunk_shape"] == CHUNK

    from spatialdata import read_zarr
    from spatialdata.transformations import get_transformation
    sd = read_zarr(store, selection=("images",))
    for name in done:
        lv0 = sd.images[name]["scale0"].image
        ref = zarr.open_array(work / "backup" / store.name / name / "s0", mode="r")
        assert lv0.shape == ref.shape and int(lv0.data.sum().compute()) == int(ref[...].astype(np.int64).sum())
        print(f"{store.name}/{name}: spatialdata read-back OK, shape {lv0.shape}, "
              f"transforms {list(get_transformation(sd.images[name], get_all=True))}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--store", type=Path, required=True, nargs="+")
    ap.add_argument("--work", type=Path, required=True, help="dir for new/ (staging) and backup/ (old elements)")
    a = ap.parse_args()
    for s in a.store:
        fix(s, a.work)


if __name__ == "__main__":
    sys.exit(main())
