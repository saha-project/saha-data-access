#!/usr/bin/env python3
"""
merge_cosmx_images.py
Add stitched image/label elements to the existing CosMx SpatialData stores.

stitch_cosmx_images.py runs on Cayuga and emits an image-only store per sample.
This adds those elements to the objects already built in ~/SAHA_zarr_build,
rather than rebuilding them -- the existing tables carry the qc_pass
reconciliation against the published h5ads, which is not reproducible on the
cluster.

It also writes an obs column `label_id` (= fov * 100000 + fov_cell_id) matching
the values in the stitched `cell_labels` image, so a cell in the table can be
found in the label image and vice versa. The table keeps annotating the
`cells` shapes element; nothing about the existing structure changes.

Usage:
    P=.venv-sd/bin/python

    $P scripts/merge_cosmx_images.py --images-dir ~/SAHA_cosmx_images \
        --zarr-dir ~/SAHA_zarr_build --dry-run

    $P scripts/merge_cosmx_images.py --images-dir ~/SAHA_cosmx_images \
        --zarr-dir ~/SAHA_zarr_build
"""

import argparse
import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

FOV_STRIDE = 100_000       # must match stitch_cosmx_images.py


def label_ids(obs):
    if not {"fov", "fov_cell_id"} <= set(obs.columns):
        raise KeyError("table obs needs both 'fov' and 'fov_cell_id'")
    return (obs["fov"].astype("int64") * FOV_STRIDE
            + obs["fov_cell_id"].astype("int64")).astype("uint32")


def merge(target: Path, source: Path, overwrite: bool) -> dict:
    import spatialdata as sd

    dst = sd.read_zarr(target)
    src = sd.read_zarr(source)
    added, skipped = [], []

    for kind in ("images", "labels"):
        for name, el in getattr(src, kind).items():
            if name in dst.images or name in dst.labels:
                if not overwrite:
                    skipped.append(name)
                    continue
                dst.delete_element_from_disk(name)
                del getattr(dst, "images" if name in dst.images else "labels")[name]
            getattr(dst, kind)[name] = el
            dst.write_element(name)
            added.append(name)

    # label_id lets users join the table to the stitched cell_labels image. It
    # is only meaningful when such an image exists: the data_TAP exports carry
    # no CellLabels, so their stores hold morphology alone, and writing a join
    # key to a label image that is not there would be a claim the object cannot
    # honour -- besides rewriting 22 tables for nothing.
    if not dst.tables:
        raise ValueError("target has no table -- refusing to touch it")
    tname = list(dst.tables)[0]
    table = dst.tables[tname]
    has_labels = bool(dst.labels)
    if not has_labels:
        skipped.append("table:label_id (no label image in this object)")
    elif "label_id" not in table.obs.columns or overwrite:
        table.obs["label_id"] = label_ids(table.obs).to_numpy()

        # write_element(overwrite=True) refuses to write into its own store, so
        # the element is removed first. That ordering is destructive: if the
        # write raises, or the process dies between the two calls, the object is
        # left with NO table. Two objects were lost that way before this guard
        # existed, so validate while the good copy is still on disk.
        from spatialdata.models import TableModel
        try:
            TableModel.parse(table)
        except Exception as e:
            raise ValueError(f"table fails validation, nothing written: {e}")
        attrs = dict(table.uns.get("spatialdata_attrs", {}))
        for k in ("region_key", "instance_key"):
            col = attrs.get(k)
            if col and col not in table.obs.columns:
                raise ValueError(f"{k} '{col}' missing from obs, nothing written")

        dst.delete_element_from_disk(tname)
        dst.write_element(tname)
        added.append("table:label_id")

    return {"added": added, "skipped": skipped}


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--images-dir", type=Path, required=True,
                    help="stores produced by stitch_cosmx_images.py")
    ap.add_argument("--zarr-dir", type=Path, required=True,
                    help="existing CosMx objects to add elements to")
    ap.add_argument("--only", nargs="*")
    ap.add_argument("--overwrite", action="store_true",
                    help="replace image elements that are already present")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    srcs = sorted(args.images_dir.glob("*.zarr"))
    if not srcs:
        sys.exit(f"ERROR: no .zarr stores in {args.images_dir}")

    pairs, missing = [], []
    for s in srcs:
        if args.only and s.stem not in args.only:
            continue
        t = args.zarr_dir / s.name
        (pairs if t.exists() else missing).append((s, t) if t.exists() else s.stem)

    if missing:
        print(f"WARNING: no target object for {len(missing)}: {', '.join(missing)}\n")
    if not pairs:
        sys.exit("ERROR: nothing to merge")

    print(f"{len(pairs)} sample(s) to merge\n")
    if args.dry_run:
        import spatialdata as sd
        for s, t in pairs:
            src = sd.read_zarr(s)
            print(f"  {t.name}: + {list(src.images)} {list(src.labels)}")
        print("\n[dry-run] nothing written")
        return

    for i, (s, t) in enumerate(pairs, 1):
        print(f"[{i}/{len(pairs)}] {t.name}", flush=True)
        try:
            r = merge(t, s, args.overwrite)
            print(f"    added {', '.join(r['added']) or 'nothing'}"
                  + (f"; skipped existing {', '.join(r['skipped'])}"
                     if r["skipped"] else ""))
        except Exception as e:
            print(f"    FAILED {type(e).__name__}: {e}")

    print("\ndone -- re-run package_for_zenodo.py to rebuild the upload archives")


if __name__ == "__main__":
    main()
