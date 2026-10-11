#!/usr/bin/env python3
"""
build_aws_tier.py
Make the AWS-tier copy of a set of objects: same tables, shapes and points,
full-resolution morphology instead of the 4x the Zenodo tier carries.

Why this is not merge_cosmx_images.py --overwrite
-------------------------------------------------
Once an object has been copied, its `morphology` element is backed by files
inside the copy's own store. Asking SpatialData to delete that element raises:

    The file path specified is a parent directory of one or more files used for
    backing ... Deleting the data would corrupt the SpatialData object.

which is the library protecting itself, correctly. Detaching the element on the
filesystem first does not help either: the parent `images` group still lists
`morphology` as a member, so the next read fails with "Image location ... does
not seem to exist".

What does work is replacing the element in place. Element directories are
self-describing -- the multiscale metadata lives inside `images/morphology/` --
and the parent group references members by name only. So swapping the directory
for the full-resolution one leaves a valid object whose morphology is now 4x
larger in each axis. Verified by reading the result back.

Usage:
    P=~/.venv-sd08/bin/python
    $P scripts/build_aws_tier.py --src .../tap_zarr --images .../tap_images_full \\
        --out .../tap_aws [--dry-run]
"""

import argparse
import shutil
import subprocess
import sys
from pathlib import Path


def base_shape(el):
    """Base-resolution shape of a possibly-multiscale image element."""
    if hasattr(el, "children"):
        node = el["scale0"] if "scale0" in el else next(iter(el.children.values()))
        return tuple(int(x) for x in next(iter(node.data_vars.values())).shape)
    return tuple(int(x) for x in el.shape)


def verify(path: Path) -> dict:
    import warnings
    warnings.filterwarnings("ignore")
    from spatialdata import read_zarr
    sd = read_zarr(path)
    if not sd.tables:
        raise ValueError("no table")
    t = list(sd.tables.values())[0]
    return {"morphology": base_shape(sd.images["morphology"]) if sd.images else None,
            "n_obs": int(t.n_obs), "shapes": list(sd.shapes), "points": list(sd.points)}


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--src", type=Path, required=True,
                    help="Zenodo-tier objects (4x morphology)")
    ap.add_argument("--images", type=Path, required=True,
                    help="full-resolution image stores")
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--element", default="morphology")
    ap.add_argument("--only", nargs="*")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    stores = sorted(args.src.glob("*.zarr"))
    if args.only:
        stores = [s for s in stores if s.stem in args.only]
    if not stores:
        sys.exit(f"ERROR: no .zarr under {args.src}")

    args.out.mkdir(parents=True, exist_ok=True)
    ok = skipped = failed = 0
    for src in stores:
        sample = src.stem
        dst = args.out / src.name
        full = args.images / src.name / "images" / args.element

        if not full.is_dir():
            # No full-resolution imagery for this acquisition -- the object is
            # identical in both tiers, so copy it and move on rather than
            # leaving a hole in the AWS tier.
            if not dst.exists():
                print(f"  {sample:26s} no full-res imagery; copying as-is")
                if not args.dry_run:
                    shutil.copytree(src, dst)
                ok += 1
            else:
                print(f"  {sample:26s} SKIP (exists)")
                skipped += 1
            continue

        if dst.exists() and not args.dry_run:
            try:
                got = verify(dst)
                if got["morphology"] == base_shape_of(full):
                    print(f"  {sample:26s} SKIP (already full-res)")
                    skipped += 1
                    continue
            except Exception:
                pass

        print(f"  {sample}")
        if args.dry_run:
            print(f"    copy {src} -> {dst}, then swap in {full}")
            continue
        try:
            if not dst.exists():
                shutil.copytree(src, dst)
            target = dst / "images" / args.element
            if target.exists():
                shutil.rmtree(target)
            shutil.copytree(full, target)
            got = verify(dst)
            print(f"    morphology {got['morphology']}, {got['n_obs']:,} cells, "
                  f"shapes {got['shapes']}")
            ok += 1
        except Exception as e:
            print(f"    FAILED {type(e).__name__}: {e}")
            failed += 1

    print(f"\n{ok} built, {skipped} skipped, {failed} failed"
          + ("   [dry-run]" if args.dry_run else ""))
    return 1 if failed else 0


def base_shape_of(element_dir: Path):
    """Base shape of an on-disk image element, without loading the parent object."""
    import warnings
    warnings.filterwarnings("ignore")
    from spatialdata import read_zarr
    sd = read_zarr(element_dir.parent.parent)
    return base_shape(sd.images[element_dir.name])


if __name__ == "__main__":
    sys.exit(main())
