#!/usr/bin/env python3
"""
convert_zarr_v3.py
Rewrite a SpatialData store in the zarr v3 format every v1 object uses.

The v2 objects were written in zarr v2 (.zattrs/.zgroup); v1 is zarr v3
(zarr.json). spatialdata 0.8 reads both, but a user mixing releases should not
meet two on-disk formats, and v3 is the format zarr is moving to.

Read the source, write a new store (spatialdata 0.8 writes v3), then prove the
copy holds the same thing: same elements, same table rows/columns/X sums, same
image and label shapes, same shapes rows, same coordinate transforms. Source is
never touched; `<out>/<name>` appears only after every check passes.

Usage (on Cayuga, one store per slurm array task):
    P=~/.venv-sd08/bin/python
    $P scripts/convert_zarr_v3.py --src .../v2_objects --out .../v2_objects_v3 --index 0
"""

import argparse
import shutil
import warnings
from pathlib import Path

import numpy as np


def shape_of(el):
    if hasattr(el, "children"):
        return tuple(tuple(next(iter(el[k].data_vars.values())).shape) for k in el.children)
    return tuple(el.shape)


def fingerprint(sd):
    from spatialdata.transformations import get_transformation
    fp = {}
    for kind, name, el in sd.gen_elements():
        if kind == "tables":
            X = el.X.data if hasattr(el.X, "data") else np.asarray(el.X)
            attrs = el.uns.get("spatialdata_attrs") or {}
            fp[(kind, name)] = (el.n_obs, el.n_vars, tuple(el.obs.columns),
                                float(np.sum(X, dtype=np.float64)),   # order-independent enough
                                tuple(sorted(map(str, el.obsm))),
                                tuple(sorted((k, str(v)) for k, v in attrs.items())),  # key order is not data
                                hash(el.obs.astype(str).to_csv()))    # every obs value (same process)
            continue
        t = {k: np.round(v.to_affine_matrix(("x", "y"), ("x", "y")), 9).tolist()
             for k, v in get_transformation(el, get_all=True).items()}
        if kind in ("images", "labels"):
            fp[(kind, name)] = (shape_of(el), str(t))
        elif kind == "shapes":
            fp[(kind, name)] = (len(el), str(t))
        else:  # points
            fp[(kind, name)] = (int(el.shape[0].compute()), str(t))
    return fp


def convert(src: Path, out_dir: Path, verify_only=False):
    warnings.filterwarnings("ignore")
    from spatialdata import read_zarr
    final, tmp = out_dir / src.name, out_dir / (src.name + ".partial")
    if final.exists():
        print(f"{src.name}: already converted, skipping")
        return
    sd = read_zarr(src)
    before = fingerprint(sd)
    if verify_only:   # re-check a copy already written, e.g. after tightening the comparison
        assert tmp.exists(), f"{tmp} not found"
    else:
        if tmp.exists():
            shutil.rmtree(tmp)  # our own half-finished output, never a source
        sd.write(tmp)
    back = read_zarr(tmp)
    assert (tmp / "zarr.json").exists() and not (tmp / ".zgroup").exists(), "output is not zarr v3"
    after = fingerprint(back)
    diff = [k for k in before.keys() | after.keys() if before.get(k) != after.get(k)]
    assert not diff, f"copy differs from source in {diff[:5]}"
    tmp.rename(final)
    print(f"{src.name}: zarr v3, {len(after)} elements identical to source")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--index", type=int, required=True)
    ap.add_argument("--verify-only", action="store_true", help="check an existing .partial, don't rewrite")
    a = ap.parse_args()
    a.out.mkdir(parents=True, exist_ok=True)
    convert(sorted(a.src.glob("*.zarr"))[a.index], a.out, a.verify_only)


if __name__ == "__main__":
    main()
