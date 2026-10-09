#!/usr/bin/env python3
"""
upgrade_he_level.py
Replace an attached H&E element with a finer pyramid level of the SAME scan, keeping its registration.

The v2 Xenium H&E was attached from svs level 2 (~4 um/px). No re-registration is needed to go finer:
the stored transform A_old maps level-k pixels to global, and a level-0 pixel is 1/ds_k of a level-k one, so
    A_new = A_old @ diag(1/ds_k, 1/ds_k, 1)
where ds_k = openslide level_downsamples[k] (exactly what attach_he_xenium_v2.py used).

The new element is written into a scratch SpatialData store, then its directory is swapped into the
release store (old element -> --work/backup), metadata re-consolidated, and the store read back:
  * the element's footprint corners in global coords match the old element's (within one old pixel);
  * the old element's shape equals svs level k, so k was identified correctly;
  * level 0 pixels on 8 random tiles equal the svs read directly.
Table uns (source, accuracy_note) is untouched: same scan, same registration.

Usage (on Cayuga):
    $P scripts/upgrade_he_level.py --store .../SAHA_XR_PROS_A2.zarr --element he_serial_section \
        --he .../PROS_A2_Serial_Section_H&E.svs --from-level 2 --to-level 0 --work .../v2_he_l0
"""

import argparse
import json
import shutil
import sys
import warnings
from pathlib import Path

import numpy as np
import zarr

sys.path.insert(0, str(Path(__file__).parent))
from attach_he_xenium_v2 import he_dask  # noqa: E402


def corners(A, W, H):
    P = np.array([[0, 0, 1], [W, 0, 1], [0, H, 1], [W, H, 1]], float).T
    return (A @ P)[:2].T


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--store", type=Path, required=True)
    ap.add_argument("--element", required=True)
    ap.add_argument("--he", type=Path, required=True)
    ap.add_argument("--from-level", type=int, required=True)
    ap.add_argument("--to-level", type=int, default=0)
    ap.add_argument("--work", type=Path, required=True)
    a = ap.parse_args()
    warnings.filterwarnings("ignore")
    import openslide
    from spatialdata import SpatialData, read_zarr
    from spatialdata.models import Image2DModel
    from spatialdata.transformations import Affine, get_transformation

    store = a.store.resolve()
    sl = openslide.OpenSlide(str(a.he))
    ds_from = float(sl.level_downsamples[a.from_level]); ds_to = float(sl.level_downsamples[a.to_level])
    sd = read_zarr(store, selection=("images",))
    old = sd.images[a.element]
    old0 = next(iter(old["scale0"].data_vars.values())) if hasattr(old, "children") else old
    Wf, Hf = sl.level_dimensions[a.from_level]
    assert tuple(old0.shape[1:]) == (Hf, Wf), f"old element {old0.shape} is not svs level {a.from_level} {(Hf, Wf)}"
    A_old = get_transformation(old, "global").to_affine_matrix(("x", "y"), ("x", "y"))
    A_new = A_old @ np.diag([ds_to / ds_from, ds_to / ds_from, 1.0])

    arr, _, _ = he_dask(a.he, a.to_level, chunk=1024)
    H, W = arr.shape[:2]
    err = np.abs(corners(A_new, W, H) - corners(A_old, Wf, Hf)).max()
    old_px = np.sqrt(abs(np.linalg.det(A_old[:2, :2])))
    print(f"{store.name}/{a.element}: level {a.from_level} {Hf}x{Wf} -> level {a.to_level} {H}x{W}; "
          f"footprint corner mismatch {err:.2f} global px (old pixel = {old_px:.1f})")
    assert err <= old_px, "footprints disagree -- transform rescale is wrong"

    n_scales = int(np.floor(np.log2(min(H, W) / 512)))
    img = Image2DModel.parse(arr, dims=("y", "x", "c"), c_coords=list(old0.coords["c"].values),
                             transformations={"global": Affine(A_new, input_axes=("x", "y"), output_axes=("x", "y"))},
                             scale_factors=[2] * min(n_scales, 6), chunks={"c": 3, "y": 1024, "x": 1024})
    tmp = a.work / "new" / store.name
    bak = a.work / "backup" / store.name
    assert not (bak / a.element).exists(), f"backup exists: {bak / a.element}"
    if tmp.exists():
        shutil.rmtree(tmp)
    SpatialData(images={a.element: img}).write(tmp)

    bak.mkdir(parents=True, exist_ok=True)
    shutil.copy2(store / "zarr.json", bak / "root_zarr.json")
    (store / "images" / a.element).rename(bak / a.element)
    (tmp / "images" / a.element).rename(store / "images" / a.element)
    zarr.consolidate_metadata(str(store))

    back = read_zarr(store, selection=("images",)).images[a.element]
    b0 = next(iter(back["scale0"].data_vars.values()))
    assert b0.shape == (3, H, W), b0.shape
    assert np.allclose(get_transformation(back, "global").to_affine_matrix(("x", "y"), ("x", "y")), A_new)
    rng = np.random.default_rng(0)
    for y, x in zip(rng.integers(0, H - 512, 8), rng.integers(0, W - 512, 8)):
        want = np.asarray(sl.read_region((int(x * ds_to), int(y * ds_to)), a.to_level, (512, 512)).convert("RGB"))
        got = np.moveaxis(np.asarray(b0[:, y:y + 512, x:x + 512]), 0, -1)
        assert np.array_equal(got, want), f"pixels differ at {(y, x)}"
    s0 = json.loads((store / "images" / a.element / "s0" / "zarr.json").read_text())
    n = sum(1 for p in (store / "images" / a.element).rglob("*") if p.is_file())
    print(f"{store.name}/{a.element}: swapped in; {len(list(back.children))} levels, chunks "
          f"{s0['chunk_grid']['configuration']['chunk_shape']}, {n} files; transform + 8 tiles verified; "
          f"old element at {bak / a.element}")


if __name__ == "__main__":
    sys.exit(main())
