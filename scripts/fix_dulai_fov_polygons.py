#!/usr/bin/env python3
"""
fix_dulai_fov_polygons.py
Repair the cell polygons of already-built Dulai CosMx objects whose FOVs were
laid out on the wrong grid (SAHA_CR_IBD_PD_A2_{1..4}_NU02). The export
intermediates are gone, so this applies build_from_seurat.snap_polygons_to_fovs
-- the same function a rebuild would now use -- to a copy of the built object.

Never in place: the store is copied to --out as `<name>.partial`, the `cells`
shapes are rewritten in the copy, the copy is read back and checked (polygon
centroids within tolerance of obs centroids, table untouched), then renamed.

Usage (on Cayuga):
    P=~/.venv-sd08/bin/python
    $P scripts/fix_dulai_fov_polygons.py --src .../dulai_zarr --out .../dulai_zarr_fixed \
        SAHA_CR_IBD_PD_A2_1_NU02 [...]
"""

import argparse
import shutil
import sys
import warnings
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_from_seurat import snap_polygons_to_fovs  # noqa: E402


def centroid_dist(sd):
    o = sd.tables["table"].obs
    c = sd.shapes["cells"].loc[o["cell"].values].geometry.centroid
    return np.hypot(c.x.to_numpy() - o.x_centroid_px.to_numpy(float),
                    c.y.to_numpy() - o.y_centroid_px.to_numpy(float))


def fix(src: Path, out_dir: Path):
    warnings.filterwarnings("ignore")
    from spatialdata import read_zarr
    from spatialdata.models import ShapesModel
    from spatialdata.transformations import get_transformation

    final = out_dir / src.name
    if final.exists():
        print(f"{src.name}: already fixed, skipping")
        return
    tmp = out_dir / (src.name + ".partial")
    if tmp.exists():
        shutil.rmtree(tmp)  # our own half-finished copy, never a source
    shutil.copytree(src, tmp)

    sd = read_zarr(tmp)
    o = sd.tables["table"].obs
    before_med = float(np.median(centroid_dist(sd)))
    shp = sd.shapes["cells"].loc[o["cell"].values]   # row-aligned with obs
    tf = get_transformation(sd.shapes["cells"], get_all=True)
    new, moved = snap_polygons_to_fovs(shp[["geometry"]], o.x_centroid_px.to_numpy(float),
                                       o.y_centroid_px.to_numpy(float), o["fov"].to_numpy())
    if not len(moved):
        sys.exit(f"{src.name}: no FOV is misplaced -- nothing to fix, refusing to rewrite")
    new = ShapesModel.parse(new)   # validated before the delete; carries the source transforms
    assert get_transformation(new, get_all=True) == tf, "transform lost in snapping"
    n_rows, cols = len(o), list(o.columns)

    sd.shapes["cells"] = new
    sd.delete_element_from_disk("cells")
    sd.write_element("cells")

    back = read_zarr(tmp)
    d = centroid_dist(back)
    assert len(back.shapes["cells"]) == len(new) and back.tables["table"].n_obs == n_rows
    assert list(back.tables["table"].obs.columns) == cols
    assert np.median(d) < 10 and np.percentile(d, 95) < 50, (np.median(d), np.percentile(d, 95))
    tmp.rename(final)
    print(f"{src.name}: moved {len(moved)} FOVs; centroid distance median "
          f"{before_med:,.0f} -> {np.median(d):.1f} px, p95 {np.percentile(d, 95):.1f} px")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("samples", nargs="+")
    a = ap.parse_args()
    a.out.mkdir(parents=True, exist_ok=True)
    for s in a.samples:
        fix(a.src / f"{s}.zarr", a.out)


if __name__ == "__main__":
    main()
