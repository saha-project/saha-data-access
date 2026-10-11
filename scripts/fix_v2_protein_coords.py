#!/usr/bin/env python3
"""
fix_v2_protein_coords.py
Put the v2 merged CosMx protein table's obs centroids in the object's frame.

In SAHA_CosMx_PROS_A2 the protein->RNA registration was applied to the protein
polygons and to obsm['spatial'/'global'] (both within ~3 px of the polygons),
but obs CenterX/Y_global_px still hold the protein run's own frame -- a median
18,733 px away. A3 needed no shift, so its columns are already right.

The raw values are kept, renamed to CenterX/Y_prt_run_px, and
CenterX/Y_global_px are refilled from obsm['global'], so every table in v2
shares one column schema and one frame.

Never in place: copy to --out as `<name>.partial`, rewrite the table in the
copy, read back and check, then rename.

Usage (on Cayuga):
    P=~/.venv-sd08/bin/python
    $P scripts/fix_v2_protein_coords.py --src .../v2_objects --out .../v2_objects_fixed SAHA_CosMx_PROS_A2
"""

import argparse
import shutil
import sys
import warnings
from pathlib import Path

import numpy as np

TABLE = "protein"
RENAME = {"CenterX_global_px": "CenterX_prt_run_px", "CenterY_global_px": "CenterY_prt_run_px"}


def fix(src: Path, out_dir: Path):
    warnings.filterwarnings("ignore")
    from spatialdata import read_zarr
    from spatialdata.models import TableModel

    final = out_dir / src.name
    if final.exists():
        print(f"{src.name}: already fixed, skipping")
        return
    tmp = out_dir / (src.name + ".partial")
    if tmp.exists():
        shutil.rmtree(tmp)  # our own half-finished copy, never a source
    shutil.copytree(src, tmp)

    sd = read_zarr(tmp)
    t = sd.tables[TABLE]
    if "CenterX_prt_run_px" in t.obs:
        sys.exit(f"{src.name}: already has CenterX_prt_run_px -- refusing to rewrite")
    g = np.asarray(t.obsm["global"])[:, :2].astype(float)
    a = t.uns["spatialdata_attrs"]
    c = sd.shapes[a["region"]].loc[t.obs[a["instance_key"]].values].geometry.centroid
    poly = np.c_[c.x, c.y]
    if np.median(np.hypot(*(g - poly).T)) > 10:
        sys.exit(f"{src.name}: obsm['global'] does not match the polygons; not using it")

    new = t.copy()
    raw = new.obs[list(RENAME)].copy()
    new.obs = new.obs.rename(columns=RENAME)
    new.obs.insert(new.obs.columns.get_loc("CenterX_prt_run_px"), "CenterX_global_px",
                   np.rint(g[:, 0]).astype(raw["CenterX_global_px"].dtype))
    new.obs.insert(new.obs.columns.get_loc("CenterY_prt_run_px"), "CenterY_global_px",
                   g[:, 1].astype(raw["CenterY_global_px"].dtype))
    attrs = new.uns.pop("spatialdata_attrs")
    new = TableModel.parse(new, region=attrs["region"], region_key=attrs["region_key"],
                           instance_key=attrs["instance_key"])   # validated before the delete
    n, x_sum = t.n_obs, float(t.X.sum())

    sd.tables[TABLE] = new
    sd.delete_element_from_disk(TABLE)
    sd.write_element(TABLE)

    back = read_zarr(tmp)
    b = back.tables[TABLE]
    d = np.hypot(b.obs.CenterX_global_px.to_numpy(float) - poly[:, 0],
                 b.obs.CenterY_global_px.to_numpy(float) - poly[:, 1])
    assert b.n_obs == n and np.isclose(float(b.X.sum()), x_sum)
    assert (b.obs.CenterX_prt_run_px.values == raw.CenterX_global_px.values).all()
    assert np.median(d) < 10, np.median(d)
    assert set(back.tables) == set(sd.tables) and back.tables["rna"].n_obs == sd.tables["rna"].n_obs
    tmp.rename(final)
    print(f"{src.name}: {TABLE} CenterX/Y_global_px now {np.median(d):.1f} px from polygons "
          f"(raw kept as CenterX/Y_prt_run_px)")


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
