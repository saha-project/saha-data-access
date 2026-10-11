#!/usr/bin/env python3
"""
link_xenium_tables.py
Make the v1 Xenium tables annotate their cells.

The 25 v1 Xenium objects carry a table with no `spatialdata_attrs`, so nothing
ties a row to a geometry: spatialdata-plot cannot colour cells by a gene or by
donor_core, and `sd.aggregate` / `get_values` fail. The rows are already keyed
by cell id -- `obs.index` equals the `cell_circles` / `cell_boundaries` index
(100% resolved, see qc_release_objects.py) -- so the fix is metadata only:
add `region` and `cell_id` columns and declare them through TableModel.parse.

Never in place. Each store is copied whole to --out, the table is rewritten in
the copy, the copy is read back and compared to the source, and only then is
it renamed from `<name>.partial` to `<name>`. The source directory is untouched;
swapping --out in for --src is a separate, deliberate step.

Usage (on Cayuga, one store per slurm array task):
    P=~/.venv-sd08/bin/python
    $P scripts/link_xenium_tables.py --src .../xenium_zarr --out .../xenium_zarr_linked --index 0
"""

import argparse
import shutil
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

REGION = "cell_circles"


def link(src: Path, out_dir: Path):
    warnings.filterwarnings("ignore")
    from spatialdata import read_zarr
    from spatialdata.models import TableModel

    final = out_dir / src.name
    if final.exists():
        print(f"{src.name}: already linked, skipping")
        return
    tmp = out_dir / (src.name + ".partial")
    if tmp.exists():
        shutil.rmtree(tmp)  # our own half-finished copy, never a source
    shutil.copytree(src, tmp)

    sd = read_zarr(tmp)
    (tname,) = list(sd.tables)
    t = sd.tables[tname]
    if t.uns.get("spatialdata_attrs"):
        sys.exit(f"{src.name}: table is already linked -- nothing to do, refusing to rewrite")
    shapes_ids = pd.Index(sd.shapes[REGION].index.astype(str))
    if not t.obs.index.astype(str).isin(shapes_ids).all():
        sys.exit(f"{src.name}: obs index does not fully resolve to {REGION}; not linking")

    before = {"n_obs": t.n_obs, "cols": list(t.obs.columns), "X_sum": float(t.X.sum()),
              "obsm": sorted(map(str, t.obsm)), "layers": sorted(map(str, t.layers))}
    new = t.copy()
    new.obs["cell_id"] = new.obs.index.astype(str)
    new.obs["region"] = pd.Categorical([REGION] * new.n_obs)
    new = TableModel.parse(new, region=REGION, region_key="region", instance_key="cell_id")

    # Validated before anything is deleted (see project memory: delete-then-write).
    sd.tables[tname] = new
    sd.delete_element_from_disk(tname)
    sd.write_element(tname)

    back = read_zarr(tmp).tables[tname]
    a = back.uns["spatialdata_attrs"]
    assert a["region"] == REGION and a["instance_key"] == "cell_id", a
    assert back.n_obs == before["n_obs"]
    assert list(back.obs.columns) == before["cols"] + ["cell_id", "region"]
    assert np.isclose(float(back.X.sum()), before["X_sum"])
    assert sorted(map(str, back.obsm)) == before["obsm"] and sorted(map(str, back.layers)) == before["layers"]
    assert (back.obs["cell_id"].values == t.obs.index.astype(str).values).all()
    tmp.rename(final)
    print(f"{src.name}: linked {back.n_obs:,} rows to {REGION}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--index", type=int, required=True)
    a = ap.parse_args()
    a.out.mkdir(parents=True, exist_ok=True)
    link(sorted(a.src.glob("*.zarr"))[a.index], a.out)


if __name__ == "__main__":
    main()
