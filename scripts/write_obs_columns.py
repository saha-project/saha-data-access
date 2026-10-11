#!/usr/bin/env python3
"""
write_obs_columns.py
ADD columns from a sidecar parquet (index = obs_names) to a release table's obs, in place.

Nothing existing is deleted or rewritten (the table delete-then-write destroyed two objects
before; hardlinked copies hit the file-count quota): each new column is written as a new array
in the obs group, then appended to obs' column-order, then the store's consolidated metadata is
rebuilt so readers that trust it see the columns. Root and obs metadata are backed up first.
Verified by reading the store back with spatialdata: every pre-existing obs column is
bit-identical (hashed), the new columns equal the sidecar.

Usage (on Cayuga):
    P=~/.venv-sd08/bin/python
    $P scripts/write_obs_columns.py --store <sample>.zarr --sidecar <sample>.cell_type_broad.parquet

--table picks the table in multi-table stores (v2 CosMx: rna, protein; default the first).
--replace-null lets a sidecar column overwrite an existing obs column ONLY if that column is
null in every cell (v2 PROS_A3 donor_core): nothing can be lost, it keeps its column-order slot,
and its old array is backed up.
--replace-from VALUE does the same for a column that holds VALUE in every cell (a constant that
was assigned wrongly, e.g. the Xenium donor_core suffix rule); the old value is fully known.
"""

import argparse
import shutil
import sys
import warnings
from pathlib import Path

import anndata as ad
import pandas as pd
import zarr

warnings.filterwarnings("ignore")
BACKUP = Path("/athena/masonlab/scratch/users/jip2007/release_qc/obs_write_backup")


def obs_hash(obs):
    return pd.util.hash_pandas_object(obs.astype(object).where(obs.notna(), "<NA>").astype(str), index=True).sum()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--store", type=Path, required=True)
    ap.add_argument("--sidecar", type=Path, required=True)
    ap.add_argument("--table")
    ap.add_argument("--replace-null", action="store_true")
    ap.add_argument("--replace-from", metavar="VALUE")
    a = ap.parse_args()
    store = a.store.resolve()
    tname = a.table or list(zarr.open_group(str(store / "tables"), mode="r").keys())[0]
    og_path = store / "tables" / tname / "obs"
    og = zarr.open_group(str(og_path), mode="a")
    obs = ad.io.read_elem(og)
    new = pd.read_parquet(a.sidecar)
    clash = [c for c in new.columns if c in obs.columns]
    replaceable = (a.replace_null and all(obs[c].isna().all() for c in clash)) or \
        (a.replace_from is not None and all((obs[c].astype(str) == a.replace_from).all() for c in clash))
    if clash and not replaceable:
        sys.exit(f"{store.name}: obs already has {clash} (not all-null / not all {a.replace_from!r}) -- refusing")
    keep = [c for c in obs.columns if c not in clash]
    if not new.index.equals(obs.index):
        if set(new.index) != set(obs.index) or len(new) != len(obs):
            sys.exit(f"{store.name}: sidecar cells do not match obs ({len(new)} vs {len(obs)}) -- refusing")
        new = new.loc[obs.index]                       # same cells, other order (another tier's copy)
    before = obs_hash(obs[keep])

    bak = BACKUP / f"{store.parent.name}__{store.name}" / tname if a.table else BACKUP / f"{store.parent.name}__{store.name}"
    bak.mkdir(parents=True, exist_ok=True)
    shutil.copy2(store / "zarr.json", bak / "root.zarr.json")
    shutil.copy2(og_path / "zarr.json", bak / "obs.zarr.json")
    for c in clash:
        shutil.copytree(og_path / c, bak / f"obs.{c}", dirs_exist_ok=True)

    for c in new.columns:                               # arrays first: column-order still old if this dies
        ad.io.write_elem(og, c, new[c].values if not isinstance(new[c].dtype, pd.CategoricalDtype) else new[c].array)
    og.attrs["column-order"] = list(og.attrs["column-order"]) + [c for c in new.columns if c not in clash]
    cols = list(obs.columns) + [c for c in new.columns if c not in clash]
    zarr.consolidate_metadata(str(store))

    from spatialdata import read_zarr
    t = read_zarr(store).tables[tname]
    assert t.n_obs == len(obs), "n_obs changed"
    assert list(t.obs.columns) == cols, "column order wrong"
    assert obs_hash(t.obs[keep]) == before, "a pre-existing obs column changed"
    for c in new.columns:
        got, want = t.obs[c], new[c]
        same = (got.astype(object).fillna("<NA>").astype(str).values == want.astype(object).fillna("<NA>").astype(str).values).all() \
            if not pd.api.types.is_float_dtype(want) else ((got.values == want.values) | (pd.isna(got.values) & pd.isna(want.values))).all()
        assert same, f"{c} differs after write"
    vc = t.obs[new.columns[0]].astype(object).fillna("NA").value_counts(normalize=True).head(5).round(3).to_dict()
    print(f"{store.parent.name}/{store.name}[{tname}]: added {list(new.columns)} (replaced {clash}); verified; {vc}")


if __name__ == "__main__":
    main()
