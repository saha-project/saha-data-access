"""Relabel an attached H&E (add under the new element name, then remove the old one), or, with
OLD == NEW, replace it with the H&E from another registration.
Usage: relabel_he.py REG_JSON STORE TIER OLD NEW"""
import sys, warnings, logging
from pathlib import Path
warnings.filterwarnings("ignore"); logging.disable(logging.WARNING)
sys.path.insert(0, "/athena/masonlab/scratch/users/jip2007/saha-data-access/scripts")
import zarr
from spatialdata import read_zarr
from attach_he import attach
reg, store, tier, old, new = sys.argv[1:6]
store = Path(store)
sd = read_zarr(store)
assert old in sd.images, list(sd.images)
tname = list(sd.tables)[0]
n_obs, images = sd.tables[tname].n_obs, set(sd.images) - {new}


def drop(name):
    sd = read_zarr(store)
    del sd[name]               # spatialdata refuses to delete an element still backed in memory
    sd.delete_element_from_disk(name)
    uns = zarr.open_group(str(store / "tables" / tname / "uns"), mode="a")
    if f"{name}_registration" in uns:
        del uns[f"{name}_registration"]


if old == new:                 # replace with a different scan: the H&E is re-creatable from its registration
    drop(old)
    attach(Path(reg), store, tier, element=new, in_place=True)
else:
    if new not in sd.images:                                     # a re-run after a failed delete skips this
        attach(Path(reg), store, tier, element=new, in_place=True)  # add first: never without an H&E
    drop(old)
back = read_zarr(store)
assert set(back.images) == (images - {old}) | {new}, set(back.images)
t = back.tables[tname]
assert t.n_obs == n_obs and f"{new}_registration" in t.uns and (old == new or f"{old}_registration" not in t.uns)
print(f"{store.name}: {old} -> {new}; images {sorted(back.images)}; note: {t.uns[f'{new}_registration']['accuracy_note']}")
