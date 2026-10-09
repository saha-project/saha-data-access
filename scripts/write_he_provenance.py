#!/usr/bin/env python3
"""
write_he_provenance.py
ADD uns['<element>_registration'] (source, method, accuracy_note, ...) to every table of a store
that has an H&E element but no record of where it came from -- the v2 CosMx objects, whose
he_same_section was attached by merge_slide.py at build without one. qc_release_objects.py and
the gallery read source/accuracy_note from here.

Add-only: refuses if the key exists. Root metadata backed up, store re-consolidated, read back.

Usage (on Cayuga):
    $P scripts/write_he_provenance.py --store <obj>.zarr --element he_same_section --json prov.json
"""

import argparse
import json
import shutil
import sys
import warnings
from pathlib import Path

import anndata as ad
import zarr

warnings.filterwarnings("ignore")
BACKUP = Path("/athena/masonlab/scratch/users/jip2007/release_qc/obs_write_backup")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--store", type=Path, required=True)
    ap.add_argument("--element", required=True)
    ap.add_argument("--json", type=Path, required=True)
    a = ap.parse_args()
    store = a.store.resolve()
    rec = json.loads(a.json.read_text())
    key = f"{a.element}_registration"
    assert (store / "images" / a.element).is_dir(), f"no image element {a.element}"
    tables = list(zarr.open_group(str(store / "tables"), mode="r").keys())
    for t in tables:
        if key in zarr.open_group(str(store / "tables" / t / "uns"), mode="r"):
            sys.exit(f"{store.name}[{t}]: uns already has {key} -- refusing")

    bak = BACKUP / f"{store.parent.name}__{store.name}"
    bak.mkdir(parents=True, exist_ok=True)
    shutil.copy2(store / "zarr.json", bak / "root.zarr.json.before_he_provenance")
    for t in tables:
        ad.io.write_elem(zarr.open_group(str(store / "tables" / t / "uns"), mode="a"), key, rec)
    zarr.consolidate_metadata(str(store))

    from spatialdata import read_zarr
    sd = read_zarr(store)
    for t in tables:
        got = {k: (v.item() if hasattr(v, "item") else v) for k, v in dict(sd.tables[t].uns[key]).items()}
        assert got == rec, f"{t}: read-back differs: {got}"
    print(f"{store.name}: {key} written to {tables}; read back OK")


if __name__ == "__main__":
    main()
