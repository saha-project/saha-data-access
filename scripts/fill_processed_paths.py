#!/usr/bin/env python3
"""
fill_processed_paths.py
Fill samples.s3_processed_path from the uploaded stores.

Paths are written relative to the data root ("processed/<record>/<store>.zarr"),
so the same table works for the reviewer copy (s3://saha-public on Wasabi) and the
AWS Open Data bucket (s3://saha-open-data) once it is live.

    python scripts/fill_processed_paths.py [DATA_ROOT]   # default: Wasabi reviewer copy
"""
import sys
from pathlib import Path
import fsspec
import pandas as pd

REPO = Path(__file__).resolve().parent.parent
root = sys.argv[1] if len(sys.argv) > 1 else "s3://saha-public"
opts = {"anon": True, "client_kwargs": {"endpoint_url": "https://s3.wasabisys.com"}} \
    if root.startswith("s3://saha-public") else ({"anon": True} if root.startswith("s3://") else {})
fs, base = fsspec.core.url_to_fs(root, **opts)

stores = {}
for p in fs.glob(f"{base}/processed/*/*.zarr"):
    name = p.rsplit("/", 1)[-1][: -len(".zarr")]
    sid = name[: -len("_RNA")] if name.endswith("_RNA") else name   # 25 Xenium stores carry _RNA
    assert sid not in stores, f"two stores for {sid}: {stores[sid]}, {p}"
    stores[sid] = p[len(base) + 1:]

f = REPO / "data/samples.parquet"
s = pd.read_parquet(f)
missing = sorted(set(s.sample_id) - set(stores))
assert not missing, f"no store for {len(missing)} samples: {missing[:5]}"
s["s3_processed_path"] = s.sample_id.map(stores)
s.to_parquet(f, index=False)
print(f"filled {len(s)} paths from {root}; {len(set(stores) - set(s.sample_id))} extra stores ignored")
