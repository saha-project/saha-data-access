# SAHA Data Quick Start

How to find and load SAHA spatial omics data. Open-access data need no account.

> **Status (November 2026):** the AWS Open Data bucket `s3://saha-open-data` is not live yet.
> A copy with the same layout is readable now at `s3://saha-public` (Wasabi; anonymous read).
> Every example below takes one data root, so switching later changes a single line.

```bash
pip install pandas pyarrow s3fs fsspec zarr anndata scanpy
```

## 1. Pick the data root

```python
import os

DATA_ROOT = os.environ.get("SAHA_DATA_ROOT", "s3://saha-public")
if DATA_ROOT.startswith("s3://saha-public"):
    S3_OPTS = {"anon": True, "client_kwargs": {"endpoint_url": "https://s3.wasabisys.com"}}
elif DATA_ROOT.startswith("s3://"):
    S3_OPTS = {"anon": True}          # s3://saha-open-data, once live
else:
    S3_OPTS = {}                      # a local copy with the same layout
```

Layout under the root:

```
metadata/                  samples.parquet, donors.parquet, runs.parquet, panels.parquet, measurements.csv
processed/<record>/        one SpatialData store per acquisition, e.g. processed/saha-v1-colon/SAHA_CR_COL_A1_TAP01.zarr
```

The same four tables are also in this repository's [`data/`](../data/) folder.

## 2. Query the metadata

```python
import pandas as pd

samples = pd.read_parquet(f"{DATA_ROOT}/metadata/samples.parquet", storage_options=S3_OPTS)
donors  = pd.read_parquet(f"{DATA_ROOT}/metadata/donors.parquet",  storage_options=S3_OPTS)

colon = samples.query("organ == 'colon' and platform == 'cosmx'")
print(colon[["sample_id", "panel_name", "n_cells", "s3_processed_path"]])
```

`s3_processed_path` is relative to the data root. Most slides are tissue microarrays, so donor
identity is a per-cell property (`obs['donor_core']`); join `donors` on `donor_core`.

## 3. Load one acquisition's cell table

Reading only the table avoids downloading images (stores with images are 2–6 GB).

```python
import anndata as ad
import zarr

row = samples.set_index("sample_id").loc["SAHA_CR_COL_A1_TAP01"]
store = zarr.storage.FsspecStore.from_url(f"{DATA_ROOT}/{row.s3_processed_path}",
                                          storage_options=S3_OPTS, read_only=True)
adata = ad.io.read_elem(zarr.open_group(store, mode="r")["tables/table"])
if "raw" in adata.layers:          # SAHA Xenium stores keep counts in layers['raw']
    adata.X = adata.layers["raw"]
print(adata)
```

## 4. Open the full SpatialData object (images, shapes)

`spatialdata.read_zarr` does not open remote stores, so copy the store first:

```bash
aws s3 sync s3://saha-public/processed/saha-v1-colon/SAHA_CR_COL_A1_TAP01.zarr ./SAHA_CR_COL_A1_TAP01.zarr \
    --no-sign-request --endpoint-url https://s3.wasabisys.com
```

```python
import spatialdata
sdata = spatialdata.read_zarr("SAHA_CR_COL_A1_TAP01.zarr")
```

## Amazon Athena (once the AWS bucket is live)

Athena will work only after you register the tables in your own AWS account; setup steps will be
published with the bucket. There is no shared `saha` database.

## Access tiers

Open and registered tiers are CC-BY 4.0; controlled-tier data need a signed DUA. Raw instrument
output is not in the public copy. See [ACCESS_TIERS.md](ACCESS_TIERS.md).

Worked examples: [tutorials/](../tutorials/).
