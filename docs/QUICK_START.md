# SAHA Data Quick Start

This guide shows how to find and load SAHA spatial omics data — no account required for open-access data.

## Option 1: Local Parquet (fastest)

Download the metadata tables once, then filter locally.

```bash
# Install dependencies
pip install pandas pyarrow s3fs fsspec
```

```python
import pandas as pd

# Load metadata (directly from S3 — no credentials needed for open data)
samples = pd.read_parquet(
    "s3://saha-open-data/metadata/samples/",
    storage_options={"anon": True},
)
donors  = pd.read_parquet("s3://saha-open-data/metadata/donors/",  storage_options={"anon": True})
runs    = pd.read_parquet("s3://saha-open-data/metadata/runs/",    storage_options={"anon": True})
panels  = pd.read_parquet("s3://saha-open-data/metadata/panels/",  storage_options={"anon": True})

# Find QC-passing colon samples on CosMx
colon = samples.query("organ == 'colon' and platform == 'cosmx' and qc_pass == True")
print(colon[["sample_id", "run_id", "panel_name", "n_cells"]].head())
```

## Option 2: Amazon Athena (SQL, no download)

Query petabytes of SAHA data with standard SQL — pay only for bytes scanned (~$5/TB).

```sql
-- All QC-passing colon samples
SELECT sample_id, run_id, n_cells, panel_name
FROM saha.samples
WHERE organ = 'colon' AND qc_pass = true;

-- Sample counts per organ/platform
SELECT organ, platform, COUNT(*) AS n_samples, SUM(n_cells) AS total_cells
FROM saha.samples
WHERE qc_pass = true
GROUP BY organ, platform
ORDER BY total_cells DESC;

-- Join to donors to get sex and age group
SELECT s.sample_id, s.organ, s.condition, d.sex, d.age_group
FROM saha.samples s
JOIN saha.donors d ON s.donor_id = d.donor_id
WHERE s.qc_pass = true;
```

## Option 3: AWS CLI (browse raw files)

```bash
# List all CosMx runs from Cornell
aws s3 ls s3://saha-open-data/raw/cosmx/cornell/ --no-sign-request

# Download one sample's raw data
aws s3 cp s3://saha-open-data/raw/cosmx/cornell/TAP01/SAHA_CR_COL_A1_TAP01/ \
    ./SAHA_CR_COL_A1_TAP01/ --recursive --no-sign-request
```

## Load a Processed AnnData Object

```python
import scanpy as sc
import pandas as pd

samples = pd.read_parquet("s3://saha-open-data/metadata/samples/",
                          storage_options={"anon": True})

# Pick a sample
row = samples.query("organ == 'ileum' and qc_pass == True").iloc[0]

# Stream the processed H5AD directly from S3
adata = sc.read_h5ad(row["s3_processed_path"],
                     backed="r")   # backed="r" streams without loading all into RAM

print(adata)
# AnnData object with n_obs=42101, n_vars=1000
#   obs: cell_type, organ, condition, donor_id, ...
#   var: gene_ids, ...
#   obsm: X_umap, spatial, ...
```

## Load a Processed Seurat Object (R)

```r
library(Seurat)
library(arrow)

# Read metadata
samples <- read_parquet("s3://saha-open-data/metadata/samples/")
row <- subset(samples, organ == "colon" & qc_pass == TRUE)[1, ]

# Download and load
s3_path <- row$s3_processed_path
local_rds <- tempfile(fileext = ".rds")
aws.s3::save_object(s3_path, file = local_rds, region = "us-east-1")
obj <- readRDS(local_rds)
```

## Explore Spatial Coordinates

```python
import scanpy as sc
import squidpy as sq
import matplotlib.pyplot as plt

adata = sc.read_h5ad(row["s3_processed_path"])

# Plot cell types in tissue space
sq.pl.spatial_scatter(adata, color="cell_type", shape=None, size=2)
plt.savefig("spatial_cell_types.png", dpi=150, bbox_inches="tight")
```

## Common Filters

```python
# All normal tissue samples with ≥ 10,000 cells
large_normal = samples.query("condition == 'normal' and n_cells >= 10000")

# Samples run with the 6K panel
panel_6k = samples.query("panel_plex == 6000")

# Multi-organ samples from a single donor
donor_samples = samples.groupby("donor_id").filter(lambda g: g["organ"].nunique() > 1)

# All available platforms
print(samples["platform"].value_counts())
```

## Next Steps

- [DATA_DICTIONARY.md](DATA_DICTIONARY.md) — full column definitions for all four metadata tables
- [ACCESS_TIERS.md](ACCESS_TIERS.md) — how to request registered/controlled-access data
- [FILE_FORMATS.md](FILE_FORMATS.md) — raw file formats per platform
- [CONTRIBUTING.md](CONTRIBUTING.md) — submitting data from your institution
- `tutorials/` — Jupyter notebooks for end-to-end analyses
