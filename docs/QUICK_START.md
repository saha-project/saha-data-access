# SAHA Data Quick Start

This guide shows how to find and load SAHA spatial omics data — no account required for open-access data.

> **Status (October 2026):** the AWS bucket `s3://saha-open-data` is not live yet. S3 paths in these
> docs show the planned layout. Until it opens, use the metadata tables in [`data/`](data/).

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

# Colon samples on CosMx
colon = samples.query("organ == 'colon' and platform == 'cosmx'")
print(colon[["sample_id", "run_id", "panel_name", "n_cells"]].head())
```

## Option 2: Amazon Athena (SQL, once the bucket is live)

> **Not available yet.** Once the AWS bucket is live, Athena works only after you register the
> tables in **your own** AWS account (setup steps will be published with the bucket). There is
> no shared `saha` database to connect to. The queries below assume
> that setup; without it, use pandas (Option 1) on the same Parquet files.

Query petabytes of SAHA data with standard SQL — pay only for bytes scanned (~$5/TB).

```sql
-- All colon samples
SELECT sample_id, run_id, n_cells, panel_name
FROM saha.samples
WHERE organ = 'colon';

-- Sample counts per organ/platform
SELECT organ, platform, COUNT(*) AS n_samples, SUM(n_cells) AS total_cells
FROM saha.samples
GROUP BY organ, platform
ORDER BY total_cells DESC;

-- Donors on a slide. There is NO samples.donor_id: most SAHA slides are tissue
-- microarrays carrying 2-28 donor cores, so one acquisition images many donors
-- and identity is a per-CELL property in obs['donor_core']. Joining a scalar
-- donor id is what once collapsed 32 samples onto one fictional donor.
-- (Donor age group, sex and ethnicity are registered-tier, not in these tables.)
SELECT s.sample_id, s.organ, s.n_donors, d.donor_core, d.condition
FROM saha.samples s
JOIN saha.donors d ON s.slide_id = d.slide_id AND s.organ = d.organ
WHERE d.in_release;

-- Which cores were imaged in which acquisition (the authoritative crossing)
--   s3://saha-open-data/metadata/measurements.csv
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
row = samples.query("organ == 'ileum'").iloc[0]

# Open the SpatialData store directly from S3. Nothing is downloaded until you
# touch it, so inspecting a 5 GB object costs a few MB.
import spatialdata
sdata = spatialdata.read_zarr(row["s3_processed_path"])

print(sdata)
# SpatialData object
#   images: morphology            (CosMx)  /  morphology_focus (Xenium)
#   labels: cell_labels                    (CosMx only)
#   shapes: cells                 (CosMx)  /  cell_boundaries, nucleus_boundaries
#   points: transcripts
#   tables: table

table = sdata.tables["table"]
table.obs["donor_core"]   # per-cell donor identity; join to donors.parquet
```

## Load a Processed Seurat Object (R)

```r
library(Seurat)
library(arrow)

# Read metadata
samples <- read_parquet("s3://saha-open-data/metadata/samples/")
row <- subset(samples, organ == "colon")[1, ]

# The release ships SpatialData (OME-NGFF zarr), not .rds. From R, read the
# store with SpatialExperiment/zellkonverter, or pull the Zenodo zip for a
# whole organ if you would rather work locally.
# s3://saha-open-data/processed/<record_slug>/<sample_id>.zarr/   e.g. saha-v1-colon
```

## Explore Spatial Coordinates

```python
import scanpy as sc
import squidpy as sq
import matplotlib.pyplot as plt

sdata = spatialdata.read_zarr(row["s3_processed_path"])
adata = sdata.tables["table"]

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

# Acquisitions covering more than one donor core (most TMA slides do)
multi_donor = samples.query("n_donors > 1")

# Donors appearing in more than one organ: go through donors.parquet, not
# samples -- samples has no donor column by design.
donors = pd.read_parquet("s3://saha-open-data/metadata/donors/",
                         storage_options={"anon": True})
multi_organ = (donors[donors.in_release]
               .groupby("donor_patient").filter(lambda g: g["organ"].nunique() > 1))

# All available platforms
print(samples["platform"].value_counts())
```

## Next Steps

- [DATA_DICTIONARY.md](DATA_DICTIONARY.md) — full column definitions for all four metadata tables
- [ACCESS_TIERS.md](ACCESS_TIERS.md) — how to request registered/controlled-access data
- [FILE_FORMATS.md](FILE_FORMATS.md) — raw file formats per platform
- [CONTRIBUTING.md](CONTRIBUTING.md) — submitting data from your institution
- `tutorials/` — Jupyter notebooks for end-to-end analyses
