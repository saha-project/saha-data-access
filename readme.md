# SAHA Data Access

Data schemas, metadata infrastructure, and access documentation for the **Spatial Atlas of Human Anatomy (SAHA)** — hosted on the [AWS Open Data Program](https://registry.opendata.aws/).

> **Status (October 2026):** the AWS bucket `s3://saha-open-data` is not live yet. S3 paths in these
> docs show the planned layout. Until it opens, use the metadata tables in [`data/`](data/).

## About SAHA

SAHA is a multimodal, subcellular-resolution spatial reference atlas of human tissues. This release holds 11.2M cells from 94 donor cores across 9 organs, in 71 acquisitions on the CosMx and Xenium platforms, with matched RNA (up to 19,000 genes) and protein (64-67 markers) on the same tissue. The atlas includes both healthy and disease tissues (colorectal cancer, pancreatic ductal adenocarcinoma, inflammatory bowel disease).

Most slides are **tissue microarrays** carrying 2-28 donor cores, so donor identity is a per-**cell** property held in `obs['donor_core']`, not a column on `samples.parquet`. See [docs/DATA_DICTIONARY.md](docs/DATA_DICTIONARY.md).

- **Website**: [saha-project.org](https://www.saha-project.org/)
- **Preprint**: [bioRxiv 2025.06.16.658716](https://www.biorxiv.org/content/10.1101/2025.06.16.658716v3)
- **Data deposits**: [zenodo.org/communities/saha](https://zenodo.org/communities/saha)

## Repository Contents

```
data/             # Metadata Parquet tables (samples, donors, runs, panels)
schemas/          # JSON Schema definitions for each metadata table
scripts/          # Data standardization and validation scripts
docs/             # Data dictionary, quick start, access tiers, file formats
tutorials/        # Jupyter notebooks for querying and analyzing SAHA data
dataset.yaml      # AWS Open Data Registry submission file
```

## Dataset Tally

Current cohort and run counts, with breakdowns by organ, status, assay, panel, and site:
**[View the live tally →](https://saha-project.github.io/saha-data-access/stats.html)** — built from publication Tables 1 & 2; regenerate with `python scripts/generate_stats.py`.

## Metadata Tables

Four Parquet tables are provided in `data/` and mirrored at `s3://saha-open-data/metadata/`.

| Table | Rows | Description |
|-------|------|-------------|
| `samples.parquet` | 126 | One row per acquisition — organ, condition, platform, n_donors, n_cells, QC status, S3 paths |
| `donors.parquet` | 121 | One row per donor **core** — slide, organ, condition (demographics are registered-tier; see [ACCESS_TIERS](docs/ACCESS_TIERS.md)) |
| `runs.parquet` | 19 | One row per acquisition run — instrument, institution, date, panel, protocol fields |
| `panels.parquet` | 8 | Panel configurations — platform, plex, gene/protein lists |

## Data Organization on S3

```
s3://saha-open-data/
├── processed/
│   ├── h5ad/
│   │   ├── cosmx/RNA/        # Organ-level CosMx RNA AnnData (e.g. SAHA_COL_RNA.h5ad)
│   │   ├── cosmx/PRT/        # Organ-level CosMx Protein AnnData
│   │   └── xenium/           # Sample-level Xenium AnnData
│   └── zarr/
│       ├── xenium/           # Xenium SpatialData zarr objects
│       └── cosmx/            # CosMx SpatialData zarr objects
├── raw/
│   ├── cosmx/{sample_id}/    # Raw CosMx flat files (controlled access)
│   └── xenium/{sample_id}/   # Raw Xenium output (controlled access)
├── metadata/                 # Parquet tables (samples, donors, runs, panels)
└── docs/                     # Documentation
```

CosMx processed data is aggregated to organ level (all samples from one organ in one h5ad). Xenium processed data is kept at sample level. Raw data requires registration — see [docs/ACCESS_TIERS.md](docs/ACCESS_TIERS.md).

## Note on Cell Types

Cell types were assigned by a mix of manual and automated methods: expert annotation where it
existed, mapping of earlier reference-based labels onto one shared vocabulary, and label
transfer (logistic regression) from annotated SAHA objects for objects that had no labels.
Each cell records how its label was made in `cell_type_broad_source`; transferred labels also
carry `cell_type_broad_confidence`, and cells without a confident label are `NA`. The original
labels of the source objects (`cell_type`, `cell_type_detailed`, ...) are kept unchanged.

**These labels are a starting point, not ground truth. We strongly recommend evaluating them,
and re-labelling, for your own analysis.**

## Quick Start

**Load metadata locally** (no AWS credentials needed):

```python
import pandas as pd

samples = pd.read_parquet("data/samples.parquet")
colon = samples.query("organ == 'colon'")
print(colon[["sample_id", "n_donors", "n_cells", "s3_processed_path"]])

# donor identity is per cell, inside the objects:
#   adata.obs.merge(donors, on="donor_core", how="left")
```

**Load metadata from S3** (anonymous access):

```python
import pandas as pd

samples = pd.read_parquet(
    "s3://saha-open-data/metadata/samples.parquet",
    storage_options={"anon": True},
)
```

**Open a SpatialData object** (anonymous access):

```python
import spatialdata

sdata = spatialdata.read_zarr(
    "s3://saha-open-data/processed/saha-v1-colon/SAHA_CR_COL_A1_TAP01.zarr"
)
```

**Query the metadata tables** (works today on the copies in `data/`):

```python
import pandas as pd
samples = pd.read_parquet("data/samples.parquet")
samples.groupby(["organ", "platform"]).n_cells.agg(["count", "sum"])
```

Amazon Athena (SQL over S3) will work once the AWS bucket is live, and only after you register the
tables in your own AWS account (setup steps will be published with the bucket); there is no
shared `saha` database to connect to.

See [tutorials/](tutorials/) for full worked examples.

## Scripts

| Script | Purpose |
|--------|---------|
| `standardize_h5ad.py` | Produce public h5ad files from internal objects (whitelist obs columns, add `sample_id` and `donor_core`) |
| `create_cosmx_zarr.py` | Build CosMx SpatialData zarr from raw flat files |
| `ingest_samples.py` | Validate and convert sample manifest → `samples.parquet` |
| `build_donors_table.py` | Build `donors.parquet`, one row per donor core, from the resolved assignments + Table 1 |
| `donor_map.py` | Single source for sample → donor cores; used by ingest, build and validate |
| `release_tally.py` | Generate `zenodo/release_breakdown.csv` and refresh the record split |
| `ingest_runs.py` | Validate and convert run metadata → `runs.parquet` |
| `ingest_panels.py` | Validate and convert panel definitions → `panels.parquet` |
| `validate_release.py` | Cross-check parquet referential integrity and S3 object existence |
| `test_athena.py` | Run validation queries against the Glue/Athena catalog |
| `generate_data_dictionary.py` | Auto-generate `docs/DATA_DICTIONARY.md` from schemas |
| `generate_stats.py` | Auto-generate `docs/stats.html` dataset tally from publication Tables 1 & 2 |

## Contributing Data

Institutions contributing spatial omics data to SAHA should see [docs/CONTRIBUTING.md](docs/CONTRIBUTING.md) for the submission process, required metadata fields, and validation instructions.

## License

Code and schemas in this repository: MIT.
SAHA datasets (open and registered tiers): [CC-BY 4.0](https://creativecommons.org/licenses/by/4.0/). Controlled-tier data are released under a signed DUA, not a public license. See [docs/ACCESS_TIERS.md](docs/ACCESS_TIERS.md) for details on the three access tiers and the SAHA Data Use Statement.

## Citation

> Park J, et al. Spatial Atlas of Human Anatomy (SAHA). in revision. Preprint: bioRxiv 2025.06.16.658716v3.

## Contact

Jiwoon Park — [saha-project.org](https://www.saha-project.org/)
