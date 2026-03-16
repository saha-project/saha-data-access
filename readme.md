# SAHA Data Access

Data schemas, metadata infrastructure, and access documentation for the **Spatial Atlas of Human Anatomy (SAHA)** — hosted on the AWS Open Data Program.

## About SAHA

SAHA is the first multimodal, subcellular-resolution spatial reference atlas of healthy human tissues. It profiles 15M+ cells from 100+ donors across 16 tissue types using multiple spatial platforms (CosMx, Xenium, GeoMx, RNAscope), capturing matched RNA (up to 19,000 genes), protein (67 markers), and histological data at ~50 nm resolution.

- **Website**: [saha-project.org](https://www.saha-project.org/)
- **Preprint**: [bioRxiv 2025.06.16.658716](https://www.biorxiv.org/content/10.1101/2025.06.16.658716v3)
- **Data deposits**: [zenodo.org/communities/saha](https://zenodo.org/communities/saha)

## Repository Contents
```
schemas/          # JSON Schema definitions for metadata tables
scripts/          # Format conversion and validation tools
docs/             # Data dictionary, quick start, access tiers
tutorials/        # Jupyter notebooks for analysis on AWS
```

## Metadata Tables

| Table | Description |
|-------|-------------|
| `samples` | One row per sample — links to raw/processed data on S3, organ, condition, platform, QC status |
| `donors` | Donor demographics — age, sex, ethnicity, tissue source |
| `runs` | Acquisition run metadata — platform, institution, date, panel |
| `panels` | Gene/protein panel definitions per platform and plex level |

## Data Organization on S3
```
saha-open-data/
├── raw/{platform}/{institution}/{run_id}/{sample_id}/
├── processed/by_sample/
├── processed/by_organ/
├── images/
├── metadata/
└── visualization/
```

Raw data is organized by **platform → generation institution → run → sample** and is immutable. Access is metadata-driven: query the metadata tables to find samples by organ, platform, cell type, or donor attributes rather than browsing folders.

## Quick Start
```python
# Query metadata to find samples (via Athena or locally)
import pandas as pd
samples = pd.read_parquet("metadata/samples.parquet")
colon = samples.query("organ == 'colon' and qc_pass == True")

# Load a processed object
import scanpy as sc
adata = sc.read_h5ad(colon.iloc[0]["s3_processed_path"])
```

## Contributing Data

Institutions contributing spatial omics data to SAHA should see [docs/CONTRIBUTING.md](docs/CONTRIBUTING.md) for the submission process, required metadata fields, and validation instructions.

## License

Metadata and code in this repository: MIT. SAHA datasets are licensed under CC-BY-NC-ND 4.0. See [docs/ACCESS_TIERS.md](docs/ACCESS_TIERS.md) for details.

## Citation

> Park J, et al. Spatial Atlas of Human Anatomy (SAHA). in revision. Preprint: bioRxiv 2025.06.16.658716v3.

## Contact

SAHA Consortium — [saha-project.org](https://www.saha-project.org/)
