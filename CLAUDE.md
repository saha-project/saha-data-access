# SAHA Data Infrastructure

## Project Goal
Build the metadata database, schema definitions, format conversion 
scripts, and documentation for hosting SAHA spatial omics data on 
AWS Open Data Program (S3 + Glue Catalog + Athena).

## Data Architecture
- Raw data organized by: platform → generation_institution → run_id → sample_id
- Metadata as Parquet files queryable via Athena
- Four metadata tables: samples, donors, runs, panels
- Tiered access: open (CC-BY 4.0), registered, controlled (DUA)

## Key Metadata Tables

### samples.parquet
One row per sample. Columns: sample_id, run_id, platform, 
generation_institution, tissue_source_institution, organ, condition, 
anatomical_region, donor_id, panel_name, panel_plex, n_cells, n_fovs, 
qc_pass, processing_date, s3_raw_prefix, s3_processed_path

### donors.parquet
One row per donor. Columns: donor_id, age, sex, ethnicity, 
comorbidities, tissue_source_institution, tissue_source_country

### runs.parquet
One row per acquisition run. Columns: run_id, platform, instrument_id, 
generation_institution, run_date, panel_name, panel_version, n_samples, 
qc_summary, s3_raw_prefix

### panels.parquet
One row per panel config. Columns: panel_name, platform, plex, 
gene_list (array), protein_list (array), version

## Current Data
- Primarily from Cornell, CosMx platform
- Existing master manifest is a CSV at Master_Sample_Manifest.csv
- Panels: CosMx 1000-plex, 6000-plex, 19000-plex; Xenium; G4X
- ~15M cells across GI and immune organs from 100+ donors

## File Formats
- Tabular data: CSV → Parquet (with partitioning by organ, platform)
- Images: TIFF/OME-TIFF → Cloud-Optimized GeoTIFF or Zarr
- Analysis objects: RDS (Seurat), H5AD (AnnData)
- Metadata: CSV/JSON → Parquet

## Tech Stack
- Python (pandas, pyarrow for Parquet conversion)
- AWS SDK (boto3 for S3/Glue operations)
- JSON Schema for validation

## Documentation Outputs
- docs/DATA_DICTIONARY.md (auto-generated from schemas)
- docs/QUICK_START.md
- docs/ACCESS_TIERS.md
- docs/FILE_FORMATS.md
- docs/CONTRIBUTING.md (institutional data submission guide)

## Conventions
- Use snake_case for all column names and file names
- ISO 8601 for dates
- Two-letter country codes (ISO 3166-1 alpha-2)
- Platform names lowercase: cosmx, xenium, g4x, geomx, rnascope
