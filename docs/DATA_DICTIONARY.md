# DATA_DICTIONARY.md

> Auto-generated from `schemas/*.json` on 2026-10-10. Edit the JSON Schema
> files to update this document, then re-run
> `python scripts/generate_data_dictionary.py`.

This document describes the four Parquet metadata tables that form the
SAHA spatial omics open-data catalog.  All column names use `snake_case`;
dates follow ISO 8601 (`YYYY-MM-DD`); institution codes and platform names
are lowercase per SAHA conventions.


## Tables

- [samples.parquet](#samplesparquet)

- [donors.parquet](#donorsparquet)

- [runs.parquet](#runsparquet)

- [panels.parquet](#panelsparquet)


---

## samples.parquet

One row per acquisition.  Joins to `runs.parquet` on `run_id` and to `panels.parquet` on `panel_name`.

There is **no** `donor_id` column, and no direct join to `donors.parquet`. Most SAHA slides are tissue microarrays carrying 2-28 donor cores, so a single acquisition images many donors and donor identity is a per-**cell** property. Each released object carries it in `obs['donor_core']`; join that to `donors.parquet.donor_core`:

```python
adata.obs.merge(donors, on="donor_core", how="left")
```

`n_donors` records how many cores an acquisition covers, for filtering without opening the object.


> **Partitioning:** Partitioned by `organ` and `platform`.


| Column | Type | Required | Constraints | Description |
|--------|------|:--------:|-------------|-------------|
| `sample_id` | `string` | yes | pattern: `^SAHA_[A-Z]{2}_[A-Z0-9]+_.+$` | Unique sample identifier derived from folder name (e.g. SAHA_CR_COL_A1_TAP01). |
| `run_id` | `string` | yes |  | Run/batch identifier linking to runs.parquet (e.g. TAP01, EC02, SJJP01). |
| `platform` | `string` | yes | enum: `cosmx`, `xenium`, `g4x`, `geomx`, `rnascope` | Spatial omics instrument platform. |
| `assay_type` | `string | null` |  | enum: `rna`, `protein`, `multiome` | Molecular modality measured (rna or protein). |
| `generation_institution` | `string` | yes | enum: `cornell`, `wcm`, `st_jude`, `msk`, `broad`, `northwestern`, `upitt`, `other` | Institution that performed the data acquisition run. |
| `tissue_source_institution` | `string | null` |  |  | Institution that sourced the tissue (ISO 3166-1 alpha-2 country appended if needed). |
| `organ` | `string` | yes | enum: `appendix`, `bone_marrow`, `colon`, `ileum`, `kidney`, `liver`, `lung`, `lymph_node`, `pancreas`, `prostate`, `rectum`, `spleen`, `stomach`, `thymus`, `other` | Organ of tissue origin. |
| `condition` | `string` | yes | enum: `normal`, `cancer`, `disease`, `other` | Gross tissue condition. |
| `anatomical_region` | `string | null` |  |  | Short tissue-type code from the source manifest (e.g. COL, LIV, APE). |
| `n_donors` | `integer | null` |  | min=1 | Number of distinct donor cores this acquisition covers. These slides are tissue microarrays, so one acquisition can span 1-28 donors; the identities are per-cell in obs `donor_core` and resolve through donors.parquet. There is deliberately no scalar donor_id. |
| `slide_id` | `string | null` |  |  | Instrument slide or section identifier. |
| `panel_name` | `string | null` |  |  | Panel name linking to panels.parquet. |
| `panel_plex` | `integer | null` |  | min=1 | Number of targets (genes or proteins) in the panel. |
| `n_cells` | `integer | null` |  | min=0 | Total cells in the released object, i.e. every cell the instrument segmented. Use n_cells_qc_pass for the QC-filtered subset the manuscript reports. |
| `n_cells_qc_pass` | `integer | null` |  | min=0 | Cells retained by the published QC filtering, matching obs `qc_pass`. Lower than n_cells wherever a published h5ad exists; null where none does. |
| `n_fovs` | `integer | null` |  | min=0 | Number of fields of view (CosMx) or sections (Xenium) in this sample. |
| `processing_date` | `string | null` |  | format: date | Instrument run start date (ISO 8601 YYYY-MM-DD). |
| `slide_prepared_date` | `string | null` |  | format: date | Slide preparation date (ISO 8601 YYYY-MM-DD). |
| `s3_raw_prefix` | `string | null` |  |  | S3 URI prefix for raw instrument output (e.g. s3://saha-open-data/cosmx/wcm/TAP01/SAHA_CR_COL_A1_TAP01/). |
| `s3_processed_path` | `string | null` |  |  | S3 URI for the processed AnnData/Seurat object. |


---

## donors.parquet

One row per donor **core** -- the unit of donor identity on a tissue microarray, not the slide.  Joins to `obs['donor_core']` in the released objects.

`donor_patient` groups cores known to come from the same person (the PDAC slide carries 28 cores from 14 KRAS-typed patients). Cores are never collapsed: each is treated as an independent case.

Demographics come from Table 1 of the manuscript and are null for cores outside the published cohort.


| Column | Type | Required | Constraints | Description |
|--------|------|:--------:|-------------|-------------|
| `donor_id` | `string` | yes |  | Unique donor identifier (de-identified, e.g. SAHA_D001). |
| `donor_core` | `string` | yes |  | Donor core label, `<SLIDE>_<CORE>` (e.g. COL_A1_5). Joins to obs `donor_core` in the released objects and to SampleID in Table 1 of the manuscript. snake_case throughout. |
| `donor_patient` | `string | null` |  |  | Grouping key where two cores are known to be the same person. Populated for the PDAC slide only (28 cores from 14 KRAS-typed patients); cores are never collapsed. |
| `slide_id` | `string | null` |  |  | Slide the core sits on (e.g. PANC_A1). |
| `organ` | `string | null` |  |  | Organ of tissue origin for this core. |
| `condition` | `string | null` |  | enum: `normal`, `cancer`, `disease` | Gross tissue condition for this core. |
| `age` | `integer | null` |  | min=0; max=120 | Controlled tier only; absent from the open donors.parquet. Donor age in years at time of tissue collection. |
| `age_group` | `string | null` |  | enum: `pediatric`, `adult`, `elderly` | Registered tier; absent from the open donors.parquet. Broad age category for access-tier reporting. |
| `sex` | `string | null` |  | enum: `M`, `F`, `unknown` | Registered tier; absent from the open donors.parquet. Biological sex (M/F/unknown). |
| `ethnicity` | `string | null` |  |  | Registered tier; absent from the open donors.parquet. Self-reported ethnicity using NIH standard categories. |
| `in_release` | `boolean` |  |  | Whether any released object carries cells from this core. False for cohort members whose block is held on a slide but which no acquisition covers yet -- they are kept so the table describes the cohort, not just the subset with data. |
| `status_note` | `string | null` |  |  | Why a core has no released data yet (block not acquired, or outside the acquired fields of view). Null for released cores. |


---

## runs.parquet

One row per instrument acquisition run (batch).  Links to `samples.parquet` via `run_id`.


| Column | Type | Required | Constraints | Description |
|--------|------|:--------:|-------------|-------------|
| `run_id` | `string` | yes |  | Unique run/batch identifier (e.g. TAP01, EC02, SJJP01, MS01). |
| `platform` | `string` | yes | enum: `cosmx`, `xenium`, `g4x`, `geomx`, `rnascope` | Spatial omics instrument platform used for this run. |
| `instrument_id` | `string | null` |  |  | Specific instrument serial number or lab-assigned ID. |
| `generation_institution` | `string` | yes | enum: `cornell`, `wcm`, `st_jude`, `msk`, `broad`, `other` | Institution that performed the run. |
| `run_date` | `string | null` |  | format: date | Date the instrument run started (ISO 8601 YYYY-MM-DD). |
| `panel_name` | `string | null` |  |  | Primary panel used in this run (links to panels.parquet). |
| `panel_version` | `string | null` |  |  | Panel lot or version number. |
| `n_samples` | `integer | null` |  | min=1 | Number of tissue samples processed in this run. |
| `instrument_software_version` | `string | null` |  |  | Instrument acquisition software version. |
| `slide_prep_manual` | `string | null` |  |  | NanoString/10x slide preparation manual part number. |
| `instrument_manual` | `string | null` |  |  | Instrument user manual part number. |
| `fiducial_concentration_pct` | `number | null` |  |  | Fiducial concentration used during the run (%, CosMx only). |
| `prehyb_storage_time` | `string | null` |  |  | Pre-hybridization storage time (e.g. '130 min at 4°C', CosMx only). |
| `hybridization_duration_h` | `number | null` |  |  | Hybridization duration in hours (CosMx only). |
| `prerun_overnight_4c` | `boolean | null` |  |  | Whether an optional pre-run overnight at 4°C step was performed (CosMx only). |
| `prebleach_profile` | `string | null` |  |  | Pre-bleaching profile identifier (CosMx only). |
| `cell_segmentation_profile` | `string | null` |  |  | Cell segmentation profile identifier (CosMx only). |
| `qc_summary` | `string | null` |  |  | Free-text QC summary or flag for the run. |
| `s3_raw_prefix` | `string | null` |  |  | S3 URI prefix for all raw data from this run. |


---

## panels.parquet

One row per panel configuration.  `gene_list` and `protein_list` are stored as Parquet list columns.


| Column | Type | Required | Constraints | Description |
|--------|------|:--------:|-------------|-------------|
| `panel_name` | `string` | yes |  | Canonical panel name as used in samples/runs tables (e.g. UCC 1K, 6K Discovery). |
| `platform` | `string` | yes | enum: `cosmx`, `xenium`, `g4x`, `geomx`, `rnascope` | Platform for which this panel was designed. |
| `assay_type` | `string` |  | enum: `rna`, `protein`, `multiome` | Whether the panel measures RNA or protein targets. |
| `plex` | `integer` | yes | min=1 | Total number of targeted analytes (genes + proteins). |
| `version` | `string | null` |  |  | Panel design version or catalog revision. |
| `gene_list` | `array[string] | null` |  |  | List of targeted gene symbols (HGNC). |
| `protein_list` | `array[string] | null` |  |  | List of targeted protein names or antibody clones. |
| `catalog_number` | `string | null` |  |  | Vendor catalog number for this panel. |
| `notes` | `string | null` |  |  | Free-text notes about panel design or modifications. |


---
