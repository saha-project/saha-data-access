# SAHA phase 1 — SpatialData objects

Pre-publication release of the SAHA v1.0 objects (Spatial Atlas of Human Anatomy; preprint
bioRxiv 10.1101/2025.06.16.658716), at full image resolution, open access under CC-BY 4.0.
This copy is the interim host; at publication the same objects move to the AWS Open Data
Program (`s3://saha-open-data`) and Zenodo (records `saha-v1-*`).

```
s3://saha-public/                      endpoint https://s3.wasabisys.com, no credentials needed
├── README.md                          this file
├── metadata/
│   ├── samples.parquet                126 acquisitions (one row per object)
│   ├── donors.parquet                 121 donor cores of the cohort (115 in this release)
│   ├── runs.parquet                   acquisition runs and protocol fields
│   ├── panels.parquet                 gene / protein panels
│   ├── measurements.csv               acquisition x donor core: which cells came from which core
│   └── cohort_coverage.csv            donor cores vs the manuscript's cohort table
├── processed/<record>/<sample_id>.zarr
│   saha-v1-appendix (13)   saha-v1-bone-marrow (7)  saha-v1-colon (6)          saha-v1-crc (3)
│   saha-v1-ileum (27)      saha-v1-liver (6)        saha-v1-lymph-node (2)     saha-v1-pancreas-cosmx (6)
│   saha-v1-pancreas-xenium (9)  saha-v1-prostate (5)  saha-v1-rectum (40)      saha-v1-stomach (2)
├── source_data/                       per-figure-panel Source Data tables (_MANIFEST.tsv lists their origin)
└── docs/
    ├── DATA_DICTIONARY.md  FILE_FORMATS.md
    └── gallery/index.html             one card per object: image, cells, donor cores, cell types, H&E
```

126 objects (CosMx RNA 49, CosMx protein 19, Xenium 58), 12.4M cells, 115 donor cores, 10 organs.
Every object is a zarr v3 SpatialData store.

## What is in each object

- **Table** — `sdata.tables["table"]`: counts (RNA) or per-cell intensities (protein) with `obs`.
- **Donor cores** — `obs['donor_core']` on every cell; most slides are tissue microarrays, so one
  object holds several donors. Join to `metadata/donors.parquet` on `donor_core`;
  `metadata/measurements.csv` gives the cell count per object x core.
- **Cell types** — `obs['cell_type_broad']` (one shared vocabulary) where available, with
  `cell_type_broad_source` saying how each label was made and `cell_type_broad_confidence` for
  transferred labels. The original labels (`cell_type`, `cell_type_detailed`, ...) are kept.
  Rectum (Northwestern IBD) keeps its original labels (`cell_type`, or the vendor
  `gene_expression_graphclust` clusters); a few objects have only placeholder `leiden` /
  `leiden_res*` clusters, which are not cell types. **Treat labels as a starting point and check
  them for your own analysis.**
- **Images** — morphology images at full resolution; H&E where it exists, as `he_same_section`
  (this slide, cell-level), `he_serial_section` (an adjacent cut, tissue-level), or `he_unaligned`
  (a scan whose placement could not be verified, in its own coordinate system). Registration
  provenance and an accuracy note are in `uns['<element>_registration']`.
- **Cells** — polygons and, for RNA, transcript locations.

## Reading

Anonymous access with the AWS CLI (any S3 client works; pass the endpoint):

```bash
E="--no-sign-request --endpoint-url https://s3.wasabisys.com"
aws s3 ls s3://saha-public/processed/ $E                       # list records
aws s3 cp s3://saha-public/metadata/ ./metadata/ --recursive $E
aws s3 sync s3://saha-public/processed/saha-v1-prostate/ ./saha-v1-prostate/ $E
```

Browse every object first in `docs/gallery/index.html`
(https://s3.wasabisys.com/saha-public/docs/gallery/index.html).

```python
import spatialdata as sd
sdata = sd.read_zarr("saha-v1-prostate/SAHA_CR_PRCA_UA_EC02.zarr")
adata = sdata.tables["table"]
print(adata.obs[["donor_core", "cell_type_broad"]].value_counts().head())
```

Code: https://github.com/saha-project/saha-2026-pub · schemas and tooling:
https://github.com/saha-project/saha-data-access. Raw instrument output is not in this copy; it is
released with the AWS Open Data bucket.

Contact Jiwoon Park, Ph.D. for any questions/suggestions.
