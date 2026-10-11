# SAHA phase 1 — SpatialData objects (collaborator early access)

Pre-publication copy of the SAHA v1.0 objects, shared with collaborators for validation before the
public release (Zenodo + AWS Open Data). Please do not redistribute. Phase 1 holds the same objects as
the Zenodo v1.0 records `saha-v1-*`, at full image resolution; phase 2 (`phase2/`) is new prostate data
alongside it, not a new version of it.

```
s3://<SAHA AWS bucket>/phase1/          bucket location to be announced
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
└── docs/
    ├── DATA_DICTIONARY.md  FILE_FORMATS.md  QUICK_START.md  RELEASE_BREAKDOWN.md
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

The AWS bucket name and access instructions will be added here once the bucket is set up.

```bash
aws s3 sync s3://<SAHA AWS bucket>/phase1/processed/saha-v1-prostate/ ./saha-v1-prostate/
```

```python
import spatialdata as sd
sdata = sd.read_zarr("saha-v1-prostate/SAHA_CR_PRCA_UA_EC02.zarr")
adata = sdata.tables["table"]
print(adata.obs[["donor_core", "cell_type_broad"]].value_counts().head())
```

Access is by invitation; phase 1 is not public until the Zenodo / AWS release.

## What we would like checked

- Does each object open, and do cells sit on the tissue in the image and H&E?
- Do cell types and donor cores look right for the tissues you know?
- Do the counts in `metadata/samples.parquet` (`n_cells`) match what you see?
- Anything missing that you expected to find.

Contact Jiwoon Park, Ph.D. for any questions/suggestions.
