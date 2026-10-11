# SAHA TAP -> v1 — handoff (2026-09-14)

## Built and verified on Cayuga

    /athena/project-saha/derived/tap_zarr/       24 objects, 1,762,538 cells
    /athena/project-saha/derived/tap_images_d4/  22 morphology images, 9.8 GB

- `region_key` intact 24/24; every morphology image passed the geometric check
  (3000/3000 centroids in bounds, 900-1250x DNA signal at cells vs random).
- **donor_core on 24/24 acquisitions, 100% of cells, 26 donors**, from the
  recorded FOV->core maps in `Data/CosMx/FOV_Metadata/*.csv`. Those files are
  AUTHORITATIVE: each one's FOV count matches its TAP acquisition exactly
  (APE 49/53, ILE 51/50, COL 60/68, PROS 61/61, Liver_b1 28/43, Liver_b2 80/100).
- Cell types on 12 objects; prostate and BM RNA ship unannotated by decision.

## v1 release tally with TAP included

71 acquisitions, 11,087,435 cells, 163.2 GB, 11 records (4 new: bone-marrow,
liver, crc, prostate). Largest record ileum at 38.1 GB, under the 50 GB cap.
Per-acquisition: `tap_release_breakdown.csv`, `v1_combined_breakdown.csv`.

## Two matching rules, learned the hard way

- **Cell ids**: a true same-acquisition pair matches **100%**, never 75%. Strip
  the `c_<slide>_` prefix ONLY to reconcile an already-established pairing whose
  slide numbering drifted -- never to SEARCH for which export an object belongs
  to. Stripping discards the field that distinguishes slides and manufactures
  60-75% false overlaps (Jiwoon's correction; APE_A9 and ILE_A1 are different
  slides even though both use `c_1_...`).
- **Footprints**: same-slide 0.47-0.89, different 0.13-0.25; false-positives on
  small solid slabs. Always eyeball the overlay.
- **Look for a recorded map before inferring one.** The FOV_Metadata CSVs existed
  the whole time and superseded every inference. They also audited the methods:
  footprint transfer (APE) and Jiwoon's hand annotation (ILE) both agreed 100.0%;
  my A/B/C->1/5/7 colon mapping agreed 23.5% and was wrong.

## Converter bugs fixed (all affect v1 too; each has a test)

1. `cell_key()` hardcoded slide 1 -> multi-slide runs joined zero rows (CRC C17/C18).
2. CRC_C18's tx file has no header row; pandas ate the first transcript as names.
3. CRC declares `ChannelOrder: "Unknown"` -> 7 bogus channels for a 5-channel image.
4. CRC tiles are 3648x5472, not 4256 square; the canvas clipped the right FOVs.
5. `region` is spatialdata's reserved region_key -- writing the CRC pathology
   calls there would have broken every table's link to shapes/cells.
6. Feature names contain spaces (`NF-kB p65`, `Ms IgG1`); whitespace-splitting
   shifted every marker after position 55.
7. `donor_core` wrote bare letters; now `<SLIDE>_<CORE>` so it joins donors.parquet.

## H&E alignment (started, stomach only)

Tool: `st2he-toolkit` (github.com/a663E-36z1120/st2he-toolkit), in
`SAHA/11_Software/GitHub_Repos/`. Prior art: 4 PROS runs in
`SAHA/03_Analyses/Active/st2he_alignment/` -- same-slide gives 0 um residual,
serial 20-80 um, so the residual metric classifies the section type empirically.

H&E images live in `SAHA/00_RawData/HE/` (35 GB, named by run prefix, NOT the
`SAHA_HE_*` convention sample_metadata.csv uses -- that mismatch is why I first
reported them missing). Stomach is `ECJP03_STO_A1.svs`.

Stomach result: fitted 960.9/955.7 px/mm against a true 951.3 from the SVS
header (MPP 0.262788), 100% of cells in bounds, recursive phase moved cells only
0.54 px. Verified at cellular scale on cores 1, 2, 3, 4 -- cells avoid gland
lumens and respect the mucosa/submucosa boundary. Figures in
`st2he_alignment/sto_run/figures/`.

Per-slide recipe:
1. export `cell, fov, x_mm, y_mm` from the zarr on Cayuga
2. build the h5ad LOCALLY (pandas 2) -- the cluster's pandas 3 writes string
   indices as nullable-string groups, which the toolkit's LazyH5ad cannot read
3. `align.py --h5ad ... --sample ... --svs ...`; coordinates must be mm, not px
4. fold-score the cores before picking a QC window (dense != flat; on STO_A1 they
   were anti-correlated and the densest core was the most folded)

### Open on H&E
- **The protein rows of the zoom figures borrow the RNA's H&E affine.** The two
  frames differ by a measured 100 um, and the acquisitions have different FOV
  layouts, so the rows are not strictly comparable. Fix: run align.py separately
  against SAHA_CP_STO_A1_EC03. That also yields two independent alignments of the
  same H&E as a cross-check.
- Attaching H&E as `images/he` still needs the affine inverted to warp the image
  into the CosMx frame; only coordinates are produced today.
- `eval_residual.py` not yet run on stomach.
- Pick ONE y convention for all figures (suggest y-down, matching image and H&E).

## Open, needs Jiwoon

1. `SAHA_CP_LIV_A2_TAP01` donor cores -- assigned by the left/right rule, no FOV
   metadata covers the 78/75-FOV liver protein runs.
2. WCM prostate trio (`SAHA_{CR,CP}_PRCA_UA_EC02`, `SAHA_XR_PRCA_UA_MS01`) absent
   from sample_metadata.csv; also 3 CRC per-slide rows and SAHA_CP_BM_I18_TAP04.
3. ~~`I19-5886` vs the table's `I29-5889`~~ **RESOLVED 2026-09-15**: settled as
   **I19-5889** (cohort table and H&E agree; the TAP export directory name is a
   typo). Sample id, slide id and donor core now read I19-5889 everywhere,
   including inside the object; only the raw directory keeps `BM/I19-5886`.
   Demographics picked up from Table 1: M, 75, elderly.
4. TAP colon protein X holds background-subtracted intensities, NOT raw counts
   (uns.x_is_raw_counts=False) -- the only such object; needs a docs line.
5. CRC channel names are the standard-panel fallback; the pre-commercial
   instrument recorded no dictionary.
6. Release notes must state the TAP runs are pre-commercial-instrument -- that is
   why TAP COL_A1 yields 35,267 cells against v1's 92,636 on the same slide.

## Pre-existing v1 items

- `donor_core` was never propagated into `zenodo_objects`/`aws_objects` on the
  cluster; v1's cores exist only in `zenodo/donor_qc/*.donor_core.csv`.
- No `CellLabels` anywhere in data_TAP, so no raster segmentation; 7 objects have
  circle placeholders, which must be documented as centroid+area, not boundaries.
- The two colon annotations (GI_after_Niche, colon_healthy_rna) agree on only
  54.7% of shared cells.

## Scripts (../../scripts/)

    create_cosmx_zarr.py      flat files -> zarr         + test_cell_key.py
    build_cosmx_from_rds.py   RDS -> zarr                + test_build_from_rds.py
    apply_annotations.py      partial-join obs labels    + test_apply_annotations.py
    stitch_cosmx_images.py    per-FOV TIFF -> morphology + test_stitch_channels.py
    transfer_donor_core.py    spatial core transfer      + test_transfer_donor_core.py
    apply_fovmap.py (cluster) donor_core from FOV maps

Crosswalk: `tap_crosswalk.csv` (25 rows, donor_source column distinguishes
recorded maps from transfers). FOV maps: `donor_assignment/fov_core_map.json`.
