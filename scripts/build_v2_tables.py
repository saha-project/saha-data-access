#!/usr/bin/env python3
"""
build_v2_tables.py
Write the phase 2 (v2) metadata tables into data/v2/.

v2 is a SEPARATE release from v1, not an extension of it: a later publication
after more tissue accumulates. It therefore gets its own tables rather than a
`release` column on v1's, and v1's files are never touched by this script.

v2 is also PRE-PUBLICATION and staged privately. It is not deposited to Zenodo
and not uploaded to any bucket until that is explicitly decided, so unlike v1
there is no 4x-downsampled Zenodo tier, no zip packaging and no 50 GB cap --
one full-resolution tier is the whole release.

Provenance
----------
Every number below was read off the built objects on Cayuga (2026-09-26), not
copied from a directory name or a tracker. The directory names in
`2026_SAHA_data` are actively misleading and must not be parsed:

  * `20260616_SAHA_CR_PROS_A2_EC05/` contains BOTH the A2 and A3 Seurat objects,
    for both RNA and protein. Its name mentions only A2.
  * `20260427_SAHA_XR_PROS_A2_UP01/` lists region 0108728 first, but that region
    is A3, not A2. The mapping below comes from cell counts, cross-checked
    against the vendor QC workbook:
        ML108208 -> A2, 305,676 cells
        ML108728 -> A3, 477,932 cells
  * CosMx `slide_ID` in obs is an internal index that runs OPPOSITE to the SAHA
    slide name (A2 -> slide_ID 2, A3 -> slide_ID 1). Never key off it.

What is deliberately left null
------------------------------
`condition` -- one slide is cancer and one is normal, and which is which is not
yet established. Transcriptomic markers do NOT separate them: A2's own 31 cores
span 5.29-14.10% AMACR-positive while the A2-vs-A3 slide difference is only
8.58% vs 11.20%, i.e. the between-slide difference is smaller than the
within-slide spread on a single slide. KLK3 ranges 2.5-72.5% across A2's cores,
so apparent marker shifts track epithelial content per core, not biology.
Resolving this needs the H&E in `processed/he/` or the blocks' pathology record.

`n_donors` -- populated for all four CosMx acquisitions. On A2, `Core_ID` came
annotated only on SAHA_CR_PROS_A2_EC05 (the RNA study); SAHA_CP_PROS_A2_EC05
earned its 31 by transfer from that RNA table, same slide and shared frame,
verified at 2.11 um median and inspected as a figure
(`data/v2/qc/v2_core_transfer_qc.png`).
A3: `Core_ID` is EMPTY in both AtoMx exports, so cores were recovered
GEOMETRICALLY (2026-10-09, `derive_cores_geometric.py`): connected tissue
islands of both tables' cell centroids, two broken punches joined by hand after
checking the same-section H&E -> 38 cores, matching the H&E punch layout row by
row. Labels are `PROS_A3_geo01..38` in reading order -- grouping only, NOT
AtoMx numbering and NOT donor identity. The method recovered A2's 31 AtoMx cores
with one merge (touching cores 1+8) and one split, so it is validated, not
assumed. n_donors = 38 assumes one donor per punch, as A2's 31 does. The Xenium
objects have no core column at all. Writing a number without that check would
repeat the v1 failure where a slide got collapsed onto one fictional donor.

RESOLVED 2026-09-26: the protein panel is v1's `IO Protein (64-plex)`. The 69
variables are 64 antibody targets (two of them the isotype controls Ms IgG1 and
Rb IgG, which ship with the panel) plus five `Channel-*` entries -- DNA, PanCK,
Membrane, CD45, G -- which are morphology imaging channels, not targets. Counted
from the variable names, not inferred from the total.

Usage:
    python scripts/build_v2_tables.py [--out data/v2] [--dry-run]
"""

import argparse
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parent.parent

# --------------------------------------------------------------------------
# Verified facts. cells/genes/fovs read from the objects; dates and panels
# from experiment.xenium and the acquisition directories.
# --------------------------------------------------------------------------

SAMPLES = [
    # sample_id,               run,    platform, assay,     slide, region,     cells,  fovs, panel,        plex,  n_donors
    ("SAHA_XR_PROS_A2_UP01", "UP01", "xenium",  "rna",     "PROS_A2", "ML108208", 305676, None, "Xenium 5K",  5001,  None),
    ("SAHA_XR_PROS_A3_UP01", "UP01", "xenium",  "rna",     "PROS_A3", "ML108728", 477932, None, "Xenium 5K",  5001,  None),
    ("SAHA_CR_PROS_A2_EC05", "EC05", "cosmx",   "rna",     "PROS_A2", None,       174190,  185, "19K",       19000,    31),
    ("SAHA_CR_PROS_A3_EC05", "EC05", "cosmx",   "rna",     "PROS_A3", None,       214435,  220, "19K",       19000,    38),
    ("SAHA_CP_PROS_A2_EC05", "EC05", "cosmx",   "protein", "PROS_A2", None,       221374,  185, "IO Protein (64-plex)", 64, 31),
    ("SAHA_CP_PROS_A3_EC05", "EC05", "cosmx",   "protein", "PROS_A3", None,       289773,  220, "IO Protein (64-plex)", 64, 38),
]

RUNS = [
    # run_id, platform, instrument, institution, date,         panel,       n_samples, sw
    ("UP01", "xenium", "XETG00281", "upitt", "2026-04-22", "Xenium 5K", 2, "4.0.1.4"),
    ("EC05", "cosmx",  None,        "wcm",   "2026-06-29", "19K",       4, None),
]

# The 31 donor cores on PROS_A2, the only slide whose core identity is recorded.
# Core_ID 1..31 in the RNA table; Composite_Core_ID is PROS_A2_RNA_<n>. Named
# here in v1's donor_core style (SLIDE_<n>) so the two releases read alike.
A2_CORES = list(range(1, 32))


def build_samples() -> pd.DataFrame:
    rows = []
    for (sid, run, plat, assay, slide, region, cells, fovs, panel, plex, ndon) in SAMPLES:
        rows.append({
            "sample_id": sid,
            "run_id": run,
            "platform": plat,
            "assay_type": assay,
            "generation_institution": "upitt" if run == "UP01" else "wcm",
            "tissue_source_institution": None,
            "organ": "prostate",
            "condition": None,          # see module docstring -- unresolved
            "anatomical_region": region,
            "n_donors": ndon,
            "slide_id": slide,
            "panel_name": panel,
            "panel_plex": plex,
            "n_cells": cells,
            "n_cells_qc_pass": None,
            "n_fovs": fovs,
            "processing_date": "2026-04-22" if run == "UP01" else "2026-06-29",
            "slide_prepared_date": None,
            "s3_raw_prefix": None,
            "s3_processed_path": None,
        })
    return pd.DataFrame(rows)


def build_runs() -> pd.DataFrame:
    return pd.DataFrame([{
        "run_id": r, "platform": p, "instrument_id": inst,
        "generation_institution": site, "run_date": date,
        "panel_name": panel, "panel_version": None, "n_samples": n,
        "instrument_software_version": sw,
        "qc_summary": None, "s3_raw_prefix": None,
    } for (r, p, inst, site, date, panel, n, sw) in RUNS])


def build_panels() -> pd.DataFrame:
    """Only panels actually used by v2, with their measured plex."""
    return pd.DataFrame([
        {"panel_name": "Xenium 5K", "platform": "xenium", "assay_type": "rna",
         "plex": 5001, "version": "hAtlas_v1.1", "catalog_number": None,
         "notes": "Xenium Human 5K Pan Tissue & Pathways Panel; 5001 predesigned "
                  "targets, 0 custom. Same panel row as v1."},
        {"panel_name": "19K", "platform": "cosmx", "assay_type": "rna",
         "plex": 19000, "version": None, "catalog_number": None,
         "notes": "19,244 variables measured in the v2 objects."},
        {"panel_name": "IO Protein (64-plex)", "platform": "cosmx",
         "assay_type": "protein", "plex": 64, "version": None,
         "catalog_number": None,
         "notes": "Same panel row as v1. The v2 objects carry 69 variables: "
                  "64 targets (incl. isotype controls Ms IgG1, Rb IgG) plus 5 "
                  "Channel-* morphology imaging channels (DNA, PanCK, Membrane, "
                  "CD45, G), which are not antibody targets."},
    ])


def build_donors() -> pd.DataFrame:
    """Donor cores. PROS_A2 from AtoMx Core_ID; PROS_A3 recovered geometrically."""
    rows = [{
        "donor_id": None,
        "donor_core": f"PROS_A2_{n}",
        "donor_patient": None,
        "slide_id": "PROS_A2",
        "organ": "prostate",
        "condition": None,
        "age": None, "age_group": None, "sex": None, "ethnicity": None,
        "in_release": False,        # pre-publication, staged privately
        "status_note": "Core identity from Core_ID/Composite_Core_ID in the "
                       "SAHA_CR_PROS_A2_EC05 RNA table. Demographics not yet "
                       "linked; condition unresolved at slide level.",
    } for n in A2_CORES]
    rows += [{
        "donor_id": None, "donor_core": f"PROS_A3_geo{n:02d}", "donor_patient": None,
        "slide_id": "PROS_A3", "organ": "prostate", "condition": None,
        "age": None, "age_group": None, "sex": None, "ethnicity": None,
        "in_release": False,
        "status_note": "Core grouping recovered geometrically (Core_ID empty in AtoMx): "
                       "tissue islands checked against the same-section H&E. Reading-order "
                       "label, not an AtoMx Core_ID; donor identity and demographics unlinked.",
    } for n in range(1, 39)]
    return pd.DataFrame(rows)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", type=Path, default=REPO / "data/v2")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    tables = {"samples": build_samples(), "donors": build_donors(),
              "runs": build_runs(), "panels": build_panels()}

    s = tables["samples"]
    assert len(s) == 6, f"expected 6 v2 samples, got {len(s)}"
    assert s.sample_id.is_unique, "duplicate sample_id"
    assert set(s.run_id) <= {r[0] for r in RUNS}, "sample references unknown run"
    # The whole point of v2's donor handling: no scalar donor_id anywhere.
    assert "donor_id" not in s.columns, "samples must not carry a scalar donor_id"
    # 305,676 + 477,932 + 174,190 + 214,435 + 221,374 + 289,773
    assert s.n_cells.sum() == 1683380, f"cell total changed: {s.n_cells.sum()}"

    for name, df in tables.items():
        print(f"{name:9s} {len(df):3d} rows  {list(df.columns)[:6]}...")
    print(f"\n6 acquisitions, {s.n_cells.sum():,} cells, "
          f"{s.n_donors.notna().sum()}/6 with resolved donor counts, "
          f"{s.condition.notna().sum()}/6 with resolved condition")

    if args.dry_run:
        print("\n[dry-run] nothing written")
        return 0

    args.out.mkdir(parents=True, exist_ok=True)
    for name, df in tables.items():
        p = args.out / f"{name}.parquet"
        df.to_parquet(p, index=False)
        print(f"wrote {p}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
