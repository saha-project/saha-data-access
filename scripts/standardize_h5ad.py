#!/usr/bin/env python3
"""
standardize_h5ad.py
Produce public-ready h5ad files from internal SAHA h5ads.

Operations performed:
  - Strip internal/intermediate obs columns (whitelist approach)
  - Rename cell-type columns to canonical names:
      Insitutype_Broad   → cell_type
      Insitutype_Labelled → cell_type_detailed
      c_final_cell_type  → cell_type  (CosMx Protein)
  - Add sample_id column (= SAHA_name for CosMx; already present for Xenium)
  - Strip obsm keys that are not spatial / X_umap (e.g. X_pca, X_harmony variants)

Usage — single file:
    python scripts/standardize_h5ad.py \
        --input  /path/to/SAHA_COL_RNA.h5ad \
        --output data/h5ad_public/SAHA_COL_RNA.h5ad

Usage — batch (all organ-level RNA + PRT + Xenium sample-level):
    python scripts/standardize_h5ad.py --batch \
        --src-rna    /path/to/h5ad/RNA \
        --src-prt    /path/to/h5ad/PRT \
        --src-xenium /path/to/h5ad_sample/RNA \
        --out-dir    data/h5ad_public

Requirements:
    pip install anndata
"""

import argparse
import sys
from pathlib import Path

import anndata as ad
import pandas as pd
import numpy as np

# ---------------------------------------------------------------------------
# Public obs column whitelists
# ---------------------------------------------------------------------------

# Columns kept in public CosMx RNA h5ads (present or absent — missing ones silently skipped)
RNA_KEEP = {
    # Cell identity
    "cell_ID", "cell_id", "fov",
    # Spatial (pixels — CenterX/Y_global_px = obsm['spatial'])
    "CenterX_global_px", "CenterY_global_px",
    "CenterX_local_px",  "CenterY_local_px",
    # Morphology
    "Area", "Area.um2", "AspectRatio", "Width", "Height",
    # Anchor staining channels
    "Mean.DAPI", "Max.DAPI",
    "Mean.PanCK", "Max.PanCK",
    "Mean.CD68", "Max.CD68",
    "Mean.Membrane", "Max.Membrane",
    "Mean.CD45", "Max.CD45",
    "Mean.CD8", "Max.CD8",
    "Mean.CD298_B2M", "Max.CD298_B2M",
    "Mean.CD298.B2M", "Max.CD298.B2M",
    "Mean.CK8.18", "Max.CK8.18",
    "Mean.G", "Max.G",
    # Expression QC
    "nCount_RNA", "nFeature_RNA",
    "nCount_negprobes", "nFeature_negprobes",
    "nCount_falsecode", "nFeature_falsecode",
    "n_counts", "n_genes",
    "propNegative", "complexity", "unassignedTranscripts",
    # Cell QC flags
    "QC_flagged", "qcCellsFlagged", "qcFlagsFOV",
    "qcFlagsCellArea", "qcFlagsCellComplex",
    "qcFlagsCellCounts", "qcFlagsCellPropNeg", "qcFlagsRNACounts",
    # Run metadata
    "assay_type", "Panel", "version", "slide_ID",
    # SAHA metadata
    "sample_id",           # added by this script
    "SAHA_name",           # kept as original identifier
    "SAHA_Sample", "SAHA_organcode", "SAHA_batchcode",
    "section_ID", "SAHA_FOV", "SAHA_Slide", "TissueCode",
    # Cell type (canonical names set by RENAME_MAP below)
    "cell_type", "cell_type_detailed",
    # Optional additional annotations (kept if present)
    "celltypist_pred_low_res", "celltypist_pred_low_res_max_p",
    "celltypist_pred_high_res", "celltypist_pred_high_res_max_p",
    "spaVAE_niche",
}

# Columns kept in public CosMx Protein h5ads
PRT_KEEP = {
    # Cell identity
    "cell_ID", "cell_id", "fov",
    # Spatial
    "CenterX_global_px", "CenterY_global_px",
    "CenterX_local_px",  "CenterY_local_px",
    # Morphology
    "Area", "Area.um2", "AspectRatio", "Width", "Height",
    # Anchor staining channels
    "Mean.DAPI", "Max.DAPI",
    "Mean.PanCK", "Max.PanCK",
    "Mean.CD68", "Max.CD68",
    "Mean.Membrane", "Max.Membrane",
    "Mean.CD45", "Max.CD45",
    "Mean.CD3", "Max.CD3",
    "Mean.CK8.18", "Max.CK8.18",
    "Mean.G", "Max.G",
    # Expression QC
    "nCount_RNA", "nFeature_RNA",
    "nCount_negprobes", "nFeature_negprobes",
    "n_counts", "n_genes",
    # Protein-specific QC
    "area.qc", "mean.neg", "negprobe.qc",
    "n_high_quant", "high.express.qc",
    "n_low_quant", "low.express.qc",
    "remove_flagged_cells",
    "median_RNA", "median_negprobes",
    # Run metadata
    "assay_type", "Panel", "version", "slide_ID",
    # SAHA metadata
    "sample_id",
    "SAHA_name",
    "SAHA_organcode", "SAHA_batchcode",
    # Cell type
    "cell_type",
}

# Columns to keep in Xenium h5ads (already clean — just ensure sample_id present)
XENIUM_KEEP = {
    "x", "y",
    "transcript_counts", "control_probe_counts",
    "cell_area", "nucleus_area",
    "n_counts", "n_genes",
    "leiden_res0.1", "leiden_res0.3", "leiden_res0.5", "leiden_res1.0",
    "sample_id", "platform", "modality",
    "cell_type", "cell_type_detailed",  # for when annotations are added
}

# obsm keys to retain
OBSM_KEEP = {"spatial", "X_umap"}

# ---------------------------------------------------------------------------
# Column rename maps  (old_name → new_name)
# Applied before the whitelist filter.
# ---------------------------------------------------------------------------

RNA_RENAME = {
    "Insitutype_Broad":   "cell_type",
    "Insitutype_Labelled":"cell_type_detailed",
}

PRT_RENAME = {
    "c_final_cell_type": "cell_type",
}

XENIUM_RENAME: dict = {}


# ---------------------------------------------------------------------------
# Core transform
# ---------------------------------------------------------------------------

def detect_assay(adata: ad.AnnData, path: Path) -> str:
    """Infer assay type from obs columns or filename."""
    cols = set(adata.obs.columns)
    fn = path.name.upper()
    if "XR" in fn or "XENIUM" in fn:
        return "xenium"
    if "PRT" in fn or "c_final_cell_type" in cols or "nCount_negprobes" in cols and "nCount_falsecode" not in cols:
        return "prt"
    return "rna"


def standardize(adata: ad.AnnData, assay: str) -> ad.AnnData:
    """
    Return a new (in-memory) AnnData with:
      - obs columns filtered to whitelist
      - cell-type columns renamed
      - sample_id column added
      - obsm trimmed to spatial + X_umap
    """
    if assay == "rna":
        rename_map = RNA_RENAME
        keep_set   = RNA_KEEP
    elif assay == "prt":
        rename_map = PRT_RENAME
        keep_set   = PRT_KEEP
    else:
        rename_map = XENIUM_RENAME
        keep_set   = XENIUM_KEEP

    # Work on a copy of obs
    obs = adata.obs.copy()

    # 1. Rename cell-type columns
    for old, new in rename_map.items():
        if old in obs.columns:
            obs.rename(columns={old: new}, inplace=True)

    # 2. Add sample_id
    if "sample_id" not in obs.columns:
        if "SAHA_name" in obs.columns:
            obs["sample_id"] = obs["SAHA_name"]
        # Xenium already has sample_id

    # 3. Filter to whitelist (retain only cols in keep_set that are present)
    keep_cols = [c for c in obs.columns if c in keep_set]
    obs = obs[keep_cols]

    # 4. Build new AnnData (load X into memory)
    import scipy.sparse as sp
    X = adata.X
    if hasattr(X, "toarray"):
        X = X.toarray()
    elif hasattr(X, "__array__"):
        X = np.array(X)

    new_adata = ad.AnnData(
        X=X,
        obs=obs,
        var=adata.var.copy(),
    )

    # 5. Transfer obsm keys selectively
    for key in OBSM_KEEP:
        if key in adata.obsm:
            new_adata.obsm[key] = np.array(adata.obsm[key])

    # 6. Transfer uns (global metadata)
    new_adata.uns = dict(adata.uns)

    return new_adata


# ---------------------------------------------------------------------------
# Single-file entry point
# ---------------------------------------------------------------------------

def process_file(input_path: Path, output_path: Path, dry_run: bool = False) -> None:
    print(f"Reading  {input_path.name} …")
    adata = ad.read_h5ad(input_path, backed="r")
    assay = detect_assay(adata, input_path)
    n_obs_before = adata.n_obs
    n_col_before = len(adata.obs.columns)

    if dry_run:
        # Show what would change without writing
        if assay == "rna":
            rename_map, keep_set = RNA_RENAME, RNA_KEEP
        elif assay == "prt":
            rename_map, keep_set = PRT_RENAME, PRT_KEEP
        else:
            rename_map, keep_set = XENIUM_RENAME, XENIUM_KEEP

        obs_cols = set(adata.obs.columns)
        renamed = {k: v for k, v in rename_map.items() if k in obs_cols}
        # Build post-rename column set accurately
        after_rename = (obs_cols - set(rename_map.keys())) | set(renamed.values())
        # add sample_id (will be added if SAHA_name present)
        if "SAHA_name" in obs_cols or "sample_id" in obs_cols:
            after_rename.add("sample_id")
        kept = sorted(after_rename & keep_set)
        dropped = sorted(obs_cols - set(rename_map.keys()) - keep_set)

        print(f"  assay={assay}, n_obs={n_obs_before}, cols {n_col_before} → {len(kept)}")
        print(f"  renames: {renamed}")
        print(f"  kept ({len(kept)}): {kept}")
        print(f"  dropped ({len(dropped)}): {dropped[:20]}{'...' if len(dropped)>20 else ''}")
        obsm_dropped = [k for k in adata.obsm if k not in OBSM_KEEP]
        print(f"  obsm dropped: {obsm_dropped}")
        adata.file.close()
        return

    new_adata = standardize(adata, assay)
    adata.file.close()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    new_adata.write_h5ad(output_path, compression="gzip")
    print(f"  {assay} | {n_obs_before} cells | obs cols {n_col_before} → {len(new_adata.obs.columns)} | → {output_path}")


# ---------------------------------------------------------------------------
# Batch entry point
# ---------------------------------------------------------------------------

def batch_process(src_rna: Path, src_prt: Path, src_xenium: Path,
                  out_dir: Path, dry_run: bool) -> None:
    tasks: list[tuple[Path, Path]] = []

    for src, pattern in [(src_rna, "SAHA_*_RNA.h5ad"),
                         (src_prt, "SAHA_*_PRT.h5ad")]:
        if src and src.exists():
            for p in sorted(src.glob(pattern)):
                if "All" in p.name or "GI" in p.name:
                    continue
                tasks.append((p, out_dir / p.parent.name / p.name))

    if src_xenium and src_xenium.exists():
        for p in sorted(src_xenium.glob("SAHA_XR_*.h5ad")):
            tasks.append((p, out_dir / "xenium" / p.name))

    print(f"Found {len(tasks)} h5ad files to process.\n")
    for inp, out in tasks:
        process_file(inp, out, dry_run=dry_run)
        print()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--input", type=Path, help="Single input h5ad")
    mode.add_argument("--batch", action="store_true", help="Process all SAHA h5ads")

    p.add_argument("--output",      type=Path, help="Output path (single-file mode)")
    p.add_argument("--src-rna",     type=Path,
                   default=Path("/Users/jiwoonpark/Dropbox (Personal)/2025- MasonLab/2025_SAHA/Data/preprocessed/h5ad/RNA"))
    p.add_argument("--src-prt",     type=Path,
                   default=Path("/Users/jiwoonpark/Dropbox (Personal)/2025- MasonLab/2025_SAHA/Data/preprocessed/h5ad/PRT"))
    p.add_argument("--src-xenium",  type=Path,
                   default=Path("/Users/jiwoonpark/Dropbox (Personal)/2025- MasonLab/2025_SAHA/Data/preprocessed/h5ad_sample/RNA"))
    p.add_argument("--out-dir",     type=Path, default=Path("data/h5ad_public"),
                   help="Output directory (batch mode)")
    p.add_argument("--dry-run",     action="store_true",
                   help="Print what would be done without writing files")
    return p.parse_args()


def main():
    args = parse_args()

    if args.batch:
        batch_process(
            src_rna=args.src_rna,
            src_prt=args.src_prt,
            src_xenium=args.src_xenium,
            out_dir=args.out_dir,
            dry_run=args.dry_run,
        )
    else:
        if not args.input.exists():
            sys.exit(f"ERROR: {args.input} not found")
        output = args.output or args.out_dir / args.input.name
        process_file(args.input, output, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
