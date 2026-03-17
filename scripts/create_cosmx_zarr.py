#!/usr/bin/env python3
"""
create_cosmx_zarr.py
Convert CosMx flat files to SpatialData zarr format.

Requires CosMx raw output directory containing:
  - *_exprMat_file.csv.gz  (or *_exprMat_report.csv.gz)  — cell × gene counts
  - *_metadata_file.csv.gz                                — per-cell metadata
  - *_tx_file.csv.gz                                      — transcript locations
  - CellComposite/ or Morphology/                         — TIFF images

Usage — single sample:
    python scripts/create_cosmx_zarr.py \
        --raw-dir  s3://saha-open-data/raw/cosmx/SAHA_CR_COL_A1_EC02 \
        --h5ad     data/h5ad_public/RNA/SAHA_COL_RNA.h5ad \
        --sample   SAHA_CR_COL_A1_EC02 \
        --output   data/zarr/SAHA_CR_COL_A1_EC02.zarr

Usage — batch from S3 (run after Phase 3 S3 upload):
    python scripts/create_cosmx_zarr.py --batch \
        --s3-bucket saha-open-data \
        --h5ad-dir  data/h5ad_public/RNA \
        --out-dir   data/zarr

NOTE: This script requires the raw CosMx flat files to be accessible.
      Raw files are currently on Wasabi and will be migrated to S3 in Phase 3.
      Until then, use --raw-dir with a local path if files are available locally.

Requirements:
    pip install spatialdata spatialdata-io anndata boto3 tifffile
"""

import argparse
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# CosMx SpatialData construction
# ---------------------------------------------------------------------------

def build_spatialdata_from_flat(raw_dir: Path, sample_id: str,
                                 h5ad_path: "Path | None" = None):
    """
    Build a SpatialData object from CosMx flat files.

    Parameters
    ----------
    raw_dir   : directory containing CosMx output files for one sample
    sample_id : SAHA sample identifier (e.g. SAHA_CR_COL_A1_EC02)
    h5ad_path : optional path to standardized h5ad — obs metadata merged in
    """
    try:
        import spatialdata as sd
        import spatialdata_io
    except ImportError:
        sys.exit("ERROR: spatialdata and spatialdata-io are required.\n"
                 "  pip install spatialdata spatialdata-io")

    import anndata as ad
    import pandas as pd
    import numpy as np

    raw_dir = Path(raw_dir)

    # --- Locate flat files ---
    def find_file(patterns):
        for pat in patterns:
            matches = list(raw_dir.glob(pat))
            if matches:
                return matches[0]
        return None

    expr_file = find_file(["*exprMat_file.csv*", "*exprMat_report.csv*",
                            "*_exprMat*.csv.gz", "*_exprMat*.csv"])
    meta_file = find_file(["*metadata_file.csv*", "*_metadata*.csv.gz",
                            "*_metadata*.csv"])
    tx_file   = find_file(["*tx_file.csv*", "*_tx*.csv.gz"])
    morph_dir = find_file(["CellComposite", "Morphology_ChannelID_*"])

    if not expr_file:
        sys.exit(f"ERROR: Cannot find expression matrix in {raw_dir}")
    if not meta_file:
        sys.exit(f"ERROR: Cannot find cell metadata in {raw_dir}")

    print(f"  expr:  {expr_file.name}")
    print(f"  meta:  {meta_file.name}")
    print(f"  tx:    {tx_file.name if tx_file else 'NOT FOUND (transcripts omitted)'}")
    print(f"  morph: {morph_dir.name if morph_dir else 'NOT FOUND (images omitted)'}")

    # --- Load expression matrix ---
    print("  Loading expression matrix …")
    expr = pd.read_csv(expr_file, index_col=0)
    # Rows = cells, cols = genes + fov/cell_ID columns
    # NanoString format: first cols are cell_ID, fov, then gene counts
    id_cols = [c for c in expr.columns if c.lower() in
               ("cell_id", "fov", "cell", "cellid")]
    gene_cols = [c for c in expr.columns if c not in id_cols
                 and not c.startswith("Negative") and not c.startswith("SystemControl")]
    neg_cols  = [c for c in expr.columns if c.startswith("Negative") or
                 c.startswith("SystemControl")]

    import scipy.sparse as sp2
    X = sp2.csr_matrix(expr[gene_cols].values, dtype="float32")

    # --- Load cell metadata ---
    print("  Loading cell metadata …")
    meta = pd.read_csv(meta_file, index_col=0)
    meta["sample_id"] = sample_id

    # Merge optional h5ad obs (cell type, QC) by cell_ID
    if h5ad_path and Path(h5ad_path).exists():
        print(f"  Merging obs from {Path(h5ad_path).name} …")
        ref = ad.read_h5ad(h5ad_path, backed="r")
        # Filter to this sample only
        if "sample_id" in ref.obs.columns:
            ref_obs = ref.obs[ref.obs["sample_id"] == sample_id].copy()
        elif "SAHA_name" in ref.obs.columns:
            ref_obs = ref.obs[ref.obs["SAHA_name"] == sample_id].copy()
        else:
            ref_obs = ref.obs.copy()
        # Merge on cell_ID
        if "cell_ID" in meta.columns and "cell_ID" in ref_obs.columns:
            merge_cols = [c for c in ref_obs.columns
                          if c not in meta.columns or c == "cell_ID"]
            meta = meta.merge(ref_obs[merge_cols], on="cell_ID", how="left")
        ref.file.close()

    # --- Build AnnData table ---
    table = ad.AnnData(
        X=X,
        obs=meta.loc[expr.index] if set(expr.index) <= set(meta.index) else meta,
        var=pd.DataFrame(index=gene_cols),
    )
    table.obsm["spatial"] = meta[["CenterX_global_px", "CenterY_global_px"]].values

    # Store negative probe counts in a layer
    if neg_cols:
        table.layers["negprobes"] = sp2.csr_matrix(
            expr[neg_cols].values, dtype="float32"
        )

    # --- Build SpatialData elements ---
    elements: dict = {}

    # Transcripts (points)
    if tx_file:
        print("  Loading transcripts …")
        tx = pd.read_csv(tx_file)
        required_tx_cols = {"x_global_px", "y_global_px", "target"}
        # NanoString may use different column names
        col_map = {}
        for req in ("x_global_px", "y_global_px", "target"):
            candidates = [c for c in tx.columns if req.split("_")[0].lower() in c.lower()]
            if candidates:
                col_map[candidates[0]] = req
        if col_map:
            tx = tx.rename(columns=col_map)

        if required_tx_cols <= set(tx.columns):
            import geopandas as gpd
            from shapely.geometry import Point
            # Build GeoDataFrame for spatialdata points element
            from spatialdata.models import PointsModel
            tx_sd = PointsModel.parse(
                tx[["x_global_px", "y_global_px"]].rename(
                    columns={"x_global_px": "x", "y_global_px": "y"}
                ),
                feature_key="target" if "target" in tx.columns else None,
            )
            elements["transcripts"] = tx_sd

    # Cell circles (shapes)
    try:
        from spatialdata.models import ShapesModel
        import geopandas as gpd
        from shapely.geometry import Point
        coords = table.obsm["spatial"]
        radii  = np.sqrt(meta.get("Area.um2", pd.Series(100.0, index=meta.index))
                         .reindex(table.obs.index).fillna(100.0).values / np.pi)
        cell_circles = gpd.GeoDataFrame(
            {"geometry": [Point(x, y).buffer(r) for (x, y), r in
                          zip(coords, radii)],
             "cell_ID": table.obs.get("cell_ID", table.obs.index).values},
            index=table.obs.index,
        )
        elements["cell_circles"] = ShapesModel.parse(cell_circles)
    except Exception as e:
        print(f"  WARNING: cell shapes skipped ({e})")

    # Images (if morphology TIFFs are present)
    if morph_dir:
        try:
            import tifffile
            from spatialdata.models import Image2DModel
            tiff_files = sorted(Path(morph_dir).glob("*.TIF")) + \
                         sorted(Path(morph_dir).glob("*.tif"))
            if tiff_files:
                print(f"  Loading {len(tiff_files)} TIFF(s) from {morph_dir.name} …")
                img = tifffile.imread(str(tiff_files[0]))
                elements["morphology"] = Image2DModel.parse(img)
        except Exception as e:
            print(f"  WARNING: image loading skipped ({e})")

    # --- Assemble SpatialData ---
    from spatialdata.models import TableModel
    table = TableModel.parse(
        table,
        region=sample_id,
        region_key="sample_id",
        instance_key="cell_ID" if "cell_ID" in table.obs.columns else None,
    )

    sdata = sd.SpatialData(
        points={k: v for k, v in elements.items() if "transcript" in k},
        shapes={k: v for k, v in elements.items() if "circle" in k or "bound" in k},
        images={k: v for k, v in elements.items() if "morph" in k or "image" in k},
        tables={"table": table},
    )
    return sdata


# ---------------------------------------------------------------------------
# Single-sample entry point
# ---------------------------------------------------------------------------

def process_sample(raw_dir: Path, sample_id: str, h5ad: "Path | None",
                   output: Path, dry_run: bool) -> None:
    if dry_run:
        print(f"[dry-run] Would build zarr for {sample_id}")
        print(f"          raw-dir : {raw_dir}")
        print(f"          h5ad    : {h5ad}")
        print(f"          output  : {output}")
        return

    print(f"\nBuilding SpatialData zarr for {sample_id} …")
    sdata = build_spatialdata_from_flat(raw_dir, sample_id, h5ad)
    output.parent.mkdir(parents=True, exist_ok=True)
    sdata.write(str(output))
    print(f"  → wrote {output}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--sample",  help="Single sample_id to process")
    mode.add_argument("--batch",   action="store_true")

    p.add_argument("--raw-dir",    type=Path,
                   help="CosMx raw output directory for the sample")
    p.add_argument("--h5ad",       type=Path,
                   help="Standardized h5ad for obs metadata merging")
    p.add_argument("--output",     type=Path,
                   help="Output zarr path (single-sample mode)")
    p.add_argument("--s3-bucket",  default="saha-open-data",
                   help="S3 bucket name (batch mode)")
    p.add_argument("--h5ad-dir",   type=Path,
                   help="Directory of standardized h5ads (batch mode)")
    p.add_argument("--out-dir",    type=Path, default=Path("data/zarr"))
    p.add_argument("--dry-run",    action="store_true")
    return p.parse_args()


def main():
    args = parse_args()

    if args.sample:
        if not args.raw_dir:
            sys.exit("ERROR: --raw-dir is required in single-sample mode")
        process_sample(
            raw_dir=args.raw_dir,
            sample_id=args.sample,
            h5ad=args.h5ad,
            output=args.output or args.out_dir / f"{args.sample}.zarr",
            dry_run=args.dry_run,
        )
    else:
        print("Batch CosMx zarr creation requires S3/Wasabi access (Phase 3).")
        print("Run after uploading raw CosMx flat files to S3.")
        print()
        print("Example per-sample call once flat files are on S3:")
        print("  python scripts/create_cosmx_zarr.py \\")
        print("    --sample SAHA_CR_COL_A1_EC02 \\")
        print("    --raw-dir s3://saha-open-data/raw/cosmx/SAHA_CR_COL_A1_EC02 \\")
        print("    --h5ad data/h5ad_public/RNA/SAHA_COL_RNA.h5ad \\")
        print("    --output data/zarr/SAHA_CR_COL_A1_EC02.zarr")


if __name__ == "__main__":
    main()
