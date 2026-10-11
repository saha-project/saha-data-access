#!/usr/bin/env python3
"""
create_cosmx_zarr.py
Convert CosMx flat-file exports to SpatialData zarr.

Layout expected (AtoMx flat-file export):

    <sample_dir>/flatFiles/<run_name>/
        <run>_exprMat_file.csv.gz        cell x gene raw counts
        <run>_metadata_file.csv.gz       per-cell metadata
        <run>_tx_file.csv.gz             transcript locations   (RNA only)
        <run>-polygons.csv.gz            cell boundary vertices
        <run>_fov_positions_file.csv.gz  FOV stage positions

spatialdata_io.cosmx() cannot read these: it hard-requires CellComposite/ and
CellLabels/ image directories, which flat-file exports do not contain, and it
ignores the polygons file. Hence this builder.

No images are produced — these exports contain none. Objects hold:
    tables/table       AnnData, raw counts, obs from metadata (+ optional h5ad merge)
    points/transcripts transcript locations (RNA samples only)
    shapes/cells       cell boundary polygons

All coordinates are CosMx global pixels in a single "global" coordinate system.

Usage:
    # one sample
    python scripts/create_cosmx_zarr.py \
        --raw-dir "/Volumes/Crucial X6/SAHA/Data_AllFiles/flatFiles/APE/Raw/SAHA_CR_APE_A9_SJ01" \
        --sample  SAHA_CR_APE_A9_SJ01 \
        --out-dir data/zarr

    # every sample under a flatFiles root
    python scripts/create_cosmx_zarr.py --batch \
        --root    "/Volumes/Crucial X6/SAHA/Data_AllFiles/flatFiles" \
        --out-dir data/zarr

    # check what batch would do, without building
    python scripts/create_cosmx_zarr.py --batch --root ... --out-dir ... --dry-run

Requirements:
    pip install spatialdata spatialdata-io anndata geopandas pyarrow tifffile
"""

import argparse
import re
import shutil
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# Rows of exprMat / tx read per chunk. Bounds peak memory; the dense chunk is
# CHUNK x n_genes float32 before it is compressed to sparse.
EXPR_CHUNK = 20_000
TX_CHUNK = 2_000_000

# Fixed column order of a CosMx flat-file transcript export, for the rare file
# that ships without a header row.
TX_COLUMNS = ["fov", "cell_ID", "cell", "x_local_px", "y_local_px",
              "x_global_px", "y_global_px", "z", "target", "CellComp"]

# exprMat columns that are not genes.
ID_COLS = {"cell_id", "cell_ID", "fov", "cell"}
CONTROL_PREFIXES = ("Negative", "SystemControl", "Falsecode", "FalseCode")


# ---------------------------------------------------------------------------
# Locating flat files
# ---------------------------------------------------------------------------

def find_run_dir(sample_dir: Path) -> Path:
    """Descend into flatFiles/<run_name>/. Run directory names are not derivable
    from the sample_id (e.g. SAHA_CR_COL_A1_EC02 -> ECJP7295_COL_A1_2), so glob."""
    if list(sample_dir.glob("*_exprMat_file.csv*")):
        return sample_dir
    candidates = sorted(sample_dir.glob("flatFiles/*/*_exprMat_file.csv*"))
    if not candidates:
        candidates = sorted(sample_dir.glob("*/*_exprMat_file.csv*"))
    if not candidates:
        raise FileNotFoundError(f"No *_exprMat_file.csv.gz found under {sample_dir}")
    if len({c.parent for c in candidates}) > 1:
        raise RuntimeError(
            f"{sample_dir}: multiple run directories found "
            f"({sorted({c.parent.name for c in candidates})}) — pass one explicitly "
            f"with --raw-dir"
        )
    return candidates[0].parent


def flat_files(run_dir: Path) -> dict:
    """Map role -> path. tx and polygons may be absent (protein runs have no tx)."""
    def one(pattern, required=True):
        hits = [p for p in sorted(run_dir.glob(pattern)) if not p.name.startswith("._")]
        if not hits:
            if required:
                raise FileNotFoundError(f"{run_dir}: no file matching {pattern}")
            return None
        return hits[0]

    return {
        "expr": one("*_exprMat_file.csv*"),
        "meta": one("*_metadata_file.csv*"),
        "tx": one("*_tx_file.csv*", required=False),
        "polygons": one("*-polygons.csv*", required=False),
        "fov": one("*_fov_positions_file.csv*", required=False),
    }


def cell_key(df: pd.DataFrame, what: str, slide: int = 1) -> pd.Series:
    """Globally unique cell identifier.

    CosMx `cell_ID` restarts at 1 in every FOV, so it is unique only within an
    FOV. `cell` (e.g. c_1_1_1 = c_slide_fov_cell) is unique slide-wide. Prefer
    it; otherwise synthesise the equivalent from fov + cell_ID.

    `slide` matters when several slides were imaged in one run: the exprMat
    carries no slide column, so the number has to come from the metadata. It
    is 1 for a single-slide run, which is why the default reproduces the
    original ids exactly.
    """
    if "cell" in df.columns:
        return df["cell"].astype(str)
    if {"fov", "cell_ID"} <= set(df.columns):
        return (f"c_{slide}_" + df["fov"].astype(str)
                + "_" + df["cell_ID"].astype(str))
    raise KeyError(f"{what}: need a 'cell' column, or both 'fov' and 'cell_ID'")


def slide_of(meta: pd.DataFrame) -> int:
    """Slide number the metadata's `cell` ids were minted with (c_<slide>_..)."""
    if "cell" not in meta.columns or meta.empty:
        return 1
    m = re.match(r"c_(\d+)_", str(meta["cell"].iloc[0]))
    return int(m.group(1)) if m else 1


# ---------------------------------------------------------------------------
# Table
# ---------------------------------------------------------------------------

def read_expr(expr_file: Path, slide: int = 1):
    """Chunked read of the cell x gene matrix -> (sparse counts, cells, genes,
    sparse control-probe counts, control names).

    Read whole, a 6k-plex x 300k-cell matrix is ~14 GB dense. Chunking caps peak
    memory at EXPR_CHUNK x n_genes float32 (~0.5 GB at defaults).
    """
    import scipy.sparse as sp

    header = pd.read_csv(expr_file, nrows=0).columns.tolist()
    genes = [c for c in header
             if c not in ID_COLS and not c.startswith(CONTROL_PREFIXES)]
    controls = [c for c in header if c.startswith(CONTROL_PREFIXES)]
    if not genes:
        raise ValueError(f"{expr_file}: no gene columns detected")

    gene_blocks, ctrl_blocks, cells = [], [], []
    n = 0
    for chunk in pd.read_csv(expr_file, chunksize=EXPR_CHUNK):
        cells.append(cell_key(chunk, "exprMat", slide))
        gene_blocks.append(sp.csr_matrix(chunk[genes].to_numpy(dtype="float32")))
        if controls:
            ctrl_blocks.append(sp.csr_matrix(chunk[controls].to_numpy(dtype="float32")))
        n += len(chunk)
        print(f"    {n:,} cells", end="\r", flush=True)
    print(f"    {n:,} cells      ")

    X = sp.vstack(gene_blocks, format="csr")
    C = sp.vstack(ctrl_blocks, format="csr") if ctrl_blocks else None
    return X, pd.concat(cells, ignore_index=True), genes, C, controls


def normalize_obs(obs: pd.DataFrame) -> pd.DataFrame:
    """Resolve CosMx column names that spatialdata rejects.

    CosMx metadata carries both `cell_id` (string, c_slide_fov_cell) and
    `cell_ID` (integer, restarts per FOV). spatialdata forbids names that differ
    only by case. `cell_id` duplicates the `cell` column we key on, so drop it
    and give the integer an unambiguous name.
    """
    obs = obs.copy()
    if "cell_id" in obs.columns and "cell" in obs.columns:
        if obs["cell_id"].astype(str).equals(obs["cell"].astype(str)):
            obs = obs.drop(columns=["cell_id"])
    obs = obs.rename(columns={"cell_ID": "fov_cell_id"})

    clashes = [c for c in obs.columns
               if sum(o.lower() == c.lower() for o in obs.columns) > 1]
    if clashes:
        raise ValueError(f"unresolved case-variant obs columns: {sorted(set(clashes))}")
    return obs


def build_table(files: dict, sample_id: str, h5ad_path: "Path | None",
                shapes_element: str):
    import anndata as ad
    from spatialdata.models import TableModel

    print("  cell metadata …")
    meta = pd.read_csv(files["meta"])
    meta.index = cell_key(meta, "metadata")
    slide = slide_of(meta)

    print("  expression matrix …")
    X, expr_cells, genes, controls_X, control_names = read_expr(files["expr"], slide)

    # exprMat and metadata can disagree at the margins (cells dropped by QC in
    # one export but not the other). Intersect, preserving exprMat row order.
    obs = meta.reindex(expr_cells.values)
    missing = obs.index[obs.isna().all(axis=1)]
    if len(missing):
        keep = ~obs.isna().all(axis=1).to_numpy()
        print(f"    {len(missing):,} cells in exprMat have no metadata — dropped")
        X = X[keep]
        if controls_X is not None:
            controls_X = controls_X[keep]
        obs = obs.loc[keep]

    obs = normalize_obs(obs)
    obs["sample_id"] = sample_id
    obs["cell"] = obs.index.astype(str)
    obs["region"] = pd.Categorical([shapes_element] * len(obs))

    table = ad.AnnData(
        X=X,
        obs=obs,
        var=pd.DataFrame(index=pd.Index(genes, name=None)),
    )
    if {"CenterX_global_px", "CenterY_global_px"} <= set(obs.columns):
        table.obsm["spatial"] = obs[["CenterX_global_px",
                                     "CenterY_global_px"]].to_numpy(dtype="float64")
    if controls_X is not None:
        table.obsm["control_probes"] = pd.DataFrame(
            controls_X.toarray(), index=obs.index, columns=control_names
        )

    if h5ad_path:
        merge_h5ad_obs(table, Path(h5ad_path), sample_id)

    return TableModel.parse(
        table,
        region=shapes_element,
        region_key="region",
        instance_key="cell",
    )


# Cell-type columns worth carrying over, mapped to canonical public names.
# The raw organ-level h5ads still use the pre-standardization NanoString names;
# standardize_h5ad.py renames them downstream, so accept both spellings.
CELLTYPE_COLS = {
    "cell_type": "cell_type",
    "cell_type_detailed": "cell_type_detailed",
    "Insitutype_Broad": "cell_type",
    "Insitutype_Labelled": "cell_type_detailed",
    "c_final_cell_type": "cell_type",
    "niche": "niche",
    "leiden": "leiden",
    # Donor identity. These slides are tissue microarrays: one acquisition
    # covers several donor cores, so donor is a per-CELL property, not a
    # per-sample one. SAHA_Sample holds `<SLIDE>_<CORE>` (e.g. COL_A1_5),
    # which joins to SampleID in the published cohort table.
    "SAHA_Sample": "donor_core",
}


def merge_h5ad_obs(table, h5ad_path: Path, sample_id: str) -> None:
    """Annotate against the published organ-level h5ad, matching on `cell`.

    Adds `qc_pass` — whether the cell survived into the published h5ad. Flat
    files hold every segmented cell; the published h5ads are QC-filtered, so
    the object is a superset and this column is how a user reconciles the two.

    Cell types are merged when present. Refuses rather than silently producing
    an all-NaN column — a cell_type of NaN looks like real output.
    """
    import anndata as ad

    if not h5ad_path.exists():
        print(f"  WARNING: {h5ad_path} not found — no qc_pass or cell types added")
        return

    print(f"  annotating from {h5ad_path.name} …")
    ref = ad.read_h5ad(h5ad_path, backed="r")
    ref_obs = ref.obs
    for col in ("sample_id", "SAHA_name"):
        if col in ref_obs.columns:
            ref_obs = ref_obs[ref_obs[col].astype(str) == sample_id]
            break
    ref_obs = ref_obs.copy()
    ref.file.close()

    if not len(ref_obs):
        print(f"  WARNING: {h5ad_path.name} has no rows for {sample_id} — skipped")
        return

    try:
        ref_obs.index = cell_key(ref_obs, "h5ad obs")
    except KeyError:
        print(f"  WARNING: {h5ad_path.name} obs has no 'cell' or fov+cell_ID — skipped")
        return

    member = table.obs.index.isin(ref_obs.index)
    hit = member.mean()
    if hit < 0.5:
        print(f"  WARNING: only {hit:.1%} of cells matched the h5ad — NOT annotating. "
              f"Cell IDs likely differ between exports; reconcile manually.")
        return

    table.obs["qc_pass"] = member
    print(f"    qc_pass: {member.sum():,} / {len(member):,} cells ({hit:.1%}) "
          f"are in the published h5ad")

    src = {s: d for s, d in CELLTYPE_COLS.items()
           if s in ref_obs.columns and d not in table.obs.columns}
    if not src:
        print("    no cell-type columns in the h5ad obs")
        return

    aligned = ref_obs[list(src)].rename(columns=src).reindex(table.obs.index)
    aligned = aligned.loc[:, ~aligned.columns.duplicated()]
    for c in aligned.columns:
        table.obs[c] = aligned[c].values
    print(f"    merged {list(aligned.columns)}")


# ---------------------------------------------------------------------------
# Points and shapes
# ---------------------------------------------------------------------------

def build_points(tx_file: Path, tmp_parquet: Path):
    """Transcripts -> dask-backed points.

    Streamed csv.gz -> parquet first: a tx file holds 10^8+ rows and gzip is not
    splittable, so dask cannot partition the csv itself.
    """
    import dask.dataframe as dd
    import pyarrow as pa
    import pyarrow.parquet as pq
    from spatialdata.models import PointsModel

    cols = ["x_global_px", "y_global_px", "z", "target", "fov", "CellComp", "cell"]
    header = pd.read_csv(tx_file, nrows=0).columns.tolist()
    # Some exports ship the tx file with no header row (seen on CRC_C18), in
    # which case the first transcript is mistaken for the column names. The
    # schema is fixed across every CosMx flat-file export, so supply it.
    if "x_global_px" not in header:
        header = TX_COLUMNS
        names = TX_COLUMNS
        print(f"    no header row — assuming standard column order")
    else:
        names = None
    use = [c for c in cols if c in header]
    for req in ("x_global_px", "y_global_px", "target"):
        if req not in use:
            raise KeyError(f"{tx_file.name}: missing required column {req}")

    # Declare every dtype. Left to inference, pandas hits a bug in
    # _concatenate_chunks when usecols and chunksize are combined and a column
    # looks mixed-typed: it indexes the full column-name list with a position
    # from the selected subset and raises IndexError.
    dtypes = {"x_global_px": "float64", "y_global_px": "float64", "z": "float64",
              "target": "string", "CellComp": "string", "cell": "string",
              "fov": "int32"}

    tmp_parquet.parent.mkdir(parents=True, exist_ok=True)
    writer = None
    n = 0
    try:
        for chunk in pd.read_csv(tx_file, usecols=use, chunksize=TX_CHUNK,
                                 names=names, header=(None if names else "infer"),
                                 dtype={k: v for k, v in dtypes.items() if k in use}):
            for c in ("target", "CellComp", "cell"):
                if c in chunk.columns:
                    chunk[c] = chunk[c].astype(str)
            tbl = pa.Table.from_pandas(chunk, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(tmp_parquet, tbl.schema, compression="zstd")
            writer.write_table(tbl)
            n += len(chunk)
            print(f"    {n:,} transcripts", end="\r", flush=True)
    finally:
        if writer is not None:
            writer.close()
    print(f"    {n:,} transcripts      ")

    ddf = dd.read_parquet(tmp_parquet)
    return PointsModel.parse(
        ddf,
        coordinates={"x": "x_global_px", "y": "y_global_px"},
        feature_key="target",
    )


def build_shapes(polygons_file: Path, keep_cells: pd.Index):
    """Cell boundary polygons -> GeoDataFrame indexed by cell.

    ponytail: shapely Polygon per cell in a Python loop, ~10s per 10^5 cells.
    Vectorise with shapely.polygons(ragged) if this ever dominates runtime.
    """
    import geopandas as gpd
    from shapely.geometry import Polygon
    from spatialdata.models import ShapesModel

    poly = pd.read_csv(polygons_file)
    poly.index = cell_key(poly, "polygons")
    poly = poly[poly.index.isin(keep_cells)]
    if not len(poly):
        raise ValueError(f"{polygons_file.name}: no vertices match table cells")

    geoms, idx = [], []
    for cell, grp in poly.groupby(level=0, sort=False):
        if len(grp) < 3:
            continue                      # degenerate, cannot form a polygon
        geoms.append(Polygon(zip(grp["x_global_px"], grp["y_global_px"])))
        idx.append(cell)

    gdf = gpd.GeoDataFrame({"geometry": geoms}, index=pd.Index(idx, name=None))
    print(f"    {len(gdf):,} cell polygons")
    return ShapesModel.parse(gdf)


def build_circles(table, sample_id: str):
    """Fallback when no polygons file exists: circles of equivalent area."""
    import geopandas as gpd
    from shapely.geometry import Point
    from spatialdata.models import ShapesModel

    if "spatial" not in table.obsm:
        raise ValueError("no polygons file and no centroid columns — cannot build shapes")
    coords = table.obsm["spatial"]
    area = table.obs["Area.um2"] if "Area.um2" in table.obs.columns else None
    radii = np.sqrt((area.to_numpy(dtype="float64") if area is not None
                     else np.full(len(coords), 100.0)) / np.pi)
    gdf = gpd.GeoDataFrame(
        {"geometry": [Point(x, y) for x, y in coords], "radius": radii},
        index=table.obs.index,
    )
    print(f"    {len(gdf):,} cell circles (no polygons file)")
    return ShapesModel.parse(gdf)


# ---------------------------------------------------------------------------
# Assembly
# ---------------------------------------------------------------------------

def build_sample(sample_dir: Path, sample_id: str, out_path: Path,
                 h5ad_path: "Path | None", scratch: Path) -> None:
    import spatialdata as sd

    run_dir = find_run_dir(sample_dir)
    files = flat_files(run_dir)
    print(f"  run dir: {run_dir.name}")
    for k, v in files.items():
        print(f"    {k:9s} {v.name if v else '— absent'}")

    shapes_element = "cells"
    table = build_table(files, sample_id, h5ad_path, shapes_element)

    print("  cell boundaries …")
    if files["polygons"]:
        shapes = build_shapes(files["polygons"], table.obs.index)
        # A cell without a usable polygon cannot stay in the table: spatialdata
        # requires every instance_key value to exist in the annotated element.
        if len(shapes) < len(table):
            from spatialdata.models import TableModel
            keep = table.obs.index.isin(shapes.index)
            print(f"    dropping {(~keep).sum():,} cells with no valid polygon")
            table = table[keep].copy()
            del table.uns[TableModel.ATTRS_KEY]
            table = TableModel.parse(table, region=shapes_element,
                                     region_key="region", instance_key="cell")
    else:
        shapes = build_circles(table, sample_id)

    points = {}
    if files["tx"]:
        print("  transcripts …")
        points["transcripts"] = build_points(
            files["tx"], scratch / f"{sample_id}_tx.parquet"
        )

    sdata = sd.SpatialData(
        points=points,
        shapes={shapes_element: shapes},
        tables={"table": table},
    )

    if out_path.exists():
        shutil.rmtree(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"  writing {out_path} …")
    sdata.write(out_path)

    tmp = scratch / f"{sample_id}_tx.parquet"
    if tmp.exists():
        tmp.unlink()

    strip_appledouble(out_path)


def strip_appledouble(path: Path) -> None:
    """Remove macOS ._* shadow files from the store.

    Writing a zarr to exFAT/FAT (external drives) makes macOS emit an AppleDouble
    ._<name> beside every file. Readers warn on them and they would be archived
    into the Zenodo zips, so drop them.
    """
    junk = [p for p in path.rglob("._*")]
    for p in junk:
        p.unlink(missing_ok=True)
    if junk:
        print(f"    removed {len(junk)} AppleDouble shadow files")


def patch_obs(out_path: Path, sample_id: str, h5ad_path: "Path | None") -> bool:
    """Add qc_pass and cell types to an already-built object.

    Rewrites only the table element, so re-annotating costs seconds instead of
    re-staging hundreds of millions of transcripts.
    """
    import spatialdata as sd

    if h5ad_path is None:
        print(f"  no h5ad for {sample_id} — nothing to patch")
        return True

    sdata = sd.read_zarr(out_path)
    table = sdata.tables["table"]
    before = set(table.obs.columns)
    merge_h5ad_obs(table, Path(h5ad_path), sample_id)
    added = sorted(set(table.obs.columns) - before)
    if not added:
        print("  no new columns — left untouched")
        return True

    # write_element(overwrite=True) refuses to overwrite inside the store it is
    # reading from. Drop the on-disk element first, then write the in-memory one.
    table.obs = table.obs.copy()
    sdata.delete_element_from_disk("table")
    sdata.write_element("table")
    print(f"  rewrote table with {added}")
    return True


def verify(out_path: Path) -> bool:
    """Read the object back and report what landed in it."""
    import spatialdata as sd
    try:
        sdata = sd.read_zarr(out_path)
        t = sdata.tables["table"]
        print(f"  VERIFIED {out_path.name}")
        print(f"    table  : {t.n_obs:,} cells x {t.n_vars:,} genes")
        print(f"    shapes : {', '.join(f'{k} ({len(v):,})' for k, v in sdata.shapes.items())}")
        if sdata.points:
            for k, v in sdata.points.items():
                print(f"    points : {k} ({len(v):,} rows)")
        else:
            print("    points : none (protein sample)")
        obs_extra = [c for c in ("cell_type", "cell_type_detailed") if c in t.obs.columns]
        print(f"    obs    : {t.obs.shape[1]} columns"
              + (f", incl. {obs_extra}" if obs_extra else ", no cell types"))
        return True
    except Exception as e:
        print(f"  VERIFY FAILED for {out_path}: {type(e).__name__}: {e}")
        return False


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def discover(root: Path) -> list:
    """Every sample directory containing a flat-file export, under a flatFiles root."""
    seen = {}
    for expr in root.rglob("*_exprMat_file.csv.gz"):
        if expr.name.startswith("._"):
            continue
        run_dir = expr.parent
        # Two layouts in the wild:
        #   <sample_id>/flatFiles/<run>/files   -> sample dir is two levels up
        #   <sample_id>/flatFiles/files         -> sample dir is one level up
        if run_dir.parent.name == "flatFiles":
            sample_dir = run_dir.parent.parent
        elif run_dir.name == "flatFiles":
            sample_dir = run_dir.parent
        else:
            sample_dir = run_dir
        seen.setdefault(sample_dir.name, sample_dir)
    return sorted(seen.items())


def parse_args():
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--sample", help="sample_id to build (with --raw-dir)")
    mode.add_argument("--batch", action="store_true",
                      help="build every sample found under --root")

    p.add_argument("--raw-dir", type=Path, help="sample directory (single-sample mode)")
    p.add_argument("--root", type=Path, help="flatFiles root (batch mode)")
    p.add_argument("--h5ad", type=Path, help="organ-level h5ad to merge cell types from")
    p.add_argument("--h5ad-dir", type=Path,
                   help="directory of organ-level RNA h5ads (SAHA_<ORGAN>_RNA.h5ad)")
    p.add_argument("--h5ad-dir-prt", type=Path,
                   help="directory of organ-level protein h5ads (SAHA_<ORGAN>_PRT.h5ad)")
    p.add_argument("--out-dir", type=Path, default=Path("data/zarr"))
    p.add_argument("--scratch", type=Path, default=Path("/tmp"),
                   help="scratch space for transcript parquet staging")
    p.add_argument("--only", nargs="*", help="restrict batch to these sample_ids")
    p.add_argument("--from-manifest", type=Path,
                   help="restrict batch to cosmx sample_ids in this samples.parquet — "
                        "excludes pre-commercial runs and other non-release samples")
    p.add_argument("--skip-existing", action="store_true",
                   help="leave already-built zarrs alone")
    p.add_argument("--patch-obs", action="store_true",
                   help="do not rebuild; re-annotate existing zarrs from the h5ads "
                        "(adds qc_pass and cell types, rewrites only the table)")
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


def h5ad_for(sample_id: str, rna_dir: "Path | None",
             prt_dir: "Path | None" = None) -> "Path | None":
    """SAHA_CR_APE_A9_SJ01 -> <rna_dir>/SAHA_APE_RNA.h5ad
       SAHA_CP_APE_A9_SJ01 -> <prt_dir>/SAHA_APE_PRT.h5ad"""
    parts = sample_id.split("_")
    if len(parts) < 3:
        return None
    assay = parts[1]
    directory, suffix = ((rna_dir, "RNA") if assay.startswith("CR")
                         else (prt_dir, "PRT") if assay.startswith("CP")
                         else (None, None))
    if directory is None:
        return None
    candidate = directory / f"SAHA_{parts[2]}_{suffix}.h5ad"
    return candidate if candidate.exists() else None


def main():
    args = parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    if args.sample:
        if not args.raw_dir:
            sys.exit("ERROR: --raw-dir is required with --sample")
        todo = [(args.sample, args.raw_dir)]
    else:
        if not args.root:
            sys.exit("ERROR: --root is required with --batch")
        todo = discover(args.root)
        found = {s for s, _ in todo}
        if args.from_manifest:
            man = pd.read_parquet(args.from_manifest)
            wanted = set(man.loc[man["platform"] == "cosmx", "sample_id"])
            todo = [(s, d) for s, d in todo if s in wanted]
            absent = sorted(wanted - found)
            if absent:
                print(f"NOTE: {len(absent)} manifest sample(s) have no flat files "
                      f"on disk and will not be built: {', '.join(absent)}\n")
        if args.only:
            todo = [(s, d) for s, d in todo if s in args.only]
        if not todo:
            sys.exit(f"ERROR: no flat-file exports found under {args.root}")

    print(f"{len(todo)} sample(s) to build\n")
    built, failed, skipped = [], [], []

    for sample_id, sample_dir in todo:
        out_path = args.out_dir / f"{sample_id}.zarr"
        if args.skip_existing and out_path.exists():
            print(f"[skip] {sample_id} — already built")
            skipped.append(sample_id)
            continue
        if args.dry_run:
            try:
                run_dir = find_run_dir(Path(sample_dir))
                files = flat_files(run_dir)
                kinds = [k for k, v in files.items() if v]
                print(f"[dry-run] {sample_id}: {run_dir.name} -> {out_path}")
                print(f"          has {', '.join(kinds)}")
            except Exception as e:
                print(f"[dry-run] {sample_id}: ERROR {type(e).__name__}: {e}")
                failed.append(sample_id)
            continue

        print(f"\n=== {sample_id} ===")
        if args.patch_obs:
            if not out_path.exists():
                print(f"  not built yet — skipping")
                skipped.append(sample_id)
                continue
            try:
                patch_obs(out_path, sample_id,
                          args.h5ad or h5ad_for(sample_id, args.h5ad_dir, args.h5ad_dir_prt))
                built.append(sample_id)
            except Exception as e:
                import traceback
                traceback.print_exc()
                print(f"  FAILED {sample_id}: {type(e).__name__}: {e}")
                failed.append(sample_id)
            continue

        try:
            build_sample(
                Path(sample_dir), sample_id, out_path,
                args.h5ad or h5ad_for(sample_id, args.h5ad_dir, args.h5ad_dir_prt),
                args.scratch,
            )
            built.append(sample_id) if verify(out_path) else failed.append(sample_id)
        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"  FAILED {sample_id}: {type(e).__name__}: {e}")
            failed.append(sample_id)

    if not args.dry_run:
        print(f"\n{'=' * 60}")
        print(f"built   {len(built)}: {', '.join(built) if built else '—'}")
        if skipped:
            print(f"skipped {len(skipped)}")
        if failed:
            print(f"FAILED  {len(failed)}: {', '.join(failed)}")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
