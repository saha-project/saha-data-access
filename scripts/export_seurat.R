#!/usr/bin/env Rscript
# export_seurat.R
# Dump a Seurat object to the platform-neutral intermediate that
# build_from_seurat.py consumes.
#
# Used for the Northwestern / Dulai IBD cohort, which ships only as 52 Seurat
# RDS objects -- there is no AtoMx flat-file export and no h5ad. (The h5ad that
# sits beside them, SAHA_IBD_RNA.h5ad, is the WCM IBD_A1/IBD_A2 data already in
# v1, not this cohort.)
#
# Writes, per object:
#   counts.mtx / features.txt / cells.txt     default assay, raw counts
#   controls.mtx / controls_features.txt      negprobes+falsecode (CosMx) or
#                                             Blank/Control codewords (Xenium)
#   obs.csv                                   meta.data + centroid x/y
#   polygons.csv                              segmentation boundaries
#   meta.json                                 assay names, platform, counts
#
# Feature names go one per line, never whitespace-split: CosMx protein markers
# legitimately contain spaces and splitting shifts every name after the first.
#
# Usage:
#   Rscript export_seurat.R <in.RDS> <out_dir>

suppressPackageStartupMessages({
  library(Seurat); library(SeuratObject); library(Matrix)
})

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 2) stop("usage: export_seurat.R <in.RDS> <out_dir>")
in_rds <- args[[1]]; out_dir <- args[[2]]
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

o <- readRDS(in_rds)
main <- DefaultAssay(o)

# ---- counts -----------------------------------------------------------------
counts <- LayerData(o[[main]], layer = "counts")
writeMM(as(counts, "CsparseMatrix"), file.path(out_dir, "counts.mtx"))
writeLines(rownames(counts), file.path(out_dir, "features.txt"))
writeLines(colnames(counts), file.path(out_dir, "cells.txt"))

# ---- control probes ---------------------------------------------------------
# Every non-default assay is a control: CosMx has negprobes + falsecode, Xenium
# has BlankCodeword + ControlCodeword + ControlProbe. They go into var beside
# the targets, matching create_cosmx_zarr.py, so a user can compute background
# from the same object.
ctrl_assays <- setdiff(Assays(o), main)
if (length(ctrl_assays)) {
  mats <- list(); names_all <- character(0)
  for (a in ctrl_assays) {
    m <- LayerData(o[[a]], layer = "counts")
    m <- m[, colnames(counts), drop = FALSE]      # same cell order as counts
    mats[[a]] <- m
    names_all <- c(names_all, rownames(m))
  }
  allm <- do.call(rbind, mats)
  writeMM(as(allm, "CsparseMatrix"), file.path(out_dir, "controls.mtx"))
  writeLines(names_all, file.path(out_dir, "controls_features.txt"))
}

# ---- obs + centroids --------------------------------------------------------
md <- o@meta.data
md$cell <- rownames(md)

im <- Images(o)[[1]]
fov <- o[[im]]
cent <- GetTissueCoordinates(fov[["centroids"]])
# GetTissueCoordinates returns x, y, cell -- align it to the counts cell order
rownames(cent) <- as.character(cent$cell)
cent <- cent[md$cell, , drop = FALSE]
md$x_centroid_px <- cent$x
md$y_centroid_px <- cent$y

md <- md[match(colnames(counts), md$cell), , drop = FALSE]
stopifnot(identical(as.character(md$cell), colnames(counts)))
write.csv(md, file.path(out_dir, "obs.csv"), row.names = FALSE)

# ---- segmentation polygons --------------------------------------------------
if ("segmentation" %in% names(fov@boundaries)) {
  seg <- GetTissueCoordinates(fov[["segmentation"]])
  write.csv(seg, file.path(out_dir, "polygons.csv"), row.names = FALSE)
  n_poly <- length(unique(seg$cell))
} else {
  n_poly <- 0
}

# ---- provenance -------------------------------------------------------------
meta <- list(
  source_rds   = normalizePath(in_rds),
  main_assay   = main,
  ctrl_assays  = ctrl_assays,
  n_cells      = ncol(counts),
  n_features   = nrow(counts),
  n_cells_with_polygon = n_poly,
  image_name   = im,
  seurat_version = as.character(utils::packageVersion("Seurat"))
)
writeLines(jsonlite::toJSON(meta, auto_unbox = TRUE, pretty = TRUE),
           file.path(out_dir, "meta.json"))

cat(sprintf("%s -> %s : %d cells x %d features, %d control probes, %d polygons\n",
            basename(in_rds), out_dir, ncol(counts), nrow(counts),
            if (length(ctrl_assays)) length(names_all) else 0L, n_poly))
