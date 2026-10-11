#!/usr/bin/env python3
"""Propose cell_type_broad (SAHA 13-type vocabulary) for one record's objects. Computes only:
writes <out>/<sample>.cell_type_broad.parquet + <out>/<sample>.annot.jpg + <out>/summary.json.

  mapped     objects with labels: fine/protein vocabularies mapped onto the broad one
  transfer   unlabelled objects: logistic regression trained on labelled objects of the same
             platform family (CosMx RNA -> Xenium on shared genes; CosMx protein -> CosMx protein)
Usage: annotate_broad.py RECORD_DIR OUTDIR [REF_RECORD_DIR ...]
  ANNOT_FORCE_TRANSFER=1 ignores the record's own labels (alternative proposal for review)
  REF_RECORD_DIR: extra CosMx references (their approved cell_type_broad), for records with no labels of their own
"""
import json
import os
import sys
import warnings
from pathlib import Path

import anndata as ad
import numpy as np
import pandas as pd
import scipy.sparse as sp
import zarr

warnings.filterwarnings("ignore")
BROAD = ["Epithelial", "Stem", "Lymphoid-T", "Lymphoid-B", "Lymphoid-Plasma", "Lymphoid-DC", "Lymphoid-Other",
         "Myeloid", "Endothelial", "Mesenchymal-Stromal", "Neuronal-Glial", "Erythroid"]
FINE = {  # TAP 46/47-type vocabulary (GI_after_Niche) -> broad
    **{k: "Lymphoid-T" for k in ["Activated CD4 T", "Activated CD8 T", "CD8 Tmem", "SELL+ CD4 T", "Treg", "gdT",
                                  "TRGV2 gdT", "TRGV4 gdT", "TRGV5/7 gdT"]},
    **{k: "Lymphoid-Other" for k in ["NK cell", "ILC3"]},
    **{k: "Lymphoid-B" for k in ["Memory B", "Naive B"]},
    **{k: "Lymphoid-Plasma" for k in ["IgA plasma cell", "IgG plasma cell"]},
    **{k: "Lymphoid-DC" for k in ["cDC1", "cDC2"]},
    **{k: "Myeloid" for k in ["Macrophages", "LYVE1+ Macrophage", "Monocytes", "Mast cell"]},
    **{k: "Epithelial" for k in ["Colonocyte", "Goblet cell", "BEST2+ Goblet cell", "BEST4+ epithelial",
                                  "L cells (PYY+)", "Microfold cell", "Tuft", "TA"]},
    "Stem cells": "Stem",
    **{k: "Mesenchymal-Stromal" for k in ["Stromal 1 (ADAMDEC1+)", "Stromal 1 (CCL11+)", "Stromal 2 (NPY+)",
                                           "Stromal 3 (C7+)", "Transitional Stromal 3 (C3+)", "myofibroblast",
                                           "myofibroblast (RSPO2+)", "Contractile pericyte (PLN+)", "T reticular",
                                           "mLN Stroma (FMO2+)"]},
    **{k: "Endothelial" for k in ["Mature arterial EC", "Mature venous EC", "arterial capillary",
                                   "LEC1 (ACKR4+)", "LEC6 (ADAMTS4+)"]},
    "Adult Glia": "Neuronal-Glial",
}
PROTEIN = {  # CosMx protein cell_type_detailed -> broad
    "bcell": "Lymphoid-B", "cd4_tcell": "Lymphoid-T", "cd8_tcell": "Lymphoid-T", "tcell": "Lymphoid-T",
    "nk": "Lymphoid-Other", "dendritic": "Lymphoid-DC", "macrophage_monocyte": "Myeloid", "neutrophil": "Myeloid",
    "epithelial": "Epithelial", "endothelial": "Endothelial", "fibroblast": "Mesenchymal-Stromal",
    "vascular_smooth_muscle": "Mesenchymal-Stromal", "adipose": "Mesenchymal-Stromal",
}
DULAI = {  # Dulai IBD 8-type vocabulary -> broad; T/NK is mixed, mapped to its majority (T)
    "B": "Lymphoid-B", "Plasma": "Lymphoid-Plasma", "T/NK": "Lymphoid-T", "Myeloid": "Myeloid",
    "Epithelial": "Epithelial", "Endothelial": "Endothelial", "Mesenchymal": "Mesenchymal-Stromal",
    "Neuron": "Neuronal-Glial",
}
CRC = {  # CRC TAP00 Insitutype immune reference: "stroma" is every non-immune cell (tumour included) -> NA
    "endothelial": "Endothelial", "fibroblast": "Mesenchymal-Stromal", "plasmablast": "Lymphoid-Plasma",
    **{k: "Lymphoid-T" for k in ["T_CD4_memory", "T_CD4_naive", "T_CD8_memory", "T_CD8_naive", "Treg"]},
    "NK": "Lymphoid-Other", "B": "Lymphoid-B", "pDC": "Lymphoid-DC", "mDC": "Lymphoid-DC",
    **{k: "Myeloid" for k in ["macrophage", "monocyte", "neutrophil", "mast"]}, "stroma": None,
}
LIVER = {  # liver-atlas vocabulary (TAP liver RNA cell_type) -> broad; hepatocytes count as Epithelial
    **{f"Hep.{i}": "Epithelial" for i in range(1, 7)}, "Cholangiocytes": "Epithelial",
    "Stellate.cells": "Mesenchymal-Stromal", "Inflammatory.macrophages": "Myeloid", "Non.inflammatory.macrophages": "Myeloid",
    "Periportal.LSECs": "Endothelial", "Central.venous.LSECs": "Endothelial", "Portal.endothelial.cells": "Endothelial",
    "Antibody.secreting.B.cells": "Lymphoid-Plasma", "Mature.B.cells": "Lymphoid-B",
    "CD3..alpha.beta.T.cells": "Lymphoid-T", "gamma.delta.T.cells.1": "Lymphoid-T", "gamma.delta.T.cells.2": "Lymphoid-T",
    "NK.like.cells": "Lymphoid-Other", "Erthyroid.cells": "Erythroid",
}
MARKERS = {
    "rna": ["EPCAM", "KRT20", "LGR5", "MKI67", "CD3E", "CD4", "CD8A", "NKG7", "MS4A1", "CD79A", "MZB1", "LAMP3",
            "CD68", "CD14", "KIT", "PECAM1", "VWF", "ACTA2", "DES", "CHGA"],
    "protein": ["EpCAM", "Ki-67", "CD3", "CD4", "CD8", "CD56", "CD20", "CD19", "CD138", "CD11c", "HLA-DR",
                "CD68", "CD163", "CD14", "CD31", "SMA", "Vimentin", "FABP4"],
}


def load(z):
    tg = zarr.open_group(str(z / "tables"), mode="r")
    return ad.read_zarr(str(z / "tables" / list(tg.keys())[0]))


def counts(t):
    # Xenium tables keep scaled values in X and counts in layers["raw"]; CosMx keep counts in X
    X = next((t.layers[k] for k in ("counts", "raw") if k in t.layers), t.X)
    return X.tocsr() if sp.issparse(X) else sp.csr_matrix(X)


def rna_features(t, genes):
    """log1p(CP10k over the shared genes), z-scored within the object: removes per-platform gene efficiency."""
    X = counts(t)[:, [t.var_names.get_loc(g) for g in genes]].astype(np.float32)
    tot = np.asarray(X.sum(1)).ravel()
    X = np.log1p(sp.diags(1e4 / np.maximum(tot, 1)) @ X).toarray()
    return (X - X.mean(0)) / (X.std(0) + 1e-6), tot


def protein_features(t, feats):
    X = counts(t)[:, [t.var_names.get_loc(g) for g in feats]].toarray().astype(np.float32)
    X = np.arcsinh(X / 5.0)
    return (X - X.mean(0)) / (X.std(0) + 1e-6)


def labels_of(t, kind):
    o = t.obs
    ct = o["cell_type"].astype(object) if "cell_type" in o else None
    if ct is None:
        return None, None
    s = set(ct.dropna().astype(str))
    if not s - {"NA", "nan"}:   # column present but empty: unlabelled
        return None, None
    if s <= set(BROAD) | {"NA"}:
        return ct.where(ct.isin(BROAD)), "Seurat annotation (SAHA broad vocabulary)"
    if kind == "protein" and "cell_type_detailed" in o:
        return o["cell_type_detailed"].astype(object).map(PROTEIN), "mapped from cell_type_detailed (CosMx protein vocabulary)"
    if s <= set(LIVER) | {"NA", "nan"}:
        return ct.map(LIVER), "mapped from cell_type (liver-atlas vocabulary; hepatocytes -> Epithelial)"
    if s <= set(CRC) | {"NA", "nan"}:
        return ct.map(CRC), "mapped from cell_type (CRC immune-reference vocabulary; catch-all 'stroma', which includes tumour, -> NA)"   # before FINE: 'Treg' is in both
    if s & set(FINE):
        miss = s - set(FINE) - {"NA", "nan"}
        assert not miss, f"unmapped fine labels: {miss}"
        return ct.map(FINE), "mapped from cell_type (TAP fine vocabulary)"
    if s <= set(PROTEIN) | {"immune", "Unknown", "NA", "nan"}:   # protein vocabulary in cell_type itself
        return ct.map(PROTEIN), "mapped from cell_type (CosMx protein vocabulary; generic 'immune'/'Unknown' -> NA)"
    if s <= set(DULAI) | {"NA", "nan"}:
        return ct.map(DULAI), "mapped from cell_type (Dulai 8-type vocabulary)"
    raise SystemExit(f"unknown vocabulary: {sorted(s)[:20]}")


def fit(Xr, yr, seed=0):
    from sklearn.linear_model import LogisticRegression
    rng = np.random.default_rng(seed)
    keep = np.concatenate([rng.permutation(np.flatnonzero(yr == c))[:15000] for c in np.unique(yr)])
    return LogisticRegression(max_iter=2000, C=0.5).fit(Xr[keep], yr[keep])


def figure(path, sid, xy, lab, conf, Xm, markers, src, cv):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    cmap = plt.get_cmap("tab20")
    lut = {c: cmap(i) for i, c in enumerate(BROAD)}
    lut["NA"] = (0.8, 0.8, 0.8, 1)
    lab = pd.Series(lab).fillna("NA").astype(str).values
    fig, ax = plt.subplots(1, 3, figsize=(21, 7), gridspec_kw={"width_ratios": [1.2, 0.5, 1.1]})
    rng = np.random.default_rng(0)
    sel = rng.choice(len(lab), min(len(lab), 80000), replace=False)
    ax[0].scatter(*xy[sel].T, s=0.3, c=[lut[l] for l in lab[sel]], lw=0)
    ax[0].set_aspect("equal"); ax[0].invert_yaxis(); ax[0].axis("off")
    ax[0].legend(handles=[plt.Line2D([], [], marker="o", ls="", color=lut[c], label=c) for c in BROAD + ["NA"] if c in set(lab)],
                 fontsize=7, loc="upper left", bbox_to_anchor=(1, 1), frameon=False)
    vc = pd.Series(lab).value_counts(normalize=True).reindex(BROAD + ["NA"]).dropna()
    ax[1].barh(vc.index[::-1], vc.values[::-1], color=[lut[c] for c in vc.index[::-1]])
    ax[1].set_xlabel("fraction of cells"); ax[1].tick_params(labelsize=7)
    types = [c for c in BROAD if c in set(lab)]
    M = np.array([Xm[lab == c].mean(0) for c in types]).reshape(len(types), len(markers))
    im = ax[2].imshow(M if len(types) else np.zeros((1, len(markers))), cmap="RdBu_r", vmin=-1.5, vmax=1.5, aspect="auto")
    ax[2].set_xticks(range(len(markers))); ax[2].set_xticklabels(markers, rotation=90, fontsize=7)
    ax[2].set_yticks(range(len(types))); ax[2].set_yticklabels(types, fontsize=7)
    plt.colorbar(im, ax=ax[2], fraction=0.03, label="mean z-scored expression")
    ax[2].set_title("markers by assigned type (should light up on the diagonal blocks)", fontsize=9)
    extra = "" if conf is None else f"  ·  median confidence {np.nanmedian(conf):.2f}, {np.mean(conf < 0.5):.1%} below 0.5 (cluster-rescued where the cluster agrees, else NA)"
    fig.suptitle(f"{sid}  ·  cell_type_broad  ·  {src}{extra}{cv}", fontsize=11)
    fig.tight_layout(); fig.savefig(path, dpi=70); plt.close(fig)


def xy_of(t):
    o = t.obs
    for a, b in (("CenterX_global_px", "CenterY_global_px"), ("x", "y"), ("x_centroid", "y_centroid")):
        if a in o and b in o:
            return o[[a, b]].to_numpy(float)
    if "spatial" in t.obsm:   # Xenium tables that keep centroids only in obsm
        return np.asarray(t.obsm["spatial"], float)[:, :2]
    return np.zeros((t.n_obs, 2))


def main():
    rec, out = Path(sys.argv[1]), Path(sys.argv[2])
    out.mkdir(parents=True, exist_ok=True)
    stores = sorted(p for p in rec.glob("*.zarr"))
    xen_rec = any("_XR_" in p.name for p in stores)   # Xenium references only serve Xenium targets
    extra = [p for r in sys.argv[3:] for p in sorted(Path(r).glob("*.zarr"))
             if "_XR_" not in p.name or (xen_rec and "cell_type_broad" in zarr.open_group(str(p / "tables"), mode="r")[
                 list(zarr.open_group(str(p / "tables"), mode="r").keys())[0]]["obs"])]
    T = {p.name[:-5]: load(p) for p in stores + extra}
    ref_only = {p.name[:-5] for p in extra}
    kind = {s: ("protein" if "_CP_" in s else "rna") for s in T}
    summary = {}
    labelled, todo = {}, []
    for s, t in T.items():
        if s in ref_only:
            y = t.obs["cell_type_broad"].astype(object)
            labelled[s] = (y.where(y.isin(BROAD)), "cell_type_broad (approved)")
            continue
        # ANNOT_FORCE_TRANSFER=1: ignore the record's own labels (an alternative proposal for review)
        y, src = (None, None) if os.environ.get("ANNOT_FORCE_TRANSFER") else labels_of(t, kind[s])
        (labelled.__setitem__(s, (y, src)) if y is not None else todo.append(s))

    # transfer models, one per platform family
    models = {}
    rna_ref = [s for s in labelled if kind[s] == "rna"]
    # one model per unlabelled RNA object (Xenium, or CosMx with no labels), on the genes it shares with the
    # references: a single intersection over every panel can leave ~100 genes (prostate: 121) and too little signal
    for x in [s for s in todo if kind[s] == "rna"]:
        # same-platform references when there are any (Xenium panel vs CosMx panel share few genes)
        rna_ref = [s for s in labelled if kind[s] == "rna" and ("_XR_" in s) == ("_XR_" in x)] or \
                  [s for s in labelled if kind[s] == "rna"]
        genes = sorted(set.intersection(*[set(T[s].var_names) for s in rna_ref + [x]]))
        F = {s: rna_features(T[s], genes)[0] for s in rna_ref}
        # honest estimate: hold out each reference object in turn
        accs = {}
        for h in rna_ref:
            tr = [s for s in rna_ref if s != h]
            Xr = np.vstack([F[s] for s in tr]); yr = np.concatenate([labelled[s][0].values for s in tr])
            ok = pd.notna(yr); m = fit(Xr[ok], yr[ok].astype(str))
            yh = labelled[h][0].values; okh = pd.notna(yh)
            accs[h] = float((m.predict(F[h][okh]) == yh[okh].astype(str)).mean())
        Xr = np.vstack([F[s] for s in rna_ref]); yr = np.concatenate([labelled[s][0].values for s in rna_ref])
        ok = pd.notna(yr)
        models[x] = (fit(Xr[ok], yr[ok].astype(str)), genes, accs)
        summary.setdefault("rna_transfer", {})[x] = {"reference": rna_ref, "n_shared_genes": len(genes),
                                                     "leave_one_object_out_accuracy": accs}
    prot_ref = [s for s in labelled if kind[s] == "protein"]
    cpt = [s for s in todo if kind[s] == "protein"]
    if cpt:
        feats = [f for f in T[prot_ref[0]].var_names if not f.startswith("Channel-")
                 and f.replace(" ", "") not in ("MsIgG1", "RbIgG") and all(f in T[s].var_names for s in prot_ref + cpt)]
        from sklearn.model_selection import cross_val_score
        from sklearn.linear_model import LogisticRegression
        Xr = np.vstack([protein_features(T[s], feats) for s in prot_ref])
        yr = np.concatenate([labelled[s][0].values for s in prot_ref]); ok = pd.notna(yr)
        idx = np.random.default_rng(0).choice(np.flatnonzero(ok), min(ok.sum(), 40000), replace=False)
        cv = cross_val_score(LogisticRegression(max_iter=2000, C=0.5), Xr[idx], yr[idx].astype(str), cv=5)
        models["protein"] = (fit(Xr[ok], yr[ok].astype(str)), feats, {"cv5_accuracy": float(cv.mean())})
        summary["protein_transfer"] = {"reference": prot_ref, "n_markers": len(feats), "cv5_accuracy": float(cv.mean())}

    for s, t in T.items():
        if s in ref_only:
            continue
        k = kind[s]
        if s in labelled:
            y, src = labelled[s]; conf = None; cvtxt = ""; rescued = None
            lab = y.values
        else:
            m, feats, acc = models[s if k == "rna" else "protein"]
            X = rna_features(t, feats)[0] if k == "rna" else protein_features(t, feats)
            P = m.predict_proba(X); conf = P.max(1)
            lab = np.where(conf >= 0.5, m.classes_[P.argmax(1)], None).astype(object)
            # cluster rescue: a low-confidence cell takes its leiden cluster's label when the
            # cluster's confident cells agree >= 70% (muscularis is thin in the CosMx FOVs used as reference)
            rescued = np.zeros(len(lab), bool)
            cl = next((c for c in ("leiden_res1.0", "leiden_res0.5", "leiden") if c in t.obs), None)
            if cl:
                cv_ = t.obs[cl].astype(object).fillna("NA").astype(str).values   # categorical astype(str) keeps NaN
                for c in np.unique(cv_):
                    idx = cv_ == c
                    vc = pd.Series(lab[idx]).dropna().value_counts(normalize=True)
                    if len(vc) and vc.iloc[0] >= 0.7:
                        fill = idx & pd.isna(lab)
                        lab[fill] = vc.index[0]; rescued |= fill
            ref = (summary["rna_transfer"][s] if k == "rna" else summary["protein_transfer"])["reference"]
            src = f"label transfer (logistic regression) from {', '.join(ref)}"
            cvtxt = (f"\nheld-out accuracy on reference objects: " +
                     (", ".join(f"{h.split('_')[-1]} {a:.2f}" for h, a in acc.items()) if k == "rna" else f"5-fold CV {acc['cv5_accuracy']:.2f}"))
        mk = [g for g in MARKERS[k] if g in t.var_names]
        Xm = (rna_features(t, mk)[0] if k == "rna" else protein_features(t, mk))
        df = pd.DataFrame({"cell_type_broad": pd.Categorical(lab, categories=BROAD), "cell_type_broad_source": src},
                          index=t.obs_names)
        if conf is not None:
            df["cell_type_broad_confidence"] = conf.astype(np.float32)
            df.loc[rescued, "cell_type_broad_source"] = src + "; low-confidence cell labelled by its leiden_res1.0 cluster (>=70% agreement)"
        df.to_parquet(out / f"{s}.cell_type_broad.parquet")
        figure(out / f"{s}.annot.jpg", s, xy_of(t), lab, conf, Xm, mk, src, cvtxt)
        vc = pd.Series(lab).fillna("NA").astype(str).value_counts(normalize=True).round(3).to_dict()
        summary[s] = {"source": src, "n": int(t.n_obs), "composition": vc,
                      "rescued_frac": None if conf is None else float(rescued.mean()),
                      "median_conf": None if conf is None else float(np.median(conf))}
        print(s, src, {k2: v for k2, v in list(vc.items())[:8]}, flush=True)
    (out / "summary.json").write_text(json.dumps(summary, indent=1))


if __name__ == "__main__":
    main()
