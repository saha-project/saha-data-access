"""Correct a registration with the per-window shifts measured by he_alignment_check.py.
The check measures, per 300 um window, where the H&E content sits relative to the cells (dx, dy um).
A smooth field of such shifts (rotation/shear the translation-only refinement cannot remove) is fit
as an affine d(p) and removed: p_true = p - d(p).
Usage: refine_he_affine.py CHECK_JSON REG_JSON OUT_JSON"""
import json, sys
import numpy as np
chk, reg = json.load(open(sys.argv[1])), json.load(open(sys.argv[2]))
um = reg["global_px_um"]; w = 300.0 / um
P = np.array([[d["x0"] + w / 2, d["y0"] + w / 2, 1.0] for d in chk["windows"]])
D = np.array([[d["dx"], d["dy"]] for d in chk["windows"]]) / um          # global px
M, *_ = np.linalg.lstsq(P, D, rcond=None)                                 # 3x2: d = P @ M
res = np.hypot(*((P @ M - D) * um).T)
print("per-window shift um:", np.round(np.hypot(*(D * um).T), 1).tolist())
print("affine fit residual um:", np.round(res, 1).tolist(), "median", round(float(np.median(res)), 1))
C = np.eye(3); C[:2, :] -= M.T                                            # p -> p - d(p)
A = np.array(reg["affine_he_fullres_px_to_global_px"])
reg["affine_he_fullres_px_to_global_px"] = (C @ A).tolist()
reg["affine_correction_from_check"] = {"check": sys.argv[1], "matrix": C.tolist(),
                                       "fit_residual_um_median": float(np.median(res))}
rot = np.degrees(np.arctan2(C[1, 0] - C[0, 1], C[0, 0] + C[1, 1]))
print(f"correction: rotation {rot:.3f} deg, translation at origin ({-M[2,0]*um:.1f}, {-M[2,1]*um:.1f}) um")
json.dump(reg, open(sys.argv[3], "w"), indent=1)
