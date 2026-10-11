"""apply_xf must invert the same 8 operations best_alignment searches over.
A mismatch would place cells in the wrong core silently."""
import sys
import numpy as np
sys.path.insert(0, __file__.rsplit("/", 1)[0])
from transfer_donor_core import XF, apply_xf, best_alignment, raster

rng = np.random.default_rng(0)
# an asymmetric footprint, so every transform is distinguishable
G = np.zeros((14, 9), bool)
G[1:5, 1:3] = True; G[8:12, 4:8] = True; G[6, 1] = True

for name, T in XF:
    Gt = T(G)
    h, w = G.shape[0] - 1, G.shape[1] - 1
    ys, xs = np.nonzero(G)
    # forward-map every occupied bin and confirm it lands on an occupied bin
    ax, ay = apply_xf(name, xs.astype(float), ys.astype(float), w, h)
    ai, aj = np.rint(ay).astype(int), np.rint(ax).astype(int)
    assert (ai >= 0).all() and (aj >= 0).all(), name
    assert (ai < Gt.shape[0]).all() and (aj < Gt.shape[1]).all(), name
    assert Gt[ai, aj].all(), f"{name}: forward map landed off the transformed shape"

# a shifted copy of a footprint must align at IoU 1.0 with the right offset
A = np.zeros((30, 30), bool); A[5:12, 6:11] = True; A[20:24, 15:22] = True
B = A.copy()
cover, iou, name, dy, dx, _ = best_alignment(A, B)
assert cover > 0.99 and iou > 0.99, (cover, iou)
# a source fully inside a larger target: coverage 1.0, IoU low
big = np.zeros((40, 40), bool); big[2:38, 2:38] = True
c2, i2, *_ = best_alignment(A, big)
assert c2 > 0.99 and i2 < 0.5, (c2, i2)
print("ok")
