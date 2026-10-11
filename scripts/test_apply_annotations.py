"""join_key must survive slide-number drift and stay unique."""
import sys
sys.path.insert(0, __file__.rsplit("/", 1)[0])
from apply_annotations import join_key

# GI_after_Niche re-keyed appendix as slide 2, ileum 3, colon 1; exports use 1
assert join_key(["c_1_10_5", "c_2_10_5", "c_3_10_5"]) == ["10_5"] * 3
# multi-digit fov and cell survive
assert join_key(["c_1_100_2048"]) == ["100_2048"]
# non-matching ids pass through untouched
assert join_key(["C16.S1_1_1", "7_1"]) == ["C16.S1_1_1", "7_1"]
# uniqueness within one acquisition is what the join relies on
ids = [f"c_1_{f}_{c}" for f in range(1, 40) for c in range(1, 500)]
assert len(set(join_key(ids))) == len(ids)
print("ok")

# `region` is spatialdata's region_key and is pre-populated with "cells" in every
# object -- writing it would silently clobber the table's link to shapes/cells.
from apply_annotations import COLS, RESERVED
assert not (set(COLS) & RESERVED), "a target column collides with a spatialdata key"
assert "pathology_region" in COLS and "region" not in COLS
print("ok - reserved names guarded")
