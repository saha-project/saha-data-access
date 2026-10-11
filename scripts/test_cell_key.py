"""Multi-slide cell ids. CRC ran slides 1-3 in one batch; the exprMat carries no
slide column, so a hardcoded `c_1_` silently joined zero rows for slides 2 and 3."""
import sys, pandas as pd
sys.path.insert(0, __file__.rsplit("/", 1)[0])
from create_cosmx_zarr import cell_key, slide_of

expr = pd.DataFrame({"fov": [1, 1, 2], "cell_ID": [1, 2, 1]})          # no `cell` column
for slide in (1, 2, 3):
    meta = pd.DataFrame({"cell": [f"c_{slide}_1_1", f"c_{slide}_1_2", f"c_{slide}_2_1"],
                         "fov": [1, 1, 2], "cell_ID": [1, 2, 1]})
    assert slide_of(meta) == slide, slide
    keys = cell_key(expr, "exprMat", slide_of(meta))
    assert list(keys) == list(cell_key(meta, "metadata")), f"slide {slide} keys diverge"

# slide 1 must be byte-identical to the pre-fix output, or the 22 v1 objects' ids move
assert list(cell_key(expr, "exprMat")) == ["c_1_1_1", "c_1_1_2", "c_1_2_1"]
assert slide_of(pd.DataFrame({"fov": [1]})) == 1          # no `cell` column -> 1
assert slide_of(pd.DataFrame({"cell": ["weird"]})) == 1   # unparseable -> 1
print("ok")
