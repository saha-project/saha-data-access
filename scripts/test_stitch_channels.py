"""Channel naming and canvas geometry must not assume the commercial instrument.

The pre-commercial CRC runs write ChannelOrder "Unknown" and 3648x5472 tiles;
iterating that string produced 7 bogus names for a 5-channel image, and the
hardcoded 4256 tile made the canvas too narrow to hold a 5472px-wide FOV.
"""
import sys
import numpy as np, pandas as pd
sys.path.insert(0, __file__.rsplit("/", 1)[0])
from stitch_cosmx_images import FALLBACK_CHANNELS, canvas_geometry

# --- channel order acceptance rule (mirrors channel_names) ---
def accept(declared, n_chan):
    return bool(declared and len(declared) == n_chan
                and all(c in FALLBACK_CHANNELS for c in declared))

assert accept("BGYRU", 5)
assert not accept("Unknown", 5)     # the CRC case: 7 chars, unknown codes
assert not accept("BGYRU", 4)       # length must match the array
assert not accept("", 5)
assert "Unknown"[:5] != "BGYRU"     # truncation alone would not have saved us

# --- canvas must hold a non-square tile ---
off = pd.DataFrame({"ox": [0.0, 5000.0], "oy": [0.0, -3000.0]}).rename_axis("fov")
g = canvas_geometry(off, 1, tile=(3648, 5472))
assert g["full"][1] >= 5000 + 5472, g["full"]   # width fits the rightmost tile
assert g["full"][0] >= 3000 + 3648, g["full"]   # height fits the lowest tile
sq = canvas_geometry(off, 1, tile=(4256, 4256))
assert sq["full"][1] == 5000 + 4256              # square case unchanged
assert 0 < g["fill"] <= 1
print("ok")
