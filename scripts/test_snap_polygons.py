"""snap_polygons_to_fovs moves only misplaced FOVs, and only by their offset."""
import geopandas as gpd
import numpy as np
from shapely.geometry import Point

from build_from_seurat import snap_polygons_to_fovs


def test_snaps_shifted_fov_and_leaves_good_one():
    cx = np.array([10.0, 20.0, 1000.0, 1010.0])
    cy = np.array([10.0, 20.0, 1000.0, 1010.0])
    fov = np.array([1, 1, 2, 2])
    shift = np.array([0, 0, 8512, 8512])          # FOV 2 sits on the wrong grid
    g = gpd.GeoDataFrame(geometry=[Point(x + s, y - s).buffer(3) for x, y, s in zip(cx, cy, shift)])
    out, moved = snap_polygons_to_fovs(g, cx, cy, fov)
    assert list(moved.index) == [2]
    c = out.geometry.centroid
    assert np.allclose(c.x, cx, atol=1e-6) and np.allclose(c.y, cy, atol=1e-6)


def test_noop_when_aligned():
    cx = cy = np.array([0.0, 5.0])
    g = gpd.GeoDataFrame(geometry=[Point(0, 0).buffer(2), Point(5, 5).buffer(2)])
    out, moved = snap_polygons_to_fovs(g, cx, cy, np.array([1, 1]))
    assert len(moved) == 0 and out is g
