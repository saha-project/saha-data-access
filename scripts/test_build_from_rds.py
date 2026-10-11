"""Feature names may contain spaces; whitespace-splitting corrupts the var index."""
import sys, tempfile, pathlib
sys.path.insert(0, __file__.rsplit("/", 1)[0])
from build_cosmx_from_rds import read_lines

p = pathlib.Path(tempfile.mkdtemp()) / "features.txt"
p.write_text("PanCK\nNF-kB p65\nMs IgG1\nCD45\n")
assert read_lines(p) == ["PanCK", "NF-kB p65", "Ms IgG1", "CD45"]
assert len(p.read_text().split()) == 6, "whitespace split would have given 6, not 4"
p.write_text("A\n\nB\n")                       # blank lines ignored
assert read_lines(p) == ["A", "B"]
print("ok")
