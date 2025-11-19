import importlib
import pathlib
import pkgutil
import sys

patched = []
# Try direct module location
try:
    import kafka

    base = pathlib.Path(kafka.__file__).parent
    # target file
    p = base / "codec.py"
    if p.exists():
        txt = p.read_text()
        new = txt.replace(
            "from kafka.vendor.six.moves import range", "from six.moves import range"
        )
        new = new.replace("from kafka.vendor.six import", "from six import")
        if new != txt:
            p.write_text(new)
            print("Patched", p)
            patched.append(str(p))
except Exception as e:
    pass

# also scan site-packages inside venv for any other references
import glob
import re

site_libs = list(glob.glob("/opt/venv/lib/python*/site-packages"))
for lib in site_libs:
    import os

    for root, _, files in os.walk(lib):
        for f in files:
            if f.endswith(".py"):
                fp = os.path.join(root, f)
                s = open(fp, "r", encoding="utf-8", errors="ignore").read()
                s2 = s.replace(
                    "from kafka.vendor.six.moves import range",
                    "from six.moves import range",
                )
                s2 = s2.replace("kafka.vendor.six", "six")
                if s2 != s:
                    open(fp, "w", encoding="utf-8").write(s2)
                    patched.append(fp)
if patched:
    print("Patched files count:", len(patched))
else:
    print("No files patched")
