#!/usr/bin/env python3
"""Fail CI if the client's two version declarations disagree.

Compares ``pyproject.toml`` ``[project].version`` against
``src/turbobulk_client/__init__.py`` ``__version__``.

``__version__`` is read with a regex rather than by importing the package: the
client's ``__init__`` imports ``.client``, which needs ``requests``, and this
runs with no dependencies installed. Needs Python 3.11+ for ``tomllib``.
"""

import re
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PYPROJECT = ROOT / "pyproject.toml"
INIT = ROOT / "src" / "turbobulk_client" / "__init__.py"


def pyproject_version(path):
    return tomllib.loads(path.read_text())["project"]["version"]


def init_version(path):
    match = re.search(r"""^__version__\s*=\s*['"]([^'"]+)""", path.read_text(), re.M)
    if not match:
        raise SystemExit(f"no __version__ assignment found in {path}")
    return match.group(1)


def main():
    declared = pyproject_version(PYPROJECT)
    exported = init_version(INIT)
    if declared != exported:
        sys.exit(f"version mismatch:\n  {PYPROJECT} says {declared!r}\n  {INIT} says {exported!r}")
    print(f"OK: both declare {declared!r}")


if __name__ == "__main__":
    main()
