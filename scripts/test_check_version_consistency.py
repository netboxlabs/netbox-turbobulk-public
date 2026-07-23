"""Tests for scripts/check_version_consistency.py. Stdlib unittest only.

Run with: python -m unittest scripts.test_check_version_consistency -v
"""

import tempfile
import unittest
from pathlib import Path

from scripts.check_version_consistency import (
    INIT,
    PYPROJECT,
    init_version,
    pyproject_version,
)


def _write(tmp, name, content):
    path = Path(tmp) / name
    path.write_text(content)
    return path


class VersionReadTests(unittest.TestCase):
    def test_reads_pyproject_version(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = _write(tmp, "pyproject.toml", '[project]\nversion = "0.1.9"\n')
            self.assertEqual(pyproject_version(path), "0.1.9")

    def test_reads_init_version(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = _write(tmp, "__init__.py", "__version__ = '0.1.9'\n")
            self.assertEqual(init_version(path), "0.1.9")

    def test_missing_init_version_fails_loudly(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = _write(tmp, "__init__.py", "CLIENT_NAME = 'turbobulk'\n")
            with self.assertRaises(SystemExit):
                init_version(path)

    def test_reads_the_real_init_without_importing_it(self):
        # __init__ imports .client, which needs requests, and the job installs
        # nothing -- so this would raise ModuleNotFoundError if it imported.
        self.assertIn("from .client import", INIT.read_text())
        self.assertIsInstance(init_version(INIT), str)
        self.assertIsInstance(pyproject_version(PYPROJECT), str)


if __name__ == "__main__":
    unittest.main()
