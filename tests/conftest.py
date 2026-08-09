"""
conftest.py

Pytest automatically loads this file before running any tests in
this folder. It adds src/ to the import path, so test files can do
`from transformations.cleaning import ...` the same way main.py does.
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent / "src"))