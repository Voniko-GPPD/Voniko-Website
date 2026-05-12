"""Pytest bootstrap for dmp-services tests.

The dmp-services directory is a flat module layout (no package root), so this
``conftest.py`` adds the directory to ``sys.path`` once at collection time,
allowing tests to ``import dmp_service`` without per-file ``sys.path`` hacks.
"""

import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)
