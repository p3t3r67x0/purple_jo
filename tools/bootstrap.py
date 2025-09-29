"""Utility helpers to bootstrap standalone execution of tools scripts."""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Final

_PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parents[1]

if __name__ == "tools.bootstrap":  # pragma: no cover - alias for script imports
    sys.modules.setdefault("bootstrap", sys.modules[__name__])
else:  # pragma: no cover - alias for module imports
    sys.modules.setdefault("tools.bootstrap", sys.modules[__name__])


def setup() -> Path:
    """Ensure the project root is available on ``sys.path`` and return it."""
    project_root = _PROJECT_ROOT
    project_root_str = str(project_root)
    if project_root_str not in sys.path:
        sys.path.insert(0, project_root_str)
    return project_root


__all__ = ["setup"]
