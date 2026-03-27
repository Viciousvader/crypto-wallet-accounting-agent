from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def save_json(filepath: Path, payload: Any) -> None:
    ensure_directory(filepath.parent)
    with filepath.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
