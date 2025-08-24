import os
import json
from pathlib import Path
from typing import Any

import yaml

def load_secrets() -> None:
    path = os.getenv("SECRETS_FILE")
    if not path:
        return
    p = Path(path)
    if not p.exists():
        return
    data: dict[str, Any]
    try:
        if p.suffix in {".yml", ".yaml"}:
            data = yaml.safe_load(p.read_text()) or {}
        else:
            data = json.loads(p.read_text())
    except Exception:
        return
    for k, v in data.items():
        os.environ.setdefault(k, str(v))
