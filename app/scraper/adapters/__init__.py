from functools import lru_cache
from pathlib import Path
import yaml


@lru_cache()
def _load_selectors() -> dict:
    path = Path(__file__).with_name("selectors.yaml")
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def get_selectors(name: str) -> dict:
    return _load_selectors().get(name, {})
