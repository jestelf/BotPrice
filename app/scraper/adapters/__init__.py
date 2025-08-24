from functools import lru_cache
from pathlib import Path
from typing import Any
import json

import yaml

try:  # optional lxml for XPath fallback
    from lxml import etree
except Exception:  # pragma: no cover - lxml may be missing
    etree = None


@lru_cache()
def _load_selectors() -> dict:
    path = Path(__file__).with_name("selectors.yaml")
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def get_selectors(name: str) -> dict:
    return _load_selectors().get(name, {})


def _to_soup(node: Any):
    from bs4 import BeautifulSoup

    if hasattr(node, "select"):
        return node
    return BeautifulSoup(str(node), "html.parser")


def _json_query(data: Any, path: str):
    if not path:
        return data
    parts = path.split(".")
    cur = data
    for p in parts:
        if isinstance(cur, dict):
            cur = cur.get(p)
        elif isinstance(cur, list):
            try:
                cur = cur[int(p)]
            except Exception:
                return None
        else:
            return None
    return cur


def select_all(node: Any, selector: dict) -> list[Any]:
    """Возвращает все элементы по селектору с fallback CSS→XPath→JSON."""
    soup = _to_soup(node)
    css = selector.get("css") if selector else None
    if css:
        els = soup.select(css)
        if els:
            return els

    xpath = selector.get("xpath") if selector else None
    if xpath and etree is not None:
        try:
            tree = etree.HTML(str(node))
            els = tree.xpath(xpath)
            if els:
                from bs4 import BeautifulSoup

                return [
                    BeautifulSoup(etree.tostring(e, encoding="unicode"), "html.parser")
                    for e in els
                ]
        except Exception:
            pass

    json_path = selector.get("json") if selector else None
    if json_path:
        results = []
        for script in soup.find_all("script"):
            try:
                data = json.loads(script.string or "")
            except Exception:
                continue
            value = _json_query(data, json_path)
            if value is not None:
                results.append(value)
        if results:
            return results

    return []


def select_one(node: Any, selector: dict) -> Any:
    """Возвращает первый элемент по селектору с fallback."""
    els = select_all(node, selector)
    return els[0] if els else None
