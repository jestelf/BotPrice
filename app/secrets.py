import os
import json
from pathlib import Path
from typing import Any

import yaml


def _load_from_file(path: str) -> None:
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


def _load_from_vault(addr: str, token: str, secret_path: str) -> None:
    try:
        import hvac

        client = hvac.Client(url=addr, token=token)
        secret = client.secrets.kv.v2.read_secret_version(path=secret_path)
    except Exception:
        return
    data: dict[str, Any] = secret.get("data", {}).get("data", {}) or {}
    for k, v in data.items():
        os.environ.setdefault(k, str(v))


def load_secrets() -> None:
    path = os.getenv("SECRETS_FILE")
    if path:
        _load_from_file(path)
        return
    addr = os.getenv("VAULT_ADDR")
    token = os.getenv("VAULT_TOKEN")
    secret_path = os.getenv("VAULT_PATH")
    if addr and token and secret_path:
        _load_from_vault(addr, token, secret_path)
