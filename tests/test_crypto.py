import importlib
import base64

from app import crypto, config


def test_encrypt_random_nonce():
    token1 = crypto.encrypt_text("data")
    token2 = crypto.encrypt_text("data")
    assert token1 != token2
    assert crypto.decrypt_text(token1) == "data"
    assert crypto.decrypt_text(token2) == "data"


def test_key_rotation(monkeypatch):
    k1 = base64.urlsafe_b64encode(b"1" * 32).decode()
    k2 = base64.urlsafe_b64encode(b"2" * 32).decode()
    monkeypatch.setenv("DATA_ENCRYPTION_KEY", ",".join([k1, k2]))
    config.settings.DATA_ENCRYPTION_KEY = ",".join([k1, k2])
    importlib.reload(crypto)
    token = crypto.encrypt_text("hello")
    monkeypatch.setenv("DATA_ENCRYPTION_KEY", ",".join([k2, k1]))
    config.settings.DATA_ENCRYPTION_KEY = ",".join([k2, k1])
    importlib.reload(crypto)
    assert crypto.decrypt_text(token) == "hello"
