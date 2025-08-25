import base64
import json
import os
from typing import List

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from sqlalchemy.types import TypeDecorator, String, Text

from .config import settings

_keys: List[bytes] = [
    base64.urlsafe_b64decode(k) for k in settings.DATA_ENCRYPTION_KEY.split(",")
]
_encryptor = AESGCM(_keys[0])
_decryptors = [AESGCM(k) for k in _keys]


def encrypt_text(value: str) -> str:
    nonce = os.urandom(12)
    data = value.encode("utf-8")
    enc = _encryptor.encrypt(nonce, data, None)
    return base64.urlsafe_b64encode(nonce + enc).decode("utf-8")


def decrypt_text(token: str) -> str:
    raw = base64.urlsafe_b64decode(token)
    nonce, data = raw[:12], raw[12:]
    for aes in _decryptors:
        try:
            dec = aes.decrypt(nonce, data, None)
            return dec.decode("utf-8")
        except Exception:
            continue
    raise ValueError("Не удалось расшифровать данные")

class EncryptedStr(TypeDecorator):
    impl = String
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        return encrypt_text(str(value))

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return decrypt_text(value)

class EncryptedInt(EncryptedStr):
    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return int(decrypt_text(value))

class EncryptedJSON(TypeDecorator):
    impl = Text
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        return encrypt_text(json.dumps(value))

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return json.loads(decrypt_text(value))
