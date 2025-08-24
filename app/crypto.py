import base64
import json
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from sqlalchemy.types import TypeDecorator, String, Text

from .config import settings

_key = base64.urlsafe_b64decode(settings.DATA_ENCRYPTION_KEY)
_aes = AESGCM(_key)
_NONCE = b"\x00" * 12

def encrypt_text(value: str) -> str:
    data = value.encode("utf-8")
    enc = _aes.encrypt(_NONCE, data, None)
    return base64.urlsafe_b64encode(enc).decode("utf-8")

def decrypt_text(token: str) -> str:
    data = base64.urlsafe_b64decode(token)
    dec = _aes.decrypt(_NONCE, data, None)
    return dec.decode("utf-8")

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
