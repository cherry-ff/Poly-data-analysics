from __future__ import annotations

import base64
import os

from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC


class CryptoManager:
    """Password-based secret encryption helper for `.env` storage."""

    def _derive_key(self, password: str, salt: bytes) -> bytes:
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=480000,
        )
        return base64.urlsafe_b64encode(kdf.derive(password.encode()))

    def encrypt_secret(self, plain_secret: str, password: str) -> str:
        salt = os.urandom(16)
        key = self._derive_key(password, salt)
        token = Fernet(key).encrypt(plain_secret.encode())
        return base64.urlsafe_b64encode(salt + token).decode()

    def decrypt_secret(self, encrypted_str: str, password: str) -> str:
        try:
            decoded = base64.urlsafe_b64decode(encrypted_str.encode())
            salt, token = decoded[:16], decoded[16:]
            key = self._derive_key(password, salt)
            return Fernet(key).decrypt(token).decode()
        except InvalidToken as exc:
            raise ValueError("failed to decrypt private key: password is wrong or data is corrupted") from exc
        except Exception as exc:
            raise ValueError(f"failed to decrypt private key: {exc}") from exc
