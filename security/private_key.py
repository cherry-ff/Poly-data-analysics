from __future__ import annotations

from os import getenv

from app.env import load_project_env
from security.crypto import CryptoManager

_cached_private_key: str | None = None
_decrypt_password: str | None = None


def set_decrypt_password(password: str | None) -> None:
    global _decrypt_password
    _decrypt_password = password


def clear_cached_private_key() -> None:
    global _cached_private_key
    _cached_private_key = None


def resolve_private_key_from_env(
    *,
    decrypt_password: str | None = None,
    env_path: str | None = None,
    strict_encrypted_private_key: bool = False,
) -> str:
    """Resolve Polymarket private key from env, with encrypted-key support."""
    global _cached_private_key
    load_project_env(env_path)

    if decrypt_password is not None:
        set_decrypt_password(decrypt_password)

    if _cached_private_key:
        return _cached_private_key

    encrypted_key = _env_first("POLY15_PM_ENCRYPTED_PRIVATE_KEY", "ENCRYPTED_PRIVATE_KEY")
    plain_key = _env_first("POLY15_PM_PRIVATE_KEY", "PRIVATE_KEY")

    if encrypted_key:
        password = decrypt_password or _decrypt_password
        if not password:
            if strict_encrypted_private_key:
                raise ValueError(
                    "encrypted private key is configured but no decrypt password was provided"
                )
            return ""
        _cached_private_key = CryptoManager().decrypt_secret(encrypted_key, password)
        return _cached_private_key

    if plain_key:
        _cached_private_key = plain_key
        return _cached_private_key

    return ""


def _env_first(*names: str) -> str:
    for name in names:
        value = getenv(name, "").strip()
        if value:
            return value
    return ""
