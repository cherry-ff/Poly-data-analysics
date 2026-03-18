from __future__ import annotations

from pathlib import Path

import pytest

from app.config import RuntimeConfig
from security.crypto import CryptoManager
from security.private_key import clear_cached_private_key, set_decrypt_password


def test_crypto_manager_round_trip() -> None:
    manager = CryptoManager()
    encrypted = manager.encrypt_secret("secret_value", "pass123")
    assert manager.decrypt_secret(encrypted, "pass123") == "secret_value"


def test_runtime_config_decrypts_private_key_from_env_file(tmp_path: Path) -> None:
    clear_cached_private_key()
    set_decrypt_password(None)

    manager = CryptoManager()
    encrypted = manager.encrypt_secret("0xabc123", "pw")
    env_path = tmp_path / ".env"
    env_path.write_text(
        "\n".join(
            [
                "POLY15_EXEC_DRY_RUN=0",
                "POLY15_PM_ENCRYPTED_PRIVATE_KEY=" + encrypted,
                "POLY15_PM_FUNDER=0xfunder",
            ]
        ),
        encoding="utf-8",
    )

    config = RuntimeConfig.from_env(
        decrypt_password="pw",
        env_path=str(env_path),
        strict_encrypted_private_key=True,
    )

    assert config.execution.private_key == "0xabc123"
    assert config.execution.encrypted_private_key == encrypted


def test_runtime_config_rejects_missing_password_for_encrypted_key(tmp_path: Path) -> None:
    clear_cached_private_key()
    set_decrypt_password(None)

    manager = CryptoManager()
    encrypted = manager.encrypt_secret("0xabc123", "pw")
    env_path = tmp_path / ".env"
    env_path.write_text(
        "POLY15_PM_ENCRYPTED_PRIVATE_KEY=" + encrypted,
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="encrypted private key"):
        RuntimeConfig.from_env(
            env_path=str(env_path),
            strict_encrypted_private_key=True,
        )
