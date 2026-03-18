"""Minimal import smoke tests.

Ensures that every Agent D module (and the existing Phase-1 skeleton) can be
imported without error.  This test file intentionally mirrors the import
validation listed in agent_progress.md §1.2 and extends it for Agent D.
"""

import pytest


def test_import_app() -> None:
    import app  # noqa: F401


def test_import_app_env() -> None:
    from app.env import load_project_env  # noqa: F401


def test_import_core() -> None:
    import core  # noqa: F401


def test_import_domain() -> None:
    import domain  # noqa: F401


def test_import_market() -> None:
    import market  # noqa: F401


def test_import_state() -> None:
    import state  # noqa: F401


# ------------------------------------------------------------------
# Agent D modules
# ------------------------------------------------------------------


def test_import_storage() -> None:
    import storage  # noqa: F401


def test_import_storage_recorder() -> None:
    from storage.recorder import AsyncRecorder  # noqa: F401


def test_import_storage_database_writer() -> None:
    from storage.database_writer import ThreadedDatabaseWriter  # noqa: F401


def test_import_storage_snapshot_writer() -> None:
    from storage.snapshot_writer import SnapshotWriter  # noqa: F401


def test_import_replay() -> None:
    import replay  # noqa: F401


def test_import_replay_player() -> None:
    from replay.player import ReplayPlayer, TypeRegistry  # noqa: F401


def test_import_replay_registry() -> None:
    from replay.registry import build_default_registry  # noqa: F401


def test_import_replay_runtime_runner() -> None:
    from replay.runtime_runner import ReplayRuntimeRunner  # noqa: F401


def test_import_security() -> None:
    import security  # noqa: F401


def test_import_security_crypto() -> None:
    from security.crypto import CryptoManager  # noqa: F401


def test_import_security_private_key() -> None:
    from security.private_key import resolve_private_key_from_env  # noqa: F401


def test_import_observability() -> None:
    import observability  # noqa: F401


def test_import_observability_metrics() -> None:
    from observability.metrics import InMemoryMetrics  # noqa: F401


def test_import_observability_alerts() -> None:
    from observability.alerts import LoggingAlerts  # noqa: F401
