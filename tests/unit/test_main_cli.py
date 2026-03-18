from __future__ import annotations

import os

from app.config import (
    ChainlinkFeedConfig,
    MetadataConfig,
    PolymarketFeedConfig,
    ProxyConfig,
    RuntimeConfig,
)
from app.main import _apply_proxy_env, _parse_args, _startup_config_warnings


def test_parse_args_treats_first_positional_as_live_password() -> None:
    args = _parse_args(["my-password"])
    assert args.command == "live"
    assert args.password == "my-password"


def test_parse_args_live_subcommand_accepts_password() -> None:
    args = _parse_args(["live", "pw2"])
    assert args.command == "live"
    assert args.password == "pw2"


def test_parse_args_replay_keeps_existing_shape() -> None:
    args = _parse_args(["replay", "records", "--report", "report.json"])
    assert args.command == "replay"
    assert args.paths == ["records"]
    assert args.report_path == "report.json"


def test_startup_config_warnings_surface_incomplete_feed_configuration() -> None:
    config = RuntimeConfig(
        proxy=ProxyConfig(enabled=True, url="http://127.0.0.1:7890"),
        chainlink=ChainlinkFeedConfig(enabled=True, endpoint="", api_url="", feed_id=""),
        polymarket=PolymarketFeedConfig(
            market_enabled=True,
            market_assets_ids=(),
            user_enabled=True,
            user_market_ids=(),
            api_key="",
            api_secret="",
            passphrase="",
        ),
        metadata=MetadataConfig(
            gamma_base_url="",
            discovery_enabled=False,
        ),
    )

    warnings = _startup_config_warnings(config)

    assert any("proxy enabled via POLY15_PROXY_URL" in item for item in warnings)
    assert any("POLY15_CHAINLINK_API_URL" in item for item in warnings)
    assert any("gamma discovery is unavailable" in item for item in warnings)
    assert any("POLY15_PM_API_KEY" in item for item in warnings)


def test_apply_proxy_env_sets_and_clears_proxy_variables() -> None:
    enabled_config = RuntimeConfig(
        proxy=ProxyConfig(
            enabled=True,
            url="http://127.0.0.1:7890",
            no_proxy=("127.0.0.1", "localhost"),
        )
    )

    _apply_proxy_env(enabled_config)

    assert os.environ["HTTP_PROXY"] == "http://127.0.0.1:7890"
    assert os.environ["HTTPS_PROXY"] == "http://127.0.0.1:7890"
    assert os.environ["ALL_PROXY"] == "http://127.0.0.1:7890"
    assert os.environ["NO_PROXY"] == "127.0.0.1,localhost"

    _apply_proxy_env(RuntimeConfig(proxy=ProxyConfig(enabled=False)))

    assert "HTTP_PROXY" not in os.environ
    assert "HTTPS_PROXY" not in os.environ
    assert "ALL_PROXY" not in os.environ
