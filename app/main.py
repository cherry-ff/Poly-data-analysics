from __future__ import annotations

import asyncio
import getpass
import logging
import os
import sys
from argparse import ArgumentParser, Namespace
from pathlib import Path

if __package__ in {None, ""}:
    project_root = Path(__file__).resolve().parents[1]
    project_root_str = str(project_root)
    if project_root_str not in sys.path:
        sys.path.insert(0, project_root_str)

from app.bootstrap import AppBootstrapper
from app.config import RuntimeConfig
from app.env import load_project_env
from app.runtime import AppRuntime
from replay.runtime_runner import ReplayRuntimeRunner
from security.crypto import CryptoManager
from storage.records_layout import migrate_records_layout

logger = logging.getLogger("poly15")


def _configure_logging() -> None:
    raw_level = os.getenv("POLY15_LOG_LEVEL", "INFO").upper()
    level = getattr(logging, raw_level, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        force=True,
    )


def _startup_config_warnings(config: RuntimeConfig) -> list[str]:
    warnings: list[str] = []
    if config.proxy.enabled:
        warnings.append(f"proxy enabled via POLY15_PROXY_URL={config.proxy.url}")
    if config.binance.enabled and not config.binance.ws_url.strip():
        warnings.append("binance feed is enabled but POLY15_BINANCE_WS_URL is empty; the feed will fail immediately")
    if config.chainlink.enabled and not (
        config.chainlink.endpoint.strip()
        or (config.chainlink.api_url.strip() and config.chainlink.feed_id.strip())
    ):
        warnings.append(
            "chainlink feed is enabled but neither POLY15_CHAINLINK_ENDPOINT nor "
            "POLY15_CHAINLINK_API_URL+POLY15_CHAINLINK_FEED_ID is configured"
        )
    if (
        config.polymarket.market_enabled
        and not config.polymarket.market_assets_ids
        and not (
            config.metadata.discovery_enabled
            and config.metadata.gamma_base_url.strip()
        )
    ):
        warnings.append(
            "polymarket market feed is enabled but no asset ids are configured and "
            "gamma discovery is unavailable"
        )
    if config.polymarket.user_enabled and not (
        config.polymarket.api_key.strip()
        and config.polymarket.api_secret.strip()
        and config.polymarket.passphrase.strip()
    ):
        warnings.append("polymarket user feed is enabled but user auth is incomplete; POLY15_PM_API_KEY, POLY15_PM_API_SECRET, and POLY15_PM_PASSPHRASE must all be set")
    return warnings


def _apply_proxy_env(config: RuntimeConfig) -> None:
    if not config.proxy.enabled:
        for key in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY"):
            os.environ.pop(key, None)
        return

    os.environ["HTTP_PROXY"] = config.proxy.url
    os.environ["HTTPS_PROXY"] = config.proxy.url
    os.environ["ALL_PROXY"] = config.proxy.url
    if config.proxy.no_proxy:
        os.environ["NO_PROXY"] = ",".join(config.proxy.no_proxy)
    else:
        os.environ.pop("NO_PROXY", None)


async def run(password: str | None = None) -> None:
    logger.info("bootstrapping live runtime")
    env_path = load_project_env()
    config = RuntimeConfig.from_env(
        decrypt_password=password,
        env_path=env_path,
        strict_encrypted_private_key=True,
    )
    _apply_proxy_env(config)
    logger.info(
        "starting runtime env=%s dry_run=%s binance=%s chainlink=%s pm_market=%s pm_user=%s recorder=%s db=%s snapshot=%s",
        config.env,
        config.execution.gateway_dry_run,
        config.binance.enabled,
        config.chainlink.enabled,
        config.polymarket.market_enabled,
        config.polymarket.user_enabled,
        config.storage.recorder_enabled,
        config.storage.db_enabled,
        config.storage.snapshot_enabled,
    )
    if not any(
        (
            config.binance.enabled,
            config.chainlink.enabled,
            config.polymarket.market_enabled,
            config.polymarket.user_enabled,
        )
    ):
        logger.warning("all external feeds are disabled; runtime will stay idle until feed flags are enabled")
    if env_path:
        logger.info("loaded env file: %s", env_path)
    else:
        logger.warning("no .env file found from current working directory")
    for warning in _startup_config_warnings(config):
        logger.warning(warning)

    bootstrapper = AppBootstrapper(config)
    context = await bootstrapper.start()
    runtime = AppRuntime(context)

    await runtime.start()
    logger.info("runtime started")
    try:
        await asyncio.Event().wait()
    finally:
        logger.info("runtime stopping")
        await runtime.stop()
        await bootstrapper.stop()
        logger.info("runtime stopped")


async def run_replay(paths: list[str], report_path: str | None) -> None:
    report = await ReplayRuntimeRunner().run(paths, report_path=report_path)
    print(report.to_json())


def run_encrypt_secret() -> None:
    _configure_logging()
    manager = CryptoManager()
    print("=== Private Key Encryption Tool ===")
    secret = getpass.getpass("Private key: ")
    password = getpass.getpass("Password: ")
    encrypted = manager.encrypt_secret(secret, password)
    print("\nPut this line into your .env:")
    print(f"POLY15_PM_ENCRYPTED_PRIVATE_KEY={encrypted}")


def run_migrate_records_layout(path: str) -> None:
    _configure_logging()
    report = migrate_records_layout(path)
    logger.info(
        "records layout migration finished root=%s archived_flat_files=%s archived_invalid_market_dirs=%s",
        report.root,
        report.archived_flat_files,
        report.archived_invalid_market_dirs,
    )


def _build_parser() -> ArgumentParser:
    parser = ArgumentParser(prog="poly15")
    subparsers = parser.add_subparsers(dest="command")

    live_parser = subparsers.add_parser("live", help="run live runtime")
    live_parser.add_argument("password", nargs="?", default=None, help="decrypt password")
    replay_parser = subparsers.add_parser("replay", help="replay recorded JSONL files")
    replay_parser.add_argument("paths", nargs="+", help="jsonl files or directories")
    replay_parser.add_argument(
        "--report",
        dest="report_path",
        default=None,
        help="optional path to write replay report json",
    )
    subparsers.add_parser("encrypt-secret", help="interactively encrypt a private key")
    migrate_parser = subparsers.add_parser(
        "migrate-records-layout",
        help="archive legacy flat record files and invalid market ref folders",
    )
    migrate_parser.add_argument(
        "path",
        nargs="?",
        default="runtime_data/records",
        help="records root to migrate",
    )
    return parser


def _parse_args(argv: list[str] | None = None) -> Namespace:
    raw_args = argv or sys.argv[1:]
    if raw_args and raw_args[0] not in {
        "live",
        "replay",
        "encrypt-secret",
        "migrate-records-layout",
        "-h",
        "--help",
    }:
        return Namespace(command="live", password=raw_args[0])

    parser = _build_parser()
    args = parser.parse_args(raw_args)
    if args.command is None:
        args.command = "live"
        args.password = None
    return args


def main(argv: list[str] | None = None) -> None:
    _configure_logging()
    args = _parse_args(argv or sys.argv[1:])
    if args.command == "encrypt-secret":
        run_encrypt_secret()
        return
    if args.command == "replay":
        try:
            asyncio.run(run_replay(args.paths, args.report_path))
        except KeyboardInterrupt:
            return
        return
    if args.command == "migrate-records-layout":
        run_migrate_records_layout(args.path)
        return
    try:
        asyncio.run(run(getattr(args, "password", None)))
    except KeyboardInterrupt:
        return


if __name__ == "__main__":
    main()
