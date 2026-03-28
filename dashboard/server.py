from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import secrets
import shutil
import tarfile
import tempfile
import threading
import time
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse


DASHBOARD_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = DASHBOARD_ROOT.parent
DEFAULT_RECORDS_ROOT = PROJECT_ROOT / "runtime_data" / "records"
BUILD_SCRIPT_PATH = PROJECT_ROOT / "scripts" / "build_dashboard_data.py"
SNAPSHOT_PATH = DASHBOARD_ROOT / "data" / "market-dashboard.json"
SYNC_TOKEN_ENV = "POLY15_SYNC_API_TOKEN"
DEFAULT_SYNC_TOKEN_PATH = PROJECT_ROOT / "runtime_data" / "sync_api_token.txt"
DEFAULT_SYNC_STATE_DIR = PROJECT_ROOT / "runtime_data" / "sync_state"
DEFAULT_SYNC_GC_INDEX_PATH = DEFAULT_SYNC_STATE_DIR / "gc_index.json"
DEFAULT_SYNC_DELETE_GRACE_SECONDS = 6 * 60 * 60
DEFAULT_SYNC_GC_INTERVAL_SECONDS = 5 * 60


def _load_builder_module():
    spec = importlib.util.spec_from_file_location("build_dashboard_data", BUILD_SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load dashboard builder from {BUILD_SCRIPT_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _build_index_payload(payload: dict) -> dict:
    markets = payload.get("markets", {})
    market_order = payload.get("market_order", [])
    global_payload = payload.get("global", {})
    return {
        "generated_at": payload.get("generated_at"),
        "records_root": payload.get("records_root"),
        "market_order": market_order,
        "global": {
            key: {
                "count": value.get("count"),
                "latest": value.get("latest"),
            }
            for key, value in global_payload.items()
            if isinstance(value, dict)
        },
        "markets": {
            market_id: {
                "metadata": market.get("metadata"),
                "window": market.get("window"),
                "summary": market.get("summary"),
            }
            for market_id, market in markets.items()
            if market_id in market_order
        },
    }


class DashboardRequestHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, directory: str | None = None, **kwargs) -> None:
        super().__init__(*args, directory=directory or str(DASHBOARD_ROOT), **kwargs)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/api/sync/manifest":
            if not self._require_sync_auth():
                return
            self._serve_sync_manifest()
            return
        if parsed.path == "/data/dashboard-index.json":
            params = parse_qs(parsed.query)
            try:
                offset = max(0, int(params.get("offset", ["0"])[0]))
                limit = max(1, min(200, int(params.get("limit", ["50"])[0])))
            except (ValueError, IndexError):
                offset, limit = 0, 50
            self._serve_index_json(offset, limit)
            return
        if parsed.path == "/data/global-window.json":
            params = parse_qs(parsed.query)
            market_id = params.get("market_id", [""])[0]
            self._serve_global_window_json(market_id)
            return
        if parsed.path.startswith("/data/markets/") and parsed.path.endswith(".json"):
            market_id = Path(parsed.path).stem
            self._serve_market_json(market_id)
            return
        if parsed.path == "/data/market-dashboard.json":
            self._serve_dashboard_json()
            return
        super().do_GET()

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/api/sync/acks":
            if not self._require_sync_auth():
                return
            self._serve_sync_ack()
            return
        if parsed.path == "/api/sync/archive":
            if not self._require_sync_auth():
                return
            self._serve_sync_archive()
            return
        if parsed.path == "/api/sync/gc":
            if not self._require_sync_auth():
                return
            self._serve_sync_gc()
            return
        self.send_response(HTTPStatus.METHOD_NOT_ALLOWED)
        self.end_headers()

    def do_DELETE(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/api/sync/files":
            if not self._require_sync_auth():
                return
            self._serve_sync_delete()
            return
        self.send_response(HTTPStatus.METHOD_NOT_ALLOWED)
        self.end_headers()

    def end_headers(self) -> None:
        self.send_header("Cache-Control", "no-store, max-age=0")
        super().end_headers()

    def _serve_dashboard_json(self) -> None:
        try:
            body = self.server.dashboard_body()
        except Exception as exc:
            self._serve_error(exc)
            return
        self._serve_json_bytes(body)

    def _serve_index_json(self, offset: int = 0, limit: int = 50) -> None:
        try:
            body = self.server.dashboard_index_body(offset=offset, limit=limit)
        except Exception as exc:
            self._serve_error(exc)
            return
        self._serve_json_bytes(body)

    def _serve_market_json(self, market_id: str) -> None:
        try:
            body = self.server.market_body(market_id)
        except KeyError:
            self.send_response(HTTPStatus.NOT_FOUND)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(
                json.dumps(
                    {"error": "market_not_found", "market_id": market_id},
                    indent=2,
                ).encode("utf-8")
            )
            return
        except Exception as exc:
            self._serve_error(exc)
            return
        self._serve_json_bytes(body)

    def _serve_global_window_json(self, market_id: str) -> None:
        try:
            body = self.server.global_window_body(market_id)
        except KeyError:
            self.send_response(HTTPStatus.NOT_FOUND)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(
                json.dumps(
                    {"error": "market_not_found", "market_id": market_id},
                    indent=2,
                ).encode("utf-8")
            )
            return
        except Exception as exc:
            self._serve_error(exc)
            return
        self._serve_json_bytes(body)

    def _serve_json_bytes(self, body: bytes) -> None:
        self._serve_bytes(
            body=body,
            content_type="application/json; charset=utf-8",
        )

    def _serve_bytes(
        self,
        *,
        body: bytes,
        content_type: str,
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        for key, value in (extra_headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body)

    def _serve_sync_manifest(self) -> None:
        try:
            payload = self.server.sync_manifest()
        except Exception as exc:
            self._serve_error(exc)
            return
        self._serve_json_bytes(json.dumps(payload, indent=2).encode("utf-8"))

    def _serve_sync_ack(self) -> None:
        try:
            payload = self._read_json_body()
            result = self.server.ack_sync_entries(
                payload.get("entries"),
                source=str(payload.get("source") or "").strip(),
                client_receipt_path=str(payload.get("client_receipt_path") or "").strip(),
            )
        except ValueError as exc:
            self._serve_json_error(HTTPStatus.BAD_REQUEST, "invalid_request", str(exc))
            return
        except FileNotFoundError as exc:
            self._serve_json_error(HTTPStatus.NOT_FOUND, "sync_paths_not_found", str(exc))
            return
        except Exception as exc:
            self._serve_error(exc)
            return
        self._serve_json_bytes(json.dumps(result, indent=2).encode("utf-8"))

    def _serve_sync_archive(self) -> None:
        archive_path: Path | None = None
        try:
            payload = self._read_json_body()
            archive_path, filename, archive_size = self.server.create_sync_archive(payload.get("paths"))
        except ValueError as exc:
            self._serve_json_error(HTTPStatus.BAD_REQUEST, "invalid_request", str(exc))
            return
        except FileNotFoundError as exc:
            self._serve_json_error(HTTPStatus.NOT_FOUND, "sync_paths_not_found", str(exc))
            return
        except Exception as exc:
            self._serve_error(exc)
            return
        try:
            self._serve_file(
                path=archive_path,
                content_type="application/gzip",
                extra_headers={
                    "Content-Disposition": f'attachment; filename="{filename}"',
                    "Content-Length": str(archive_size),
                },
            )
        finally:
            if archive_path is not None:
                archive_path.unlink(missing_ok=True)

    def _serve_file(
        self,
        *,
        path: Path,
        content_type: str,
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        for key, value in (extra_headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        with path.open("rb") as handle:
            shutil.copyfileobj(handle, self.wfile, length=1024 * 1024)

    def _serve_sync_gc(self) -> None:
        try:
            payload = self._read_json_body()
            result = self.server.collect_pending_sync_garbage(
                force=bool(payload.get("force", False)),
            )
        except ValueError as exc:
            self._serve_json_error(HTTPStatus.BAD_REQUEST, "invalid_request", str(exc))
            return
        except Exception as exc:
            self._serve_error(exc)
            return
        self._serve_json_bytes(json.dumps(result, indent=2).encode("utf-8"))

    def _serve_sync_delete(self) -> None:
        try:
            payload = self._read_json_body()
            result = self.server.delete_sync_paths(
                payload.get("paths"),
                payload.get("entries"),
            )
        except ValueError as exc:
            self._serve_json_error(HTTPStatus.BAD_REQUEST, "invalid_request", str(exc))
            return
        except FileNotFoundError as exc:
            self._serve_json_error(HTTPStatus.NOT_FOUND, "sync_paths_not_found", str(exc))
            return
        except Exception as exc:
            self._serve_error(exc)
            return
        self._serve_json_bytes(json.dumps(result, indent=2).encode("utf-8"))

    def _read_json_body(self) -> dict:
        raw_length = self.headers.get("Content-Length", "0")
        try:
            length = int(raw_length)
        except ValueError as exc:
            raise ValueError("Content-Length must be an integer") from exc
        if length <= 0:
            return {}
        body = self.rfile.read(length)
        if not body:
            return {}
        try:
            payload = json.loads(body.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError("request body must be valid JSON") from exc
        if not isinstance(payload, dict):
            raise ValueError("request body must be a JSON object")
        return payload

    def _require_sync_auth(self) -> bool:
        expected_token = getattr(self.server, "sync_token", "")
        if not expected_token:
            self._serve_json_error(
                HTTPStatus.SERVICE_UNAVAILABLE,
                "sync_auth_unavailable",
                "sync API token is not configured",
            )
            return False
        candidate = self._extract_bearer_token()
        if candidate and secrets.compare_digest(candidate, expected_token):
            return True
        self.send_response(HTTPStatus.UNAUTHORIZED)
        self.send_header("WWW-Authenticate", 'Bearer realm="poly15-sync"')
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.end_headers()
        self.wfile.write(
            json.dumps(
                {
                    "error": "unauthorized",
                    "detail": "provide Authorization: Bearer <token>",
                },
                indent=2,
            ).encode("utf-8")
        )
        return False

    def _extract_bearer_token(self) -> str:
        authorization = self.headers.get("Authorization", "")
        if authorization.lower().startswith("bearer "):
            return authorization[7:].strip()
        return self.headers.get("X-Sync-Token", "").strip()

    def _serve_json_error(
        self,
        status: HTTPStatus,
        error: str,
        detail: str,
    ) -> None:
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.end_headers()
        self.wfile.write(
            json.dumps({"error": error, "detail": detail}, indent=2).encode("utf-8")
        )

    def _serve_error(self, exc: Exception) -> None:
        self.send_response(HTTPStatus.INTERNAL_SERVER_ERROR)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.end_headers()
        error_payload = {
            "error": "dashboard_data_build_failed",
            "detail": str(exc),
        }
        self.wfile.write(json.dumps(error_payload, indent=2).encode("utf-8"))


class DashboardHTTPServer(ThreadingHTTPServer):
    def __init__(
        self,
        server_address: tuple[str, int],
        handler_cls,
        records_root: Path,
        *,
        sync_token: str | None = None,
        sync_token_path: Path | None = None,
        sync_state_dir: Path | None = None,
        sync_delete_grace_seconds: int = DEFAULT_SYNC_DELETE_GRACE_SECONDS,
        sync_gc_interval_seconds: int = DEFAULT_SYNC_GC_INTERVAL_SECONDS,
    ) -> None:
        super().__init__(server_address, handler_cls)
        self.records_root = records_root
        self.snapshot_path = SNAPSHOT_PATH
        self.sync_token_path = (sync_token_path or DEFAULT_SYNC_TOKEN_PATH).resolve()
        self.sync_token = _load_or_create_sync_token(sync_token, self.sync_token_path)
        self.sync_state_dir = (sync_state_dir or DEFAULT_SYNC_STATE_DIR).resolve()
        self.sync_delete_grace_seconds = max(int(sync_delete_grace_seconds), 0)
        self.sync_gc_interval_seconds = max(int(sync_gc_interval_seconds), 0)
        self._sync_gc_state_lock = threading.Lock()
        self._sync_gc_stop_event = threading.Event()
        self._sync_gc_thread: threading.Thread | None = None
        # sha256 cache: relative_path -> (size_bytes, mtime_ns, hex_digest)
        # Sealed files are immutable so entries never need invalidation,
        # only removal when GC deletes the file.
        self._sha256_cache: dict[str, tuple[int, int, str]] = {}
        self._sha256_cache_lock = threading.Lock()
        self._cache_lock = threading.Lock()
        self._live_signature: tuple[int, int] | None = None
        self._cache_payload: dict | None = None
        self._cache_body: bytes | None = None
        self._index_body: bytes | None = None
        self._stale_index_body: bytes | None = None
        self._index_building = False
        self._market_payloads: dict[str, dict] = {}
        self._market_bodies: dict[str, bytes] = {}
        self._global_window_bodies: dict[str, bytes] = {}
        self._live_global_files: dict[str, list[Path]] | None = None
        self._live_market_files: dict[str, dict[str, list[Path]]] | None = None
        self._stream_file_ranges: dict[str, tuple[tuple[str, ...], list[tuple[Path, int | None, int | None]]]] = {}
        self._cache_signature: tuple[int, int] | None = None
        self._build_error: str | None = None
        self._building = False
        # Signature TTL cache: avoids rglob on every request
        self._sig_cache_val: tuple[int, int] | None = None
        self._sig_cache_ts: float = 0.0
        self._sig_cache_ttl: float = 2.0
        self._load_snapshot()
        self.warm_cache()
        self._start_sync_gc_thread()

    def dashboard_payload(self) -> dict:
        current_signature = self._records_signature()
        with self._cache_lock:
            if self._cache_payload is not None:
                if self._cache_signature != current_signature and not self._building:
                    self._start_background_build(current_signature)
                return self._cache_payload

            should_build_sync = not self._building
            if should_build_sync:
                self._building = True

        if should_build_sync:
            self._rebuild_cache(current_signature)

        with self._cache_lock:
            if self._cache_payload is not None:
                return self._cache_payload
            detail = self._build_error or "dashboard cache unavailable"
            raise RuntimeError(detail)

    def dashboard_body(self) -> bytes:
        with self._cache_lock:
            if self._cache_body is not None:
                return self._cache_body
        self.dashboard_payload()
        with self._cache_lock:
            if self._cache_body is not None:
                return self._cache_body
            raise RuntimeError(self._build_error or "dashboard cache unavailable")

    def dashboard_index_body(self, offset: int = 0, limit: int = 50) -> bytes:
        current_signature = self._records_signature()
        with self._cache_lock:
            self._ensure_live_caches_current(current_signature)
            if offset == 0:
                if self._index_body is not None:
                    return self._index_body
                stale = self._stale_index_body
                if stale is not None and not self._index_building:
                    self._index_building = True
                    threading.Thread(
                        target=self._rebuild_index_body,
                        daemon=True,
                        name="dashboard-index-builder",
                    ).start()
                    return stale
                if stale is not None:
                    return stale

        index_payload = self._build_live_index_payload(offset=offset, limit=limit)
        index_body = json.dumps(index_payload, indent=2).encode("utf-8")
        if offset == 0:
            with self._cache_lock:
                self._index_body = index_body
                self._stale_index_body = index_body
                self._index_building = False
        return index_body

    def _rebuild_index_body(self) -> None:
        try:
            index_payload = self._build_live_index_payload()
            index_body = json.dumps(index_payload, indent=2).encode("utf-8")
        except Exception:
            return
        finally:
            with self._cache_lock:
                self._index_building = False
        with self._cache_lock:
            self._index_body = index_body
            self._stale_index_body = index_body

    def market_body(self, market_id: str) -> bytes:
        current_signature = self._records_signature()
        with self._cache_lock:
            self._ensure_live_caches_current(current_signature)
            cached = self._market_bodies.get(market_id)
            if cached is not None:
                return cached

        market = self._get_live_market_payload(market_id)
        body = json.dumps({"market_id": market_id, "market": market}, indent=2).encode("utf-8")
        with self._cache_lock:
            self._market_payloads[market_id] = market
            self._market_bodies[market_id] = body
        return body

    def global_window_body(self, market_id: str) -> bytes:
        current_signature = self._records_signature()
        with self._cache_lock:
            self._ensure_live_caches_current(current_signature)
            cached = self._global_window_bodies.get(market_id)
            if cached is not None:
                return cached

        market = self._get_live_market_payload(market_id)

        window = market.get("window", {})
        start_ts = window.get("start_ts")
        end_ts = window.get("end_ts")
        if isinstance(start_ts, int) and isinstance(end_ts, int) and end_ts > start_ts:
            builder, global_files, _ = self._load_builder_and_files(current_signature)
            binance_paths = self._select_stream_paths_for_window(
                cache_key="feeds.binance.tick",
                paths=global_files.get("feeds.binance.tick"),
                start_ts=start_ts,
                end_ts=end_ts,
                ts_getter=lambda record: self._binance_record_ts(builder, record),
            )
            chainlink_paths = self._select_stream_paths_for_window(
                cache_key="feeds.chainlink.tick",
                paths=global_files.get("feeds.chainlink.tick"),
                start_ts=start_ts,
                end_ts=end_ts,
                ts_getter=lambda record: self._chainlink_record_ts(builder, record),
            )
            binance_raw = self._load_binance_window(
                builder,
                binance_paths,
                start_ts,
                end_ts,
            )
            chainlink_raw = self._load_chainlink_window(
                builder,
                chainlink_paths,
                start_ts,
                end_ts,
            )
            basis_raw = builder._align_basis_series(binance_raw, chainlink_raw)
            global_payload = {
                "binance": {"series": builder._downsample(binance_raw, builder.GLOBAL_MAX_POINTS)},
                "chainlink": {"series": builder._downsample(chainlink_raw, builder.GLOBAL_MAX_POINTS)},
                "basis": {"series": builder._downsample(basis_raw, builder.GLOBAL_MAX_POINTS)},
            }
        else:
            global_payload = {
                "binance": {"series": []},
                "chainlink": {"series": []},
                "basis": {"series": []},
            }
        body = json.dumps(
            {
                "market_id": market_id,
                "window": window,
                "global": {
                    key: self._slice_global_section(value, start_ts, end_ts)
                    for key, value in global_payload.items()
                    if isinstance(value, dict)
                },
            },
            indent=2,
        ).encode("utf-8")
        with self._cache_lock:
            self._global_window_bodies[market_id] = body
        return body

    def warm_cache(self) -> None:
        with self._cache_lock:
            self._index_building = True
        threading.Thread(
            target=self._rebuild_index_body,
            daemon=True,
            name="dashboard-index-warmup",
        ).start()

    def server_close(self) -> None:
        self._sync_gc_stop_event.set()
        if self._sync_gc_thread is not None and self._sync_gc_thread.is_alive():
            self._sync_gc_thread.join(timeout=1.0)
        super().server_close()

    def _start_sync_gc_thread(self) -> None:
        if self.sync_gc_interval_seconds <= 0:
            return
        thread = threading.Thread(
            target=self._run_sync_gc_loop,
            daemon=True,
            name="dashboard-sync-gc",
        )
        self._sync_gc_thread = thread
        thread.start()

    def _run_sync_gc_loop(self) -> None:
        while not self._sync_gc_stop_event.wait(self.sync_gc_interval_seconds):
            try:
                self.collect_pending_sync_garbage()
            except Exception:
                continue

    def _load_snapshot(self) -> None:
        if not self.snapshot_path.exists():
            return
        self._cache_body = self.snapshot_path.read_bytes()
        try:
            self._cache_payload = json.loads(self._cache_body.decode("utf-8"))
        except Exception:
            self._cache_payload = None

    def _start_background_build(self, signature: tuple[int, int]) -> None:
        with self._cache_lock:
            if self._building:
                return
            self._building = True
        thread = threading.Thread(
            target=self._rebuild_cache,
            args=(signature,),
            daemon=True,
            name="dashboard-cache-builder",
        )
        thread.start()

    def _rebuild_cache(self, signature: tuple[int, int]) -> None:
        try:
            builder = _load_builder_module()
            payload = builder.build_dashboard_payload(self.records_root)
            body = json.dumps(payload, indent=2).encode("utf-8")
            self.snapshot_path.parent.mkdir(parents=True, exist_ok=True)
            self.snapshot_path.write_bytes(body)
        except Exception as exc:
            with self._cache_lock:
                self._build_error = str(exc)
                self._building = False
            return

        with self._cache_lock:
            self._cache_payload = payload
            self._cache_body = body
            self._index_body = None
            self._market_bodies = {}
            self._global_window_bodies = {}
            self._cache_signature = signature
            self._build_error = None
            self._building = False

    def _records_signature(self) -> tuple[int, int]:
        now = time.monotonic()
        if self._sig_cache_val is not None and (now - self._sig_cache_ts) < self._sig_cache_ttl:
            return self._sig_cache_val
        file_count = 0
        latest_mtime_ns = 0
        for path in self.records_root.rglob("*.jsonl"):
            if not path.is_file():
                continue
            stat = path.stat()
            file_count += 1
            latest_mtime_ns = max(latest_mtime_ns, stat.st_mtime_ns)
        sig = (file_count, latest_mtime_ns)
        self._sig_cache_val = sig
        self._sig_cache_ts = now
        return sig

    @staticmethod
    def _slice_global_section(section: dict, start_ts: int | None, end_ts: int | None) -> dict:
        series = section.get("series", [])
        if isinstance(start_ts, int) and isinstance(end_ts, int):
            series = [
                point for point in series
                if isinstance(point, dict)
                and isinstance(point.get("ts"), int)
                and start_ts <= point["ts"] <= end_ts
            ]
        return {
            "count": len(series),
            "latest": series[-1] if series else None,
            "series": series,
        }

    def _ensure_live_caches_current(self, current_signature: tuple[int, int]) -> None:
        if self._live_signature == current_signature:
            return
        self._live_signature = current_signature
        if self._index_body is not None:
            self._stale_index_body = self._index_body
        self._index_body = None
        self._market_payloads = {}
        self._market_bodies = {}
        self._global_window_bodies = {}
        self._live_global_files = None
        self._live_market_files = None
        self._stream_file_ranges = {}

    def _load_builder_and_files(
        self,
        current_signature: tuple[int, int] | None = None,
    ):
        if current_signature is None:
            current_signature = self._records_signature()
        with self._cache_lock:
            self._ensure_live_caches_current(current_signature)
            cached_global_files = self._live_global_files
            cached_market_files = self._live_market_files
        if cached_global_files is not None and cached_market_files is not None:
            return _load_builder_module(), cached_global_files, cached_market_files

        builder = _load_builder_module()
        global_files, market_files = builder._discover_record_files(self.records_root)
        with self._cache_lock:
            if self._live_signature == current_signature:
                self._live_global_files = global_files
                self._live_market_files = market_files
        return builder, global_files, market_files

    def _build_live_index_payload(self, offset: int = 0, limit: int = 50) -> dict:
        builder, _, market_files = self._load_builder_and_files()
        # Sort by market_id numeric descending — no file reads required
        all_ids = sorted(
            market_files.keys(),
            key=lambda mid: builder._safe_int(mid) or 0,
            reverse=True,
        )
        total = len(all_ids)
        page_ids = all_ids[offset:offset + limit]
        markets: dict[str, dict] = {}
        market_order: list[str] = []
        for market_id in page_ids:
            files = market_files[market_id]
            market = self._build_live_index_market(builder, market_id, files)
            if market is None:
                continue
            markets[market_id] = {
                "metadata": market.get("metadata"),
                "window": market.get("window"),
                "summary": market.get("summary"),
            }
            market_order.append(market_id)
        return {
            "generated_at": self._cache_payload.get("generated_at") if self._cache_payload else None,
            "records_root": str(self.records_root),
            "market_order": market_order,
            "total": total,
            "offset": offset,
            "limit": limit,
            "has_more": (offset + limit) < total,
            "global": self._cached_global_summary(),
            "markets": markets,
        }

    @classmethod
    def _read_last_market_metadata(cls, builder, paths) -> dict | None:
        record = cls._read_last_jsonl_record(paths)
        if record is None:
            return None
        market = record.get("payload", {}).get("market")
        if not isinstance(market, dict):
            return None
        ref_price = builder._safe_float(market.get("reference_price"))
        if ref_price is not None and not (10_000 <= abs(ref_price) <= 1_000_000):
            ref_price = None
        return {
            "market_id": str(market.get("market_id") or ""),
            "condition_id": str(market.get("condition_id") or ""),
            "up_token_id": str(market.get("up_token_id") or ""),
            "down_token_id": str(market.get("down_token_id") or ""),
            "start_ts_ms": builder._safe_int(market.get("start_ts_ms")),
            "end_ts_ms": builder._safe_int(market.get("end_ts_ms")),
            "tick_size": builder._safe_float(market.get("tick_size")),
            "fee_rate_bps": builder._safe_float(market.get("fee_rate_bps")),
            "min_order_size": builder._safe_float(market.get("min_order_size")),
            "status": str(market.get("status") or ""),
            "reference_price": ref_price,
            "raw_reference_price": builder._safe_float(market.get("reference_price")),
        }

    def _build_live_index_market(self, builder, market_id: str, files: dict[str, list[Path]]) -> dict | None:
        metadata = self._read_last_market_metadata(builder, files.get("market.metadata"))
        if metadata is None:
            return None

        lifecycle_record = self._read_last_jsonl_record(files.get("market.lifecycle.transition"))
        theo_record = self._read_last_jsonl_record(files.get("pricing.theo"))
        quote_record = self._read_last_jsonl_record(files.get("pricing.quote_plan"))
        depth_record = self._read_last_jsonl_record(files.get("feeds.polymarket.market.depth"))

        latest_phase = None
        lifecycle_payload = lifecycle_record.get("payload", {}).get("transition") if lifecycle_record else None
        if isinstance(lifecycle_payload, dict):
            latest_phase = str(lifecycle_payload.get("new_phase") or "") or None

        latest_theo = None
        theo_payload = theo_record.get("payload", {}).get("snapshot") if theo_record else None
        if isinstance(theo_payload, dict):
            latest_theo = {
                "ts": builder._safe_int(theo_payload.get("ts_ms")),
                "theo_up": builder._safe_float(theo_payload.get("theo_up")),
                "theo_down": builder._safe_float(theo_payload.get("theo_down")),
                "sigma_short": builder._safe_float(theo_payload.get("sigma_short")),
                "target_full_set_cost": builder._safe_float(theo_payload.get("target_full_set_cost")),
            }

        latest_quote = None
        quote_payload = quote_record.get("payload", {}).get("plan") if quote_record else None
        if isinstance(quote_payload, dict):
            up_bid = builder._safe_float(quote_payload.get("up_bid_px"))
            down_bid = builder._safe_float(quote_payload.get("down_bid_px"))
            up_ask = builder._safe_float(quote_payload.get("up_ask_px"))
            down_ask = builder._safe_float(quote_payload.get("down_ask_px"))
            latest_quote = {
                "ts": builder._safe_int(quote_payload.get("ts_ms")),
                "quote_bid_sum": builder._sum_nullable(up_bid, down_bid),
                "quote_ask_sum": builder._sum_nullable(up_ask, down_ask),
            }

        latest_ts = max(
            value
            for value in [
                latest_theo["ts"] if latest_theo else 0,
                latest_quote["ts"] if latest_quote else 0,
                builder._safe_int(lifecycle_payload.get("ts_ms")) if isinstance(lifecycle_payload, dict) else 0,
                builder._safe_int(depth_record.get("recv_ts_ms")) if isinstance(depth_record, dict) else 0,
            ]
            if isinstance(value, int)
        )
        market_window = builder._build_market_window(metadata, latest_ts or None)
        summary = {
            "latest_phase": latest_phase or metadata.get("status") or None,
            "progress_pct": builder._progress_pct(market_window),
            "duration_minutes": market_window.get("duration_minutes"),
            "observed_end_ts": latest_ts or None,
            "latest_binance_mid": None,
            "latest_chainlink_price": None,
            "latest_basis": None,
            "latest_theo_up": latest_theo["theo_up"] if latest_theo else None,
            "latest_theo_down": latest_theo["theo_down"] if latest_theo else None,
            "latest_sigma_short": latest_theo["sigma_short"] if latest_theo else None,
            "latest_target_full_set_cost": latest_theo["target_full_set_cost"] if latest_theo else None,
            "latest_sum_best_bid": None,
            "latest_sum_best_ask": None,
            "latest_quote_bid_sum": latest_quote["quote_bid_sum"] if latest_quote else None,
            "latest_quote_ask_sum": latest_quote["quote_ask_sum"] if latest_quote else None,
            "latest_quote_vs_target": builder._subtract_nullable(
                latest_theo["target_full_set_cost"] if latest_theo else None,
                latest_quote["quote_bid_sum"] if latest_quote else None,
            ),
            "latest_market_edge": None,
            "event_counts": {
                "lifecycle": 1 if lifecycle_record else 0,
                "theo": 1 if latest_theo else 0,
                "quote": 1 if latest_quote else 0,
                "pair_book": 1 if files.get("feeds.polymarket.market.book_top") else 0,
                "depth": 1 if depth_record else 0,
            },
        }
        has_market_data = any(
            summary["event_counts"][key] > 0
            for key in ("theo", "quote", "pair_book", "depth")
        )
        if not has_market_data:
            return None
        market = {
            "metadata": metadata,
            "window": market_window,
            "summary": summary,
        }
        if not builder._market_has_dashboard_data(market):
            return None
        return market

    def _cached_global_summary(self) -> dict:
        global_payload = self._cache_payload.get("global", {}) if self._cache_payload else {}
        if not isinstance(global_payload, dict):
            global_payload = {}
        return {
            key: {
                "count": value.get("count"),
                "latest": value.get("latest"),
            }
            for key, value in global_payload.items()
            if isinstance(value, dict)
        }

    def _get_live_market_payload(self, market_id: str) -> dict:
        with self._cache_lock:
            cached = self._market_payloads.get(market_id)
            if cached is not None:
                return cached

        builder, _, market_files = self._load_builder_and_files(self._records_signature())
        files = market_files.get(market_id)
        if files is None:
            raise KeyError(market_id)
        metadata = builder._load_market_metadata(files.get("market.metadata"), market_id)
        market = builder._build_market_payload(
            market_id=market_id,
            files=files,
            metadata=metadata,
            binance_raw=[],
            chainlink_raw=[],
            market_max_points=None,
        )
        if not builder._market_has_dashboard_data(market):
            raise KeyError(market_id)
        with self._cache_lock:
            self._market_payloads[market_id] = market
        return market

    def sync_manifest(self) -> dict:
        sync_root = self._sync_root()
        entries = []
        total_bytes = 0
        for path in self._iter_syncable_files():
            entry = self._build_sync_manifest_entry(path, sync_root)
            total_bytes += entry["size_bytes"]
            entries.append(entry)
        return {
            "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "records_root": str(self.records_root),
            "sync_root": str(sync_root),
            "token_path": str(self.sync_token_path),
            "sync_state_path": str(self._sync_gc_index_path()),
            "delete_grace_seconds": self._sync_delete_grace_seconds_value(),
            "pending_gc_file_count": self._pending_sync_gc_count(),
            "file_count": len(entries),
            "total_bytes": total_bytes,
            "entries": entries,
        }

    def sync_archive(self, requested_paths: object) -> tuple[bytes, str]:
        archive_path, filename, _ = self.create_sync_archive(requested_paths)
        try:
            return archive_path.read_bytes(), filename
        finally:
            archive_path.unlink(missing_ok=True)

    def create_sync_archive(self, requested_paths: object) -> tuple[Path, str, int]:
        sync_root = self._sync_root()
        files = self._expand_sync_paths(
            requested_paths,
            base_root=sync_root,
            allow_empty_as_all=True,
            allow_root_path=True,
        )
        if not files:
            raise FileNotFoundError("no syncable jsonl files matched the requested paths")
        with tempfile.NamedTemporaryFile(
            prefix="poly15-sync-",
            suffix=".tar.gz",
            delete=False,
        ) as handle:
            archive_path = Path(handle.name)
        try:
            with tarfile.open(archive_path, mode="w:gz") as archive:
                for path in files:
                    archive.add(path, arcname=path.relative_to(sync_root).as_posix())
        except Exception:
            archive_path.unlink(missing_ok=True)
            raise
        filename = (
            "poly15-records-"
            f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.tar.gz"
        )
        return archive_path, filename, archive_path.stat().st_size

    def ack_sync_entries(
        self,
        expected_entries: object | None,
        *,
        source: str = "",
        client_receipt_path: str = "",
    ) -> dict:
        expected_by_path = self._normalize_expected_entries(expected_entries)
        if not expected_by_path:
            raise ValueError("entries must be a non-empty JSON array")

        acked_at = datetime.now(timezone.utc)
        delete_after = acked_at + timedelta(seconds=self._sync_delete_grace_seconds_value())
        acked_records: list[dict[str, Any]] = []
        lock = self._sync_gc_lock()
        with lock:
            state = self._load_sync_gc_state_locked()
            entries = state["entries"]
            for relative_path, expected_entry in expected_by_path.items():
                path = self._resolve_sync_file(relative_path)
                self._validate_expected_sync_entry(path, relative_path, expected_entry)
                manifest_entry = self._build_sync_manifest_entry(path, self._sync_root())
                ack_record = {
                    **manifest_entry,
                    "acked_at": self._format_utc_ts(acked_at),
                    "delete_after": self._format_utc_ts(delete_after),
                    "source": source,
                    "client_receipt_path": client_receipt_path,
                    "status": "pending",
                    "deleted_at": None,
                    "last_error": None,
                }
                entries[relative_path] = ack_record
                acked_records.append(ack_record)
            state["updated_at"] = self._format_utc_ts(acked_at)
            self._save_sync_gc_state_locked(state)
        return {
            "acked_file_count": len(acked_records),
            "acked_files": [record["path"] for record in acked_records],
            "delete_grace_seconds": self._sync_delete_grace_seconds_value(),
            "delete_after_min": self._format_utc_ts(delete_after),
            "delete_after_max": self._format_utc_ts(delete_after),
            "state_path": str(self._sync_gc_index_path()),
        }

    def collect_pending_sync_garbage(self, *, force: bool = False) -> dict:
        now = datetime.now(timezone.utc)
        now_iso = self._format_utc_ts(now)
        lock = self._sync_gc_lock()
        deleted_paths: list[Path] = []
        deleted_files: list[str] = []
        stale_files: list[str] = []
        missing_files: list[str] = []
        freed_bytes = 0

        with lock:
            state = self._load_sync_gc_state_locked()
            entries = state["entries"]
            for relative_path, entry in entries.items():
                status = str(entry.get("status") or "pending")
                if status == "deleted":
                    continue

                delete_after_raw = str(entry.get("delete_after") or "").strip()
                if not force and delete_after_raw:
                    if self._parse_utc_ts(delete_after_raw) > now:
                        continue

                try:
                    path = self._resolve_sync_file(relative_path)
                except FileNotFoundError:
                    entry["status"] = "deleted"
                    entry["deleted_at"] = now_iso
                    entry["last_error"] = "sync file missing during gc"
                    missing_files.append(relative_path)
                    continue

                try:
                    self._validate_expected_sync_entry(path, relative_path, entry)
                except ValueError as exc:
                    entry["status"] = "stale"
                    entry["last_error"] = str(exc)
                    stale_files.append(relative_path)
                    continue

                stat = path.stat()
                path.unlink()
                freed_bytes += stat.st_size
                deleted_paths.append(path)
                deleted_files.append(relative_path)
                entry["status"] = "deleted"
                entry["deleted_at"] = now_iso
                entry["last_error"] = None

            state["updated_at"] = now_iso
            self._save_sync_gc_state_locked(state)

        deleted_dirs: list[str] = []
        if deleted_paths:
            deleted_dirs = self._prune_empty_record_dirs(deleted_paths, self._sync_root())
            self._refresh_after_records_mutation()

        pending_count = self._pending_sync_gc_count()
        return {
            "forced": force,
            "deleted_file_count": len(deleted_files),
            "deleted_dir_count": len(deleted_dirs),
            "stale_file_count": len(stale_files),
            "missing_file_count": len(missing_files),
            "pending_file_count": pending_count,
            "freed_bytes": freed_bytes,
            "deleted_files": deleted_files,
            "deleted_dirs": deleted_dirs,
            "stale_files": stale_files,
            "missing_files": missing_files,
            "state_path": str(self._sync_gc_index_path()),
        }

    def delete_sync_paths(
        self,
        requested_paths: object,
        expected_entries: object | None = None,
    ) -> dict:
        sync_root = self._sync_root()
        expected_by_path = self._normalize_expected_entries(expected_entries)
        if requested_paths in (None, []) and expected_by_path:
            requested_paths = list(expected_by_path)
        files = self._expand_sync_paths(
            requested_paths,
            base_root=sync_root,
            allow_empty_as_all=False,
            allow_root_path=False,
        )
        if not files:
            raise FileNotFoundError("no deletable jsonl files matched the requested paths")
        freed_bytes = 0
        deleted_files: list[str] = []
        for path in files:
            if not path.exists():
                continue
            relative_path = path.relative_to(sync_root).as_posix()
            expected_entry = expected_by_path.get(relative_path)
            if expected_by_path and expected_entry is None:
                raise ValueError(f"missing expected manifest entry for delete path: {relative_path}")
            if expected_entry is not None:
                self._validate_expected_sync_entry(path, relative_path, expected_entry)
            stat = path.stat()
            path.unlink()
            freed_bytes += stat.st_size
            deleted_files.append(relative_path)
        deleted_dirs = self._prune_empty_record_dirs(files, sync_root)
        self._refresh_after_records_mutation()
        return {
            "deleted_files": deleted_files,
            "deleted_dirs": deleted_dirs,
            "deleted_file_count": len(deleted_files),
            "deleted_dir_count": len(deleted_dirs),
            "freed_bytes": freed_bytes,
        }

    def _expand_sync_paths(
        self,
        requested_paths: object,
        *,
        base_root: Path,
        allow_empty_as_all: bool,
        allow_root_path: bool,
    ) -> list[Path]:
        base_root = base_root.resolve()
        raw_paths: list[str]
        if requested_paths in (None, []):
            if not allow_empty_as_all:
                raise ValueError("paths must be a non-empty JSON array")
            raw_paths = ["."]
        elif not isinstance(requested_paths, list):
            raise ValueError("paths must be a JSON array of relative paths")
        else:
            raw_paths = [str(item).strip() for item in requested_paths]
        expanded: list[Path] = []
        seen: set[Path] = set()
        for raw_path in raw_paths:
            if raw_path in {"", "."}:
                if not allow_root_path:
                    raise ValueError("deleting the entire records root is not allowed")
                target = base_root
            else:
                relative = Path(raw_path)
                if relative.is_absolute() or ".." in relative.parts:
                    raise ValueError(f"invalid sync path: {raw_path}")
                target = (base_root / relative).resolve()
                if base_root not in target.parents and target != base_root:
                    raise ValueError(f"sync path escapes records root: {raw_path}")
            if not target.exists():
                raise FileNotFoundError(f"sync path not found: {raw_path}")
            if target.is_file():
                if target.suffix != ".jsonl":
                    raise ValueError(f"only .jsonl files are syncable: {raw_path}")
                if target not in seen:
                    seen.add(target)
                    expanded.append(target)
                continue
            matched = sorted(path for path in target.rglob("*.jsonl") if path.is_file())
            if not matched:
                raise FileNotFoundError(f"sync path contains no .jsonl files: {raw_path}")
            for path in matched:
                if path not in seen:
                    seen.add(path)
                    expanded.append(path)
        return expanded

    def _prune_empty_record_dirs(self, files: list[Path], root_path: Path) -> list[str]:
        deleted_dirs: list[str] = []
        seen: set[Path] = set()
        for path in files:
            parent = path.parent
            while parent != root_path and parent not in seen:
                if not parent.exists():
                    parent = parent.parent
                    continue
                try:
                    parent.rmdir()
                except OSError:
                    break
                seen.add(parent)
                deleted_dirs.append(parent.relative_to(root_path).as_posix())
                parent = parent.parent
        deleted_dirs.sort()
        return deleted_dirs

    def _sync_root(self) -> Path:
        return (self.records_root / "sealed").resolve()

    def _sync_state_dir_path(self) -> Path:
        raw = getattr(self, "sync_state_dir", None)
        if raw is None:
            return (self.records_root.parent / "sync_state").resolve()
        return Path(raw).resolve()

    def _sync_gc_index_path(self) -> Path:
        return (self._sync_state_dir_path() / DEFAULT_SYNC_GC_INDEX_PATH.name).resolve()

    def _sync_delete_grace_seconds_value(self) -> int:
        return max(int(getattr(self, "sync_delete_grace_seconds", DEFAULT_SYNC_DELETE_GRACE_SECONDS)), 0)

    def _sync_gc_lock(self) -> threading.Lock:
        lock = getattr(self, "_sync_gc_state_lock", None)
        if lock is None:
            lock = threading.Lock()
            self._sync_gc_state_lock = lock
        return lock

    def _load_sync_gc_state_locked(self) -> dict[str, Any]:
        path = self._sync_gc_index_path()
        if not path.exists():
            return {"updated_at": "", "entries": {}}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"invalid sync gc state JSON: {path}") from exc
        if not isinstance(payload, dict):
            raise RuntimeError(f"sync gc state root must be a JSON object: {path}")
        entries = payload.get("entries", {})
        if not isinstance(entries, dict):
            raise RuntimeError(f"sync gc state entries must be a JSON object: {path}")
        normalized_entries: dict[str, dict[str, Any]] = {}
        for key, value in entries.items():
            if isinstance(key, str) and isinstance(value, dict):
                normalized_entries[key] = value
        return {
            "updated_at": str(payload.get("updated_at") or ""),
            "entries": normalized_entries,
        }

    def _save_sync_gc_state_locked(self, state: dict[str, Any]) -> None:
        path = self._sync_gc_index_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(state, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")

    def _pending_sync_gc_count(self) -> int:
        lock = self._sync_gc_lock()
        with lock:
            state = self._load_sync_gc_state_locked()
            return sum(
                1
                for entry in state["entries"].values()
                if str(entry.get("status") or "pending") == "pending"
            )

    def _resolve_sync_file(self, relative_path: str) -> Path:
        if not relative_path.endswith(".jsonl"):
            raise ValueError(f"sync path must reference a .jsonl file: {relative_path}")
        sync_root = self._sync_root()
        candidate = Path(relative_path)
        if relative_path in {"", "."} or candidate.is_absolute() or ".." in candidate.parts:
            raise ValueError(f"invalid sync path: {relative_path}")
        path = (sync_root / candidate).resolve()
        if sync_root not in path.parents:
            raise ValueError(f"sync path escapes records root: {relative_path}")
        if not path.exists():
            raise FileNotFoundError(f"sync path not found: {relative_path}")
        if not path.is_file():
            raise ValueError(f"sync path must reference an existing file: {relative_path}")
        return path

    def _iter_syncable_files(self) -> list[Path]:
        sync_root = self._sync_root()
        if not sync_root.exists():
            return []
        return sorted(path for path in sync_root.rglob("*.jsonl") if path.is_file())

    def _build_sync_manifest_entry(self, path: Path, sync_root: Path) -> dict[str, object]:
        stat = path.stat()
        relative_path = path.relative_to(sync_root).as_posix()
        stream_key, scope, market_id, segment_seq = self._parse_sync_segment_path(relative_path)
        return {
            "path": relative_path,
            "segment_id": relative_path,
            "stream_key": stream_key,
            "scope": scope,
            "market_id": market_id,
            "segment_seq": segment_seq,
            "size_bytes": stat.st_size,
            "mtime_ns": stat.st_mtime_ns,
            "sha256": self._file_sha256_cached(relative_path, path, stat.st_size, stat.st_mtime_ns),
        }

    @staticmethod
    def _parse_sync_segment_path(relative_path: str) -> tuple[str, str, str | None, int | None]:
        parts = Path(relative_path).parts
        if len(parts) == 3 and parts[0] == "global":
            topic = parts[1]
            return (
                f"global/{topic}",
                "global",
                None,
                DashboardHTTPServer._parse_segment_seq(parts[2]),
            )
        if len(parts) == 4 and parts[0] == "markets":
            market_id = parts[1]
            topic = parts[2]
            return (
                f"markets/{market_id}/{topic}",
                "market",
                market_id,
                DashboardHTTPServer._parse_segment_seq(parts[3]),
            )
        return (relative_path, "other", None, None)

    @staticmethod
    def _parse_segment_seq(filename: str) -> int | None:
        stem = filename[:-6] if filename.endswith(".jsonl") else filename
        try:
            return int(stem)
        except ValueError:
            return None

    @staticmethod
    def _format_utc_ts(value: datetime) -> str:
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

    @staticmethod
    def _parse_utc_ts(value: str) -> datetime:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)

    def _file_sha256_cached(
        self, relative_path: str, path: Path, size_bytes: int, mtime_ns: int
    ) -> str:
        """Return sha256 for a sealed file, using an in-memory cache.

        Sealed files are immutable after rotate, so (size_bytes, mtime_ns) is a
        stable cache key.  The cache is never explicitly invalidated; entries just
        become unreachable once GC removes the file and it stops appearing in the
        manifest iteration.
        """
        cache_lock = getattr(self, "_sha256_cache_lock", None)
        if cache_lock is None:
            cache_lock = threading.Lock()
            self._sha256_cache_lock = cache_lock
        cache = getattr(self, "_sha256_cache", None)
        if cache is None:
            cache = {}
            self._sha256_cache = cache
        with cache_lock:
            cached = cache.get(relative_path)
            if cached and cached[0] == size_bytes and cached[1] == mtime_ns:
                return cached[2]
        digest = self._file_sha256(path)
        with cache_lock:
            cache[relative_path] = (size_bytes, mtime_ns, digest)
        return digest

    @staticmethod
    def _file_sha256(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            while True:
                chunk = handle.read(1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
        return digest.hexdigest()

    @staticmethod
    def _normalize_expected_entries(expected_entries: object | None) -> dict[str, dict[str, Any]]:
        if expected_entries in (None, []):
            return {}
        if not isinstance(expected_entries, list):
            raise ValueError("entries must be a JSON array when provided")
        normalized: dict[str, dict[str, Any]] = {}
        for item in expected_entries:
            if not isinstance(item, dict):
                raise ValueError("each expected entry must be an object")
            path = str(item.get("path") or "").strip()
            if not path:
                raise ValueError("each expected entry must include path")
            normalized[path] = item
        return normalized

    def _validate_expected_sync_entry(
        self,
        path: Path,
        relative_path: str,
        expected_entry: dict[str, Any],
    ) -> None:
        expected_sha256 = str(expected_entry.get("sha256") or "").strip()
        if expected_sha256 and self._file_sha256(path) != expected_sha256:
            raise ValueError(f"sync entry checksum changed before delete: {relative_path}")
        expected_size = expected_entry.get("size_bytes")
        if expected_size is not None and int(expected_size) != path.stat().st_size:
            raise ValueError(f"sync entry size changed before delete: {relative_path}")

    def _refresh_after_records_mutation(self) -> None:
        with self._cache_lock:
            self._live_signature = None
            self._cache_signature = None
            self._cache_payload = None
            self._cache_body = None
            self._index_body = None
            self._market_payloads = {}
            self._market_bodies = {}
            self._global_window_bodies = {}
            self._build_error = None
            self._building = True
        self._rebuild_cache(self._records_signature())

    @staticmethod
    def _read_last_jsonl_record(paths: list[Path] | Path | None) -> dict | None:
        for path in reversed(DashboardHTTPServer._normalize_stream_paths(paths)):
            with path.open("rb") as handle:
                handle.seek(0, 2)
                position = handle.tell()
                buffer = bytearray()
                while position > 0:
                    position -= 1
                    handle.seek(position)
                    byte = handle.read(1)
                    if byte == b"\n":
                        if buffer:
                            line = bytes(reversed(buffer)).decode("utf-8", errors="ignore").strip()
                            if line:
                                return json.loads(line)
                            buffer.clear()
                        continue
                    buffer.extend(byte)
                if buffer:
                    line = bytes(reversed(buffer)).decode("utf-8", errors="ignore").strip()
                    if line:
                        return json.loads(line)
        return None

    @classmethod
    def _load_binance_window(
        cls,
        builder,
        paths: list[Path] | Path | None,
        start_ts: int | None,
        end_ts: int | None,
    ) -> list[dict]:
        return cls._load_jsonl_window(
            paths=paths,
            start_ts=start_ts,
            end_ts=end_ts,
            ts_getter=lambda record: cls._binance_record_ts(builder, record),
            point_builder=lambda record: cls._build_binance_point(builder, record),
        )

    @classmethod
    def _load_chainlink_window(
        cls,
        builder,
        paths: list[Path] | Path | None,
        start_ts: int | None,
        end_ts: int | None,
    ) -> list[dict]:
        return cls._load_jsonl_window(
            paths=paths,
            start_ts=start_ts,
            end_ts=end_ts,
            ts_getter=lambda record: cls._chainlink_record_ts(builder, record),
            point_builder=lambda record: cls._build_chainlink_point(builder, record),
        )

    @staticmethod
    def _binance_record_ts(builder, record: dict) -> int | None:
        return builder._safe_int(
            record.get("payload", {}).get("tick", {}).get("recv_ts_ms")
            or record.get("payload", {}).get("tick", {}).get("event_ts_ms")
        )

    @staticmethod
    def _chainlink_record_ts(builder, record: dict) -> int | None:
        return builder._safe_int(
            record.get("payload", {}).get("tick", {}).get("oracle_ts_ms")
            or record.get("payload", {}).get("tick", {}).get("recv_ts_ms")
        )

    def _select_stream_paths_for_window(
        self,
        *,
        cache_key: str,
        paths: list[Path] | Path | None,
        start_ts: int | None,
        end_ts: int | None,
        ts_getter: Callable[[dict], int | None],
    ) -> list[Path]:
        normalized_paths = self._normalize_stream_paths(paths)
        if not normalized_paths:
            return []
        if not isinstance(start_ts, int) or not isinstance(end_ts, int) or end_ts <= start_ts:
            return []

        stream_ranges = self._stream_ranges_for_paths(
            cache_key=cache_key,
            paths=normalized_paths,
            ts_getter=ts_getter,
        )
        selected: list[Path] = []
        for path, range_start, range_end in stream_ranges:
            if isinstance(range_start, int) and range_start > end_ts:
                break
            if isinstance(range_end, int) and range_end < start_ts:
                continue
            if range_start is None and range_end is None:
                selected.append(path)
                continue
            if isinstance(range_start, int) and isinstance(range_end, int):
                if range_start <= end_ts and range_end >= start_ts:
                    selected.append(path)
                continue
            selected.append(path)
        return selected

    def _stream_ranges_for_paths(
        self,
        *,
        cache_key: str,
        paths: list[Path],
        ts_getter: Callable[[dict], int | None],
    ) -> list[tuple[Path, int | None, int | None]]:
        path_key = tuple(str(path) for path in paths)
        with self._cache_lock:
            cached = self._stream_file_ranges.get(cache_key)
            if cached is not None and cached[0] == path_key:
                return cached[1]

        ranges = [
            (
                path,
                self._read_first_record_ts(path, ts_getter),
                self._read_last_record_ts(path, ts_getter),
            )
            for path in paths
        ]
        with self._cache_lock:
            self._stream_file_ranges[cache_key] = (path_key, ranges)
        return ranges

    @staticmethod
    def _build_binance_point(builder, record: dict) -> dict | None:
        tick = record.get("payload", {}).get("tick")
        if not isinstance(tick, dict):
            return None
        ts = builder._safe_int(tick.get("recv_ts_ms") or tick.get("event_ts_ms"))
        bid = builder._safe_float(tick.get("best_bid"))
        ask = builder._safe_float(tick.get("best_ask"))
        return {
            "ts": ts,
            "last": builder._safe_float(tick.get("last_price")),
            "bid": bid,
            "ask": ask,
            "mid": builder._mid(bid, ask),
        }

    @staticmethod
    def _build_chainlink_point(builder, record: dict) -> dict | None:
        tick = record.get("payload", {}).get("tick")
        if not isinstance(tick, dict):
            return None
        ts = builder._safe_int(tick.get("oracle_ts_ms") or tick.get("recv_ts_ms"))
        return {
            "ts": ts,
            "price": builder._safe_float(tick.get("price")),
            "bid": builder._safe_float(tick.get("bid")),
            "ask": builder._safe_float(tick.get("ask")),
        }

    @classmethod
    def _load_jsonl_window(
        cls,
        *,
        paths: list[Path] | Path | None,
        start_ts: int | None,
        end_ts: int | None,
        ts_getter: Callable[[dict], int | None],
        point_builder: Callable[[dict], dict | None],
    ) -> list[dict]:
        points: list[dict] = []
        for path in cls._normalize_stream_paths(paths):
            with path.open("rb") as handle:
                start_offset = 0
                if isinstance(start_ts, int):
                    start_offset = cls._find_record_offset_for_ts(handle, start_ts, ts_getter)

                handle.seek(start_offset)
                while True:
                    raw_line = handle.readline()
                    if not raw_line:
                        break
                    line = raw_line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    ts = ts_getter(record)
                    if not isinstance(ts, int):
                        continue
                    if isinstance(start_ts, int) and ts < start_ts:
                        continue
                    if isinstance(end_ts, int) and ts > end_ts:
                        break
                    point = point_builder(record)
                    if point is not None:
                        points.append(point)
        return points

    @staticmethod
    def _normalize_stream_paths(paths: list[Path] | Path | None) -> list[Path]:
        if paths is None:
            return []
        if isinstance(paths, Path):
            return [paths] if paths.exists() else []
        return [path for path in paths if path.exists()]

    @classmethod
    def _find_record_offset_for_ts(
        cls,
        handle,
        target_ts: int,
        ts_getter: Callable[[dict], int | None],
    ) -> int:
        handle.seek(0, 2)
        file_size = handle.tell()
        if file_size <= 0:
            return 0

        low = 0
        high = file_size
        candidate = 0
        while low < high:
            mid = (low + high) // 2
            record_meta = cls._read_record_at_or_after(handle, mid)
            if record_meta is None:
                high = mid
                continue
            record_offset, next_offset, record = record_meta
            ts = ts_getter(record)
            if not isinstance(ts, int):
                low = next_offset
                continue
            if ts < target_ts:
                low = next_offset
                continue
            candidate = record_offset
            high = mid
        return candidate

    @staticmethod
    def _read_record_at_or_after(handle, offset: int) -> tuple[int, int, dict] | None:
        handle.seek(0, 2)
        file_size = handle.tell()
        if offset >= file_size:
            return None

        handle.seek(offset)
        if offset > 0:
            handle.readline()
        while True:
            record_offset = handle.tell()
            raw_line = handle.readline()
            if not raw_line:
                return None
            next_offset = handle.tell()
            line = raw_line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            return record_offset, next_offset, record

    @staticmethod
    def _read_first_record_ts(path: Path, ts_getter: Callable[[dict], int | None]) -> int | None:
        with path.open("rb") as handle:
            while True:
                raw_line = handle.readline()
                if not raw_line:
                    return None
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                ts = ts_getter(record)
                if isinstance(ts, int):
                    return ts
        return None

    @classmethod
    def _read_last_record_ts(cls, path: Path, ts_getter: Callable[[dict], int | None]) -> int | None:
        record = cls._read_last_jsonl_record(path)
        if record is None:
            return None
        ts = ts_getter(record)
        return ts if isinstance(ts, int) else None


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Serve the dashboard and build market-dashboard.json on demand.",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host to bind the local dashboard server",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port to bind the local dashboard server",
    )
    parser.add_argument(
        "--records-root",
        default=str(DEFAULT_RECORDS_ROOT),
        help="Path to runtime_data/records",
    )
    parser.add_argument(
        "--sync-token-file",
        default=str(DEFAULT_SYNC_TOKEN_PATH),
        help="Path used to persist the sync API bearer token",
    )
    parser.add_argument(
        "--sync-state-dir",
        default=str(DEFAULT_SYNC_STATE_DIR),
        help="Path used to persist sync ack and GC state",
    )
    parser.add_argument(
        "--sync-delete-grace-seconds",
        type=int,
        default=DEFAULT_SYNC_DELETE_GRACE_SECONDS,
        help="Grace period between ack and server-side deletion",
    )
    parser.add_argument(
        "--sync-gc-interval-seconds",
        type=int,
        default=DEFAULT_SYNC_GC_INTERVAL_SECONDS,
        help="Background sync GC poll interval; use 0 to disable",
    )
    return parser.parse_args()


def _load_or_create_sync_token(configured_token: str | None, token_path: Path) -> str:
    token = (configured_token or "").strip()
    if token:
        _persist_sync_token(token_path, token)
        return token
    if token_path.exists():
        existing = token_path.read_text(encoding="utf-8").strip()
        if existing:
            return existing
    token = secrets.token_urlsafe(32)
    _persist_sync_token(token_path, token)
    return token


def _persist_sync_token(token_path: Path, token: str) -> None:
    token_path.parent.mkdir(parents=True, exist_ok=True)
    token_path.write_text(f"{token}\n", encoding="utf-8")
    try:
        os.chmod(token_path, 0o600)
    except OSError:
        pass


def main() -> None:
    args = _parse_args()
    records_root = Path(args.records_root).resolve()
    sync_token_path = Path(args.sync_token_file).resolve()
    sync_state_dir = Path(args.sync_state_dir).resolve()
    handler_cls = lambda *handler_args, **handler_kwargs: DashboardRequestHandler(  # noqa: E731
        *handler_args,
        directory=str(DASHBOARD_ROOT),
        **handler_kwargs,
    )
    server = DashboardHTTPServer(
        (args.host, args.port),
        handler_cls,
        records_root,
        sync_token=os.getenv(SYNC_TOKEN_ENV, ""),
        sync_token_path=sync_token_path,
        sync_state_dir=sync_state_dir,
        sync_delete_grace_seconds=args.sync_delete_grace_seconds,
        sync_gc_interval_seconds=args.sync_gc_interval_seconds,
    )
    print(f"dashboard server listening on http://{args.host}:{args.port}")
    print(f"records root: {records_root}")
    print(f"sync token file: {server.sync_token_path}")
    print(f"sync state dir: {server.sync_state_dir}")
    print(f"sync delete grace seconds: {server.sync_delete_grace_seconds}")
    print(f"sync gc interval seconds: {server.sync_gc_interval_seconds}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
