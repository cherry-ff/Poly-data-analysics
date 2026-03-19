#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import socket
import shutil
import sys
import tarfile
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_ENV_PATH = SCRIPT_DIR / ".env"
DEFAULT_OUTPUT_DIR = SCRIPT_DIR / "synced_records"
DEFAULT_STATE_DIR_NAME = ".poly15_sync_state"
DEFAULT_TIMEOUT_SECONDS = 60
DEFAULT_ARCHIVE_TIMEOUT_SECONDS = 1800
DEFAULT_BATCH_MAX_BYTES = 64 * 1024 * 1024
DEFAULT_BATCH_MAX_FILES = 64
DOWNLOAD_CHUNK_SIZE = 1024 * 1024
# Grace period after a market's end_ts_ms before we consider it fully settled
# and safe to ACK (delete from server). Must be long enough for the recorder
# to flush and seal any remaining live segments (rotate_interval is 5 min).
DEFAULT_MARKET_SETTLE_GRACE_MS = 15 * 60 * 1000  # 15 minutes
UNKNOWN_PROGRESS_REPORT_STEP_BYTES = 8 * 1024 * 1024


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


class SyncApiError(RuntimeError):
    pass


class SyncClient:
    def __init__(
        self,
        *,
        base_url: str,
        token: str,
        timeout_seconds: int,
        archive_timeout_seconds: int,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._token = token.strip()
        self._timeout_seconds = timeout_seconds
        self._archive_timeout_seconds = archive_timeout_seconds
        if not self._base_url:
            raise SyncApiError("missing POLY15_SYNC_BASE_URL or --base-url")
        if not self._token:
            raise SyncApiError("missing POLY15_SYNC_API_TOKEN or --token")
        if self._timeout_seconds <= 0:
            raise SyncApiError("timeout_seconds must be positive")
        if self._archive_timeout_seconds <= 0:
            raise SyncApiError("archive_timeout_seconds must be positive")

    def manifest(self) -> dict[str, Any]:
        payload = self._request_json("GET", "/api/sync/manifest")
        if not isinstance(payload, dict):
            raise SyncApiError("manifest response is not a JSON object")
        return payload

    def archive(self, paths: list[str] | None) -> tuple[Path, str | None]:
        body = {}
        if paths:
            body["paths"] = paths
        archive_path, headers = self._request_to_file(
            "POST",
            "/api/sync/archive",
            body,
            suffix=".tar.gz",
            progress_label="downloading archive",
            timeout_seconds=self._archive_timeout_seconds,
        )
        return archive_path, headers.get("Content-Disposition")

    def ack(
        self,
        entries: list[dict[str, Any]],
        *,
        source: str = "",
        client_receipt_path: str = "",
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "entries": [
                {
                    "path": entry["path"],
                    "sha256": entry.get("sha256"),
                    "size_bytes": entry.get("size_bytes"),
                }
                for entry in entries
            ]
        }
        if source:
            payload["source"] = source
        if client_receipt_path:
            payload["client_receipt_path"] = client_receipt_path
        response = self._request_json("POST", "/api/sync/acks", payload)
        if not isinstance(response, dict):
            raise SyncApiError("ack response is not a JSON object")
        return response

    def gc(self, *, force: bool = False) -> dict[str, Any]:
        payload = {"force": force}
        response = self._request_json("POST", "/api/sync/gc", payload)
        if not isinstance(response, dict):
            raise SyncApiError("gc response is not a JSON object")
        return response

    def delete(self, paths: list[str], entries: list[dict[str, Any]] | None = None) -> dict[str, Any]:
        payload: dict[str, Any] = {"paths": paths}
        if entries:
            payload["entries"] = [
                {
                    "path": entry["path"],
                    "sha256": entry.get("sha256"),
                    "size_bytes": entry.get("size_bytes"),
                }
                for entry in entries
            ]
        payload = self._request_json("DELETE", "/api/sync/files", payload)
        if not isinstance(payload, dict):
            raise SyncApiError("delete response is not a JSON object")
        return payload

    def _request_json(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
    ) -> Any:
        response, _ = self._request_bytes(method, path, payload)
        try:
            return json.loads(response.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise SyncApiError(f"{method} {path} returned invalid JSON") from exc

    def _build_request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
    ) -> urllib.request.Request:
        url = f"{self._base_url}{path}"
        data = None
        headers = {
            "Authorization": f"Bearer {self._token}",
            "User-Agent": "poly15-sync-client/1.0",
        }
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        return urllib.request.Request(url=url, data=data, headers=headers, method=method)

    def _request_bytes(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        *,
        timeout_seconds: int | None = None,
    ) -> tuple[bytes, dict[str, str]]:
        request = self._build_request(method, path, payload)
        effective_timeout = timeout_seconds or self._timeout_seconds
        try:
            with urllib.request.urlopen(request, timeout=effective_timeout) as response:
                body = response.read()
                response_headers = {key: value for key, value in response.headers.items()}
                return body, response_headers
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="ignore").strip()
            if detail:
                try:
                    parsed = json.loads(detail)
                    detail = json.dumps(parsed, ensure_ascii=True)
                except json.JSONDecodeError:
                    pass
            raise SyncApiError(
                f"{method} {path} failed with HTTP {exc.code}: {detail or exc.reason}"
            ) from exc
        except urllib.error.URLError as exc:
            raise SyncApiError(f"{method} {path} failed: {exc}") from exc
        except (TimeoutError, socket.timeout) as exc:
            raise SyncApiError(
                f"{method} {path} timed out after {effective_timeout}s"
            ) from exc

    def _request_to_file(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        *,
        suffix: str = "",
        progress_label: str = "",
        timeout_seconds: int | None = None,
    ) -> tuple[Path, dict[str, str]]:
        request = self._build_request(method, path, payload)
        effective_timeout = timeout_seconds or self._timeout_seconds
        with tempfile.NamedTemporaryFile(
            prefix="poly15-sync-",
            suffix=suffix,
            delete=False,
        ) as handle:
            temp_path = Path(handle.name)
            try:
                with urllib.request.urlopen(request, timeout=effective_timeout) as response:
                    response_headers = {key: value for key, value in response.headers.items()}
                    total_bytes = _safe_int(response_headers.get("Content-Length"))
                    downloaded_bytes = 0
                    next_percent = 10
                    next_unknown_report = 1
                    while True:
                        chunk = response.read(DOWNLOAD_CHUNK_SIZE)
                        if not chunk:
                            break
                        handle.write(chunk)
                        downloaded_bytes += len(chunk)
                        if not progress_label:
                            continue
                        if total_bytes is not None and total_bytes > 0:
                            current_percent = int(downloaded_bytes * 100 / total_bytes)
                            while next_percent <= current_percent and next_percent < 100:
                                _print_progress(progress_label, downloaded_bytes, total_bytes)
                                next_percent += 10
                        elif downloaded_bytes >= next_unknown_report:
                            _print_progress(progress_label, downloaded_bytes, None)
                            next_unknown_report = downloaded_bytes + UNKNOWN_PROGRESS_REPORT_STEP_BYTES
                    if progress_label:
                        _print_progress(progress_label, downloaded_bytes, total_bytes, final=True)
                    return temp_path, response_headers
            except urllib.error.HTTPError as exc:
                temp_path.unlink(missing_ok=True)
                detail = exc.read().decode("utf-8", errors="ignore").strip()
                if detail:
                    try:
                        parsed = json.loads(detail)
                        detail = json.dumps(parsed, ensure_ascii=True)
                    except json.JSONDecodeError:
                        pass
                raise SyncApiError(
                    f"{method} {path} failed with HTTP {exc.code}: {detail or exc.reason}"
                ) from exc
            except urllib.error.URLError as exc:
                temp_path.unlink(missing_ok=True)
                raise SyncApiError(f"{method} {path} failed: {exc}") from exc
            except (TimeoutError, socket.timeout) as exc:
                temp_path.unlink(missing_ok=True)
                raise SyncApiError(
                    f"{method} {path} timed out after {effective_timeout}s"
                ) from exc
            except BaseException:
                temp_path.unlink(missing_ok=True)
                raise


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync Poly15 records from the remote dashboard sync API.",
    )
    parser.add_argument(
        "--env-file",
        default=str(DEFAULT_ENV_PATH),
        help="Optional env file that defines POLY15_SYNC_BASE_URL and POLY15_SYNC_API_TOKEN",
    )
    parser.add_argument(
        "--base-url",
        default="",
        help="Server base URL, for example http://167.160.190.152/poly15",
    )
    parser.add_argument(
        "--token",
        default="",
        help="Sync API bearer token",
    )
    parser.add_argument(
        "--token-file",
        default="",
        help="Optional local file that contains the sync API token",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="HTTP timeout for each request",
    )
    parser.add_argument(
        "--archive-timeout-seconds",
        type=int,
        default=DEFAULT_ARCHIVE_TIMEOUT_SECONDS,
        help="Timeout for waiting on the server to prepare and start streaming a sync archive",
    )
    parser.add_argument(
        "--state-dir",
        default="",
        help="Optional local directory that stores sync receipts and the confirmed-segment index",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    manifest_parser = subparsers.add_parser("manifest", help="Print the remote sync manifest")
    manifest_parser.add_argument(
        "--output",
        default="",
        help="Optional path to save the manifest JSON",
    )

    sync_parser = subparsers.add_parser("sync", help="Download and extract remote records")
    sync_parser.add_argument(
        "paths",
        nargs="*",
        help="Optional relative record paths, for example markets/1604309 or global",
    )
    sync_parser.add_argument(
        "--output-dir",
        default="",
        help="Local directory where records will be extracted",
    )
    sync_parser.add_argument(
        "--delete-remote",
        action="store_true",
        help="Delete the remote files after a successful download and extraction",
    )
    sync_parser.add_argument(
        "--manifest-out",
        default="",
        help="Optional path to save the manifest JSON used for the sync",
    )
    sync_parser.add_argument(
        "--receipt-out",
        default="",
        help="Optional path to save the receipt JSON for the verified local sync",
    )
    sync_parser.add_argument(
        "--force",
        action="store_true",
        help="Sync matching remote files even if they are already confirmed locally",
    )
    sync_parser.add_argument(
        "--batch-max-bytes",
        type=int,
        default=DEFAULT_BATCH_MAX_BYTES,
        help="Maximum total payload size per archive request; use 0 to disable byte-based batching",
    )
    sync_parser.add_argument(
        "--batch-max-files",
        type=int,
        default=DEFAULT_BATCH_MAX_FILES,
        help="Maximum number of files per archive request; use 0 to disable file-count batching",
    )

    delete_parser = subparsers.add_parser("delete", help="Delete remote files that were already synced")
    delete_parser.add_argument(
        "paths",
        nargs="*",
        help="Relative record paths to delete",
    )
    delete_parser.add_argument(
        "--all",
        action="store_true",
        help="Delete every remote file currently listed by the manifest",
    )
    delete_parser.add_argument(
        "--output-dir",
        default="",
        help="Local directory that contains synced records used to confirm safe deletes",
    )
    delete_parser.add_argument(
        "--force",
        action="store_true",
        help="Allow deleting matched remote files even if they are not confirmed in the local sync index",
    )

    gc_parser = subparsers.add_parser("gc", help="Run the server-side sync garbage collector")
    gc_parser.add_argument(
        "--force",
        action="store_true",
        help="Ignore grace deadlines and attempt to delete every acked segment now",
    )

    return parser.parse_args()


def _resolve_token(args: argparse.Namespace) -> str:
    if args.token:
        return args.token.strip()
    env_token = os.getenv("POLY15_SYNC_API_TOKEN", "").strip()
    if env_token:
        return env_token
    token_file = args.token_file.strip() or os.getenv("POLY15_SYNC_TOKEN_FILE", "").strip()
    if token_file:
        return Path(token_file).expanduser().read_text(encoding="utf-8").strip()
    return ""


def _resolve_base_url(args: argparse.Namespace) -> str:
    return args.base_url.strip() or os.getenv("POLY15_SYNC_BASE_URL", "").strip()


def _save_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


def _safe_int(value: object) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def _format_bytes(value: int) -> str:
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    size = float(value)
    unit = units[0]
    for candidate in units:
        unit = candidate
        if size < 1024 or candidate == units[-1]:
            break
        size /= 1024
    if unit == "B":
        return f"{int(size)} {unit}"
    return f"{size:.1f} {unit}"


def _print_status(message: str) -> None:
    print(message, flush=True)


def _print_progress(
    label: str,
    current_bytes: int,
    total_bytes: int | None,
    *,
    final: bool = False,
) -> None:
    if total_bytes is not None and total_bytes > 0:
        percent = min(current_bytes / total_bytes * 100, 100.0)
        suffix = "completed" if final else "in progress"
        print(
            f"{label}: {percent:.1f}% "
            f"({_format_bytes(current_bytes)} / {_format_bytes(total_bytes)}) [{suffix}]",
            flush=True,
        )
        return
    suffix = "completed" if final else "in progress"
    print(f"{label}: {_format_bytes(current_bytes)} [{suffix}]", flush=True)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _extract_archive(archive_path: Path, output_dir: Path) -> tuple[Path, list[str]]:
    output_dir.mkdir(parents=True, exist_ok=True)
    staging_dir = Path(tempfile.mkdtemp(prefix="poly15-sync-", dir=output_dir))
    extracted: list[str] = []
    try:
        with tarfile.open(archive_path, mode="r:gz") as archive:
            members = archive.getmembers()
            file_members = [member for member in members if member.isfile()]
            total_files = len(file_members)
            report_every = max(1, total_files // 10) if total_files > 10 else 1
            staging_root = staging_dir.resolve()
            for member in members:
                member_path = staging_dir / member.name
                resolved_target = member_path.resolve()
                if staging_root not in resolved_target.parents and resolved_target != staging_root:
                    raise SyncApiError(f"archive entry escapes output directory: {member.name}")
                if member.islnk() or member.issym():
                    raise SyncApiError(f"archive entry uses unsupported link type: {member.name}")
                if member.isdir():
                    continue
                if not member.isfile() or not member.name.endswith(".jsonl"):
                    raise SyncApiError(f"archive entry uses unsupported file type: {member.name}")

            for member in members:
                target_path = staging_dir / member.name
                if member.isdir():
                    target_path.mkdir(parents=True, exist_ok=True)
                    continue
                target_path.parent.mkdir(parents=True, exist_ok=True)
                source = archive.extractfile(member)
                if source is None:
                    raise SyncApiError(f"failed to extract archive entry: {member.name}")
                with source, target_path.open("wb") as handle:
                    shutil.copyfileobj(source, handle)
                extracted.append(member.name)
                extracted_count = len(extracted)
                if extracted_count % report_every == 0 or extracted_count == total_files:
                    _print_status(f"extracting archive: {extracted_count}/{total_files} files")
    except BaseException:
        shutil.rmtree(staging_dir, ignore_errors=True)
        raise
    return staging_dir, extracted


def _manifest_paths(manifest: dict[str, Any]) -> list[str]:
    entries = manifest.get("entries")
    if not isinstance(entries, list):
        raise SyncApiError("manifest entries are missing")
    paths: list[str] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        path = str(entry.get("path") or "").strip()
        if path:
            paths.append(path)
    return paths


def _manifest_entries_by_path(manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    entries = manifest.get("entries")
    if not isinstance(entries, list):
        raise SyncApiError("manifest entries are missing")
    normalized: dict[str, dict[str, Any]] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        path = str(entry.get("path") or "").strip()
        if path:
            normalized[path] = entry
    return normalized


def _expand_requested_paths(manifest: dict[str, Any], requested_paths: list[str]) -> list[str]:
    manifest_paths = _manifest_paths(manifest)
    if not requested_paths:
        return manifest_paths

    matched: list[str] = []
    seen: set[str] = set()
    for requested_path in requested_paths:
        normalized = requested_path.strip().strip("/")
        if not normalized:
            continue
        prefix = f"{normalized}/"
        for manifest_path in manifest_paths:
            if manifest_path == normalized or manifest_path.startswith(prefix):
                if manifest_path not in seen:
                    seen.add(manifest_path)
                    matched.append(manifest_path)
    if not matched:
        raise SyncApiError("requested paths matched no manifest entries")
    return matched


def _resolve_output_dir(args: argparse.Namespace) -> Path:
    raw_output_dir = (
        getattr(args, "output_dir", "").strip()
        or os.getenv("POLY15_SYNC_OUTPUT_DIR", "").strip()
        or str(DEFAULT_OUTPUT_DIR)
    )
    return Path(raw_output_dir).expanduser().resolve()


def _resolve_state_dir(args: argparse.Namespace, output_dir: Path) -> Path:
    raw_state_dir = args.state_dir.strip() or os.getenv("POLY15_SYNC_STATE_DIR", "").strip()
    if raw_state_dir:
        return Path(raw_state_dir).expanduser().resolve()
    return (output_dir / DEFAULT_STATE_DIR_NAME).resolve()


def _sync_index_path(state_dir: Path) -> Path:
    return state_dir / "index.json"


def _load_sync_index(state_dir: Path) -> dict[str, dict[str, Any]]:
    path = _sync_index_path(state_dir)
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SyncApiError(f"invalid sync index JSON: {path}") from exc
    if not isinstance(payload, dict):
        raise SyncApiError(f"sync index root must be a JSON object: {path}")
    entries = payload.get("entries", {})
    if not isinstance(entries, dict):
        raise SyncApiError(f"sync index entries must be a JSON object: {path}")
    normalized: dict[str, dict[str, Any]] = {}
    for key, value in entries.items():
        if isinstance(key, str) and isinstance(value, dict):
            normalized[key] = value
    return normalized


def _save_sync_index(state_dir: Path, entries: dict[str, dict[str, Any]]) -> Path:
    path = _sync_index_path(state_dir)
    _save_json(
        path,
        {
            "updated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "entries": entries,
        },
    )
    return path


def _entry_is_confirmed_locally(
    entry: dict[str, Any],
    sync_index: dict[str, dict[str, Any]],
    output_dir: Path,
) -> bool:
    path = str(entry.get("path") or "").strip()
    if not path:
        return False
    indexed = sync_index.get(path)
    if not isinstance(indexed, dict):
        return False
    manifest_sha256 = str(entry.get("sha256") or "").strip()
    if manifest_sha256 and indexed.get("sha256") != manifest_sha256:
        return False
    local_path = output_dir / path
    if not local_path.exists():
        return False
    expected_size = indexed.get("size_bytes") or entry.get("size_bytes")
    if expected_size is not None and local_path.stat().st_size != int(expected_size):
        return False
    return True


def _filter_confirmed_paths(
    archive_paths: list[str],
    manifest_entries: dict[str, dict[str, Any]],
    sync_index: dict[str, dict[str, Any]],
    output_dir: Path,
) -> list[str]:
    filtered: list[str] = []
    for path in archive_paths:
        entry = manifest_entries.get(path)
        if entry is None:
            raise SyncApiError(f"manifest is missing entry metadata for path: {path}")
        if _entry_is_confirmed_locally(entry, sync_index, output_dir):
            continue
        filtered.append(path)
    return filtered


def _entry_size_bytes(path: str, manifest_entries: dict[str, dict[str, Any]]) -> int:
    entry = manifest_entries.get(path)
    if entry is None:
        raise SyncApiError(f"manifest is missing entry metadata for path: {path}")
    return int(entry.get("size_bytes") or 0)


def _build_sync_batches(
    archive_paths: list[str],
    manifest_entries: dict[str, dict[str, Any]],
    *,
    max_batch_bytes: int,
    max_batch_files: int,
) -> list[list[str]]:
    if not archive_paths:
        return []
    if max_batch_bytes <= 0 and max_batch_files <= 0:
        return [archive_paths]

    batches: list[list[str]] = []
    current_batch: list[str] = []
    current_batch_bytes = 0
    for path in archive_paths:
        path_size = _entry_size_bytes(path, manifest_entries)
        exceeds_file_limit = max_batch_files > 0 and current_batch and len(current_batch) >= max_batch_files
        exceeds_byte_limit = (
            max_batch_bytes > 0
            and current_batch
            and current_batch_bytes + path_size > max_batch_bytes
        )
        if exceeds_file_limit or exceeds_byte_limit:
            batches.append(current_batch)
            current_batch = []
            current_batch_bytes = 0
        current_batch.append(path)
        current_batch_bytes += path_size
    if current_batch:
        batches.append(current_batch)
    return batches


def _batch_total_bytes(batch_paths: list[str], manifest_entries: dict[str, dict[str, Any]]) -> int:
    return sum(_entry_size_bytes(path, manifest_entries) for path in batch_paths)


def _build_verified_entries(
    archive_paths: list[str],
    extracted: list[str],
    manifest_entries: dict[str, dict[str, Any]],
    root_dir: Path,
) -> list[dict[str, Any]]:
    expected = sorted(set(archive_paths))
    actual = sorted(set(extracted))
    if actual != expected:
        raise SyncApiError(
            "extracted files do not match the requested manifest entries: "
            f"expected={expected} actual={actual}"
        )

    verified: list[dict[str, Any]] = []
    for path in expected:
        manifest_entry = manifest_entries.get(path)
        if manifest_entry is None:
            raise SyncApiError(f"manifest entry metadata missing for extracted path: {path}")
        local_path = root_dir / path
        if not local_path.exists():
            raise SyncApiError(f"extracted file missing after sync: {local_path}")
        size_bytes = local_path.stat().st_size
        expected_size = manifest_entry.get("size_bytes")
        if expected_size is not None and size_bytes != int(expected_size):
            raise SyncApiError(f"size mismatch for extracted file: {path}")
        sha256 = _file_sha256(local_path)
        expected_sha256 = str(manifest_entry.get("sha256") or "").strip()
        if expected_sha256 and sha256 != expected_sha256:
            raise SyncApiError(f"checksum mismatch for extracted file: {path}")
        verified.append(
            {
                "path": path,
                "segment_id": manifest_entry.get("segment_id") or path,
                "stream_key": manifest_entry.get("stream_key"),
                "scope": manifest_entry.get("scope"),
                "market_id": manifest_entry.get("market_id"),
                "segment_seq": manifest_entry.get("segment_seq"),
                "size_bytes": size_bytes,
                "sha256": sha256,
            }
        )
    return verified


def _commit_verified_entries(
    staging_dir: Path,
    output_dir: Path,
    verified_entries: list[dict[str, Any]],
) -> None:
    for entry in verified_entries:
        relative_path = str(entry.get("path") or "").strip()
        if not relative_path:
            raise SyncApiError("verified entry is missing path")
        staged_path = staging_dir / relative_path
        if not staged_path.exists():
            raise SyncApiError(f"verified staged file missing before commit: {staged_path}")
        destination_path = output_dir / relative_path
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        staged_path.replace(destination_path)


def _plan_delete_entries(
    manifest: dict[str, Any],
    requested_paths: list[str],
    sync_index: dict[str, dict[str, Any]],
    output_dir: Path,
    *,
    require_local_confirmation: bool,
) -> tuple[list[str], list[dict[str, Any]], list[str]]:
    manifest_entries = _manifest_entries_by_path(manifest)
    delete_paths = _expand_requested_paths(manifest, requested_paths)
    planned_paths: list[str] = []
    planned_entries: list[dict[str, Any]] = []
    skipped_paths: list[str] = []
    for path in delete_paths:
        entry = manifest_entries.get(path)
        if entry is None:
            raise SyncApiError(f"manifest is missing entry metadata for path: {path}")
        if require_local_confirmation and not _entry_is_confirmed_locally(entry, sync_index, output_dir):
            skipped_paths.append(path)
            continue
        planned_paths.append(path)
        planned_entries.append(entry)
    return planned_paths, planned_entries, skipped_paths


def _write_sync_receipt(
    state_dir: Path,
    receipt_payload: dict[str, Any],
    explicit_path: str = "",
) -> Path:
    if explicit_path.strip():
        receipt_path = Path(explicit_path).expanduser().resolve()
    else:
        receipts_dir = state_dir / "receipts"
        receipt_name = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ") + ".json"
        receipt_path = receipts_dir / receipt_name
    _save_json(receipt_path, receipt_payload)
    return receipt_path


def _update_sync_index(
    state_dir: Path,
    sync_index: dict[str, dict[str, Any]],
    verified_entries: list[dict[str, Any]],
    receipt_path: Path,
) -> None:
    confirmed_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    receipt_ref = str(receipt_path)
    for entry in verified_entries:
        sync_index[entry["path"]] = {
            "path": entry["path"],
            "segment_id": entry.get("segment_id"),
            "sha256": entry.get("sha256"),
            "size_bytes": entry.get("size_bytes"),
            "confirmed_at": confirmed_at,
            "receipt_path": receipt_ref,
        }
    _save_sync_index(state_dir, sync_index)


def _print_json(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, indent=2, ensure_ascii=True))


def _batch_receipt_out(explicit_path: str, batch_index: int, batch_count: int) -> str:
    if not explicit_path.strip() or batch_count <= 1:
        return explicit_path
    resolved = Path(explicit_path).expanduser().resolve()
    suffix = "".join(resolved.suffixes)
    stem = resolved.name[: -len(suffix)] if suffix else resolved.name
    batch_name = f"{stem}.part{batch_index:03d}of{batch_count:03d}{suffix}"
    return str(resolved.with_name(batch_name))


def _sync_batch(
    client: SyncClient,
    *,
    batch_paths: list[str],
    manifest_entries: dict[str, dict[str, Any]],
    output_dir: Path,
    state_dir: Path,
    sync_index: dict[str, dict[str, Any]],
    receipt_out: str,
    delete_remote: bool,
    batch_index: int,
    batch_count: int,
) -> tuple[int, int]:
    batch_label = f"batch {batch_index}/{batch_count}"
    batch_bytes = _batch_total_bytes(batch_paths, manifest_entries)
    _print_status(f"{batch_label}: syncing {len(batch_paths)} files, {_format_bytes(batch_bytes)}")
    _print_status(f"{batch_label}: requesting archive from server...")
    archive_path, content_disposition = client.archive(batch_paths)
    archive_size = archive_path.stat().st_size
    try:
        _print_status(f"{batch_label}: extracting archive to staging directory...")
        staging_dir, extracted = _extract_archive(archive_path, output_dir)
        try:
            _print_status(f"{batch_label}: verifying extracted files...")
            verified_entries = _build_verified_entries(
                batch_paths,
                extracted,
                manifest_entries,
                staging_dir,
            )
            _print_status(f"{batch_label}: committing verified files to output directory...")
            _commit_verified_entries(staging_dir, output_dir, verified_entries)
        finally:
            shutil.rmtree(staging_dir, ignore_errors=True)
    finally:
        archive_path.unlink(missing_ok=True)

    receipt_payload = {
        "verified_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "base_url": client._base_url,
        "output_dir": str(output_dir),
        "state_dir": str(state_dir),
        "entries": verified_entries,
        "batch_index": batch_index,
        "batch_count": batch_count,
    }
    receipt_path = _write_sync_receipt(
        state_dir,
        receipt_payload,
        _batch_receipt_out(receipt_out, batch_index, batch_count),
    )
    _update_sync_index(state_dir, sync_index, verified_entries, receipt_path)
    _print_status(f"{batch_label}: archive downloaded: {archive_size} bytes")
    _print_status(f"{batch_label}: files extracted: {len(verified_entries)}")
    _print_status(f"{batch_label}: output dir: {output_dir}")
    _print_status(f"{batch_label}: receipt saved to {receipt_path}")
    if content_disposition:
        _print_status(f"{batch_label}: content disposition: {content_disposition}")
    if delete_remote:
        ack_entries, skipped_active = _filter_settled_for_ack(
            verified_entries,
            output_dir,
            settle_grace_ms=DEFAULT_MARKET_SETTLE_GRACE_MS,
        )
        if skipped_active:
            _print_status(
                f"{batch_label}: skipped ACK for {skipped_active} file(s) "
                f"from active/unfinished markets (will re-check on next sync)"
            )
        if ack_entries:
            _print_status(f"{batch_label}: submitting remote delete ack for {len(ack_entries)} settled file(s)...")
            result = client.ack(
                ack_entries,
                source="sync --delete-remote",
                client_receipt_path=str(receipt_path),
            )
            _print_status(
                f"{batch_label}: remote delete scheduled: "
                f"{result.get('acked_file_count', 0)} files, "
                f"delete_after={result.get('delete_after_max')}"
            )
    return len(verified_entries), archive_size


def _run_manifest(client: SyncClient, args: argparse.Namespace) -> int:
    _print_status("requesting remote manifest...")
    manifest = client.manifest()
    if args.output:
        output_path = Path(args.output).expanduser().resolve()
        _save_json(output_path, manifest)
        print(f"manifest saved to {output_path}")
    _print_json(manifest)
    return 0


def _read_market_end_ts_ms(output_dir: Path, market_id: str) -> int | None:
    """Return end_ts_ms from local market_metadata files, or None if not found."""
    meta_dir = output_dir / "markets" / market_id / "market_metadata"
    if not meta_dir.exists():
        return None
    files = sorted(meta_dir.glob("*.jsonl"))
    for meta_file in reversed(files):
        try:
            lines = meta_file.read_text(encoding="utf-8").strip().splitlines()
            for line in reversed(lines):
                d = json.loads(line)
                end_ts = d.get("payload", {}).get("market", {}).get("end_ts_ms")
                if end_ts is not None:
                    return int(end_ts)
        except Exception:
            continue
    return None


def _filter_settled_for_ack(
    verified_entries: list[dict[str, Any]],
    output_dir: Path,
    settle_grace_ms: int,
) -> tuple[list[dict[str, Any]], int]:
    """Return (entries_to_ack, skipped_count).

    Global (non-market) files are always ACKed – they are rolling streams with
    no settlement concept.  Market files are only ACKed once the market's
    end_ts_ms has passed (plus settle_grace_ms), so the server keeps unfinished
    markets available for future incremental syncs.
    """
    now_ms = int(time.time() * 1000)
    to_ack: list[dict[str, Any]] = []
    skipped = 0
    for entry in verified_entries:
        market_id = entry.get("market_id")
        if not market_id:
            # global stream – always safe to ACK
            to_ack.append(entry)
            continue
        end_ts_ms = _read_market_end_ts_ms(output_dir, str(market_id))
        if end_ts_ms is None or now_ms >= end_ts_ms + settle_grace_ms:
            to_ack.append(entry)
        else:
            skipped += 1
    return to_ack, skipped


def _cleanup_stale_staging_dirs(output_dir: Path) -> None:
    """Remove leftover poly15-sync-* staging directories from interrupted syncs."""
    for stale in output_dir.glob("poly15-sync-*"):
        if stale.is_dir():
            shutil.rmtree(stale, ignore_errors=True)


def _run_sync(client: SyncClient, args: argparse.Namespace) -> int:
    requested_paths = [path for path in args.paths if path]
    _print_status("requesting remote manifest...")
    manifest = client.manifest()
    manifest_entries = _manifest_entries_by_path(manifest)
    output_dir = _resolve_output_dir(args)
    state_dir = _resolve_state_dir(args, output_dir)

    _cleanup_stale_staging_dirs(output_dir)

    sync_index = _load_sync_index(state_dir)

    archive_paths = _expand_requested_paths(manifest, requested_paths)
    if not args.force:
        archive_paths = _filter_confirmed_paths(
            archive_paths,
            manifest_entries,
            sync_index,
            output_dir,
        )

    # Sort oldest-first by file mtime so historical data is prioritised and
    # GC can reclaim server disk space as early as possible.
    archive_paths = sorted(
        archive_paths,
        key=lambda p: manifest_entries[p].get("mtime_ns", 0),
    )

    if args.manifest_out:
        manifest_path = Path(args.manifest_out).expanduser().resolve()
        _save_json(manifest_path, manifest)
        _print_status(f"manifest saved to {manifest_path}")

    if not archive_paths:
        _print_status("no unsynced remote files matched; skip sync")
        return 0

    total_bytes = _batch_total_bytes(
        archive_paths,
        manifest_entries,
    )
    sync_batches = _build_sync_batches(
        archive_paths,
        manifest_entries,
        max_batch_bytes=args.batch_max_bytes,
        max_batch_files=args.batch_max_files,
    )
    _print_status(
        "remote manifest received: "
        f"{len(archive_paths)} files selected for sync"
    )
    _print_status(
        "sync plan: "
        f"{len(archive_paths)} files, {_format_bytes(total_bytes)}, "
        f"{len(sync_batches)} batch(es)"
    )
    total_verified = 0
    total_archive_bytes = 0
    for batch_index, batch_paths in enumerate(sync_batches, start=1):
        batch_label = f"batch {batch_index}/{len(sync_batches)}"
        last_error: SyncApiError | None = None
        for attempt in range(1, 4):
            try:
                verified_count, archive_size = _sync_batch(
                    client,
                    batch_paths=batch_paths,
                    manifest_entries=manifest_entries,
                    output_dir=output_dir,
                    state_dir=state_dir,
                    sync_index=sync_index,
                    receipt_out=args.receipt_out,
                    delete_remote=args.delete_remote,
                    batch_index=batch_index,
                    batch_count=len(sync_batches),
                )
                total_verified += verified_count
                total_archive_bytes += archive_size
                last_error = None
                break
            except SyncApiError as exc:
                last_error = exc
                if attempt < 3:
                    wait_s = 2 ** attempt
                    _print_status(
                        f"{batch_label}: attempt {attempt} failed ({exc}); "
                        f"retrying in {wait_s}s"
                    )
                    time.sleep(wait_s)
        if last_error is not None:
            raise last_error
    _print_status(
        f"sync complete: {total_verified} files across {len(sync_batches)} batch(es), "
        f"{_format_bytes(total_archive_bytes)} downloaded"
    )
    return 0


def _run_delete(client: SyncClient, args: argparse.Namespace) -> int:
    requested_paths = [path for path in args.paths if path]
    if not requested_paths and not args.all:
        raise SyncApiError("delete requires explicit paths or --all")
    _print_status("requesting remote manifest...")
    manifest = client.manifest()
    output_dir = _resolve_output_dir(args)
    state_dir = _resolve_state_dir(args, output_dir)
    sync_index = _load_sync_index(state_dir)
    planned_paths, planned_entries, skipped_paths = _plan_delete_entries(
        manifest,
        [] if args.all else requested_paths,
        sync_index,
        output_dir,
        require_local_confirmation=not args.force,
    )
    if skipped_paths:
        print(f"skip unconfirmed remote files: {len(skipped_paths)}")
    if not planned_paths:
        raise SyncApiError("no confirmed remote files matched the delete request")
    result = client.ack(
        planned_entries,
        source="delete",
    )
    _print_json(result)
    return 0


def _run_gc(client: SyncClient, args: argparse.Namespace) -> int:
    result = client.gc(force=args.force)
    _print_json(result)
    return 0


def main() -> int:
    args = _parse_args()
    _load_env_file(Path(args.env_file).expanduser())
    client = SyncClient(
        base_url=_resolve_base_url(args),
        token=_resolve_token(args),
        timeout_seconds=args.timeout_seconds,
        archive_timeout_seconds=args.archive_timeout_seconds,
    )
    if args.command == "manifest":
        return _run_manifest(client, args)
    if args.command == "sync":
        return _run_sync(client, args)
    if args.command == "delete":
        return _run_delete(client, args)
    if args.command == "gc":
        return _run_gc(client, args)
    raise SyncApiError(f"unsupported command: {args.command}")


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SyncApiError as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
    except KeyboardInterrupt:
        print("sync interrupted by user", file=sys.stderr)
        raise SystemExit(130)
