# Poly15 Local Sync Client

This package is a standalone client for pulling `runtime_data/records` from the server sync API.

## Requirements

- Python `3.9+`
- Network access to `http://167.160.190.152/poly15`
- The sync API bearer token from the server

Server token file:

```text
/home/ubuntu/codexProxy/codex2api/deployments/Poly-15minBTC/runtime_data/sync_api_token.txt
```

## Quick Start

1. Copy `.env.example` to `.env`
2. Fill in `POLY15_SYNC_API_TOKEN`
3. Run:

```bash
python3 poly15_sync_client.py manifest
python3 poly15_sync_client.py sync --output-dir ./synced_records
```

## Common Commands

Sync all records:

```bash
python3 poly15_sync_client.py sync --output-dir ./synced_records
```

Sync one market only:

```bash
python3 poly15_sync_client.py sync markets/1604309 --output-dir ./synced_records
```

Sync and then delete the same remote files:

```bash
python3 poly15_sync_client.py sync markets/1604309 --output-dir ./synced_records --delete-remote
```

Schedule every confirmed remote file in the current manifest for server-side deletion:

```bash
python3 poly15_sync_client.py delete --all --output-dir ./synced_records
```

Run the server-side garbage collector immediately:

```bash
python3 poly15_sync_client.py gc
```

Use explicit URL and token without `.env`:

```bash
python3 poly15_sync_client.py \
  --base-url http://167.160.190.152/poly15 \
  --token YOUR_TOKEN \
  sync --output-dir ./synced_records
```

## Commands

- `manifest`: print the remote manifest JSON
- `sync`: download and extract a `tar.gz` archive returned by the server
- `delete`: ack already-synced remote files for server-side deletion after the grace period
- `gc`: run the server-side sync garbage collector

## Notes

- `sync` with no path arguments downloads everything in the manifest.
- `sync --delete-remote` no longer deletes files immediately. It submits an ack to the server, and the server deletes the acked segments only after the configured grace period.
- `sync` streams the remote `tar.gz` into a local temporary archive file, extracts into a temporary staging directory, verifies file size and `sha256`, and only then moves verified files into the final output directory.
- Each successful `sync` writes a receipt under `.poly15_sync_state/receipts` and updates `.poly15_sync_state/index.json`.
- A later `sync` skips segments that are already confirmed locally unless you pass `--force`.
- `sync --delete-remote` and `delete` send `path + sha256 + size_bytes` back to the server for a final pre-ack check.
- `delete --all` now means "ack all remote segments that are currently both in the manifest and confirmed by the local sync index". Use `delete --force` only when you intentionally want to bypass that protection.
