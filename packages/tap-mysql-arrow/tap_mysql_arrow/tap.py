"""tap-mysql-arrow: Singer tap emitting Arrow BATCH messages via ADBC."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.ipc as ipc

from tap_mysql_arrow.client import connect, stream_batches
from tap_mysql_arrow.discovery import discover_catalog
from tap_mysql_arrow.metrics import MetricEmitter


# ── Singer message helpers ──────────────────────────────────────────────────


def _write(msg: dict) -> None:
    sys.stdout.write(json.dumps(msg) + "\n")
    sys.stdout.flush()


def _write_schema(stream_name: str, schema: dict, key_properties: list[str]) -> None:
    _write(
        {
            "type": "SCHEMA",
            "stream": stream_name,
            "schema": schema,
            "key_properties": key_properties,
        }
    )


def _write_batch(stream_name: str, manifest: list[str]) -> None:
    _write(
        {
            "type": "BATCH",
            "stream": stream_name,
            "encoding": {"format": "arrow"},
            "manifest": manifest,
        }
    )


def _write_state(state: dict) -> None:
    _write({"type": "STATE", "value": state})


# ── Config schema ────────────────────────────────────────────────────────────

CONFIG_SCHEMA: dict = {
    "type": "object",
    "required": ["host", "port", "user", "password"],
    "properties": {
        # MySQL connection
        "host": {"type": "string", "description": "MySQL/MariaDB host"},
        "port": {
            "type": "integer",
            "default": 3306,
            "description": "MySQL/MariaDB port",
        },
        "user": {"type": "string", "description": "Database user"},
        "password": {"type": "string", "description": "Database password"},
        "database": {"type": "string", "description": "Default database (optional)"},
        # Scope
        "filter_dbs": {
            "type": "string",
            "description": "Comma-separated list of database names to include",
        },
        # SSL
        "ssl_ca": {"type": "string", "description": "Path to PEM CA certificate"},
        "ssl_cert": {"type": "string", "description": "Path to PEM client certificate"},
        "ssl_key": {"type": "string", "description": "Path to PEM client key"},
        # SSH tunnel
        "ssh_host": {"type": "string", "description": "SSH tunnel host"},
        "ssh_port": {
            "type": "integer",
            "default": 22,
            "description": "SSH tunnel port",
        },
        "ssh_username": {"type": "string", "description": "SSH username"},
        "ssh_private_key": {
            "type": "string",
            "description": "Path to SSH private key file",
        },
        "ssh_private_key_password": {
            "type": "string",
            "description": "Passphrase for encrypted SSH private key",
        },
        "ssh_password": {
            "type": "string",
            "description": "SSH password (alternative to key)",
        },
        # Batching
        "batch_size": {
            "type": "integer",
            "default": 500000,
            "description": "Target number of rows per Arrow IPC file",
        },
        "batch_root_dir": {
            "type": "string",
            "description": "Directory for Arrow IPC batch files (default: current directory)",
        },
        # Session
        "session_sqls": {
            "type": "array",
            "items": {"type": "string"},
            "description": "SQL statements run after connecting",
            "default": [
                "SET @@session.time_zone='+0:00'",
                "SET @@session.wait_timeout=28800",
                "SET @@session.net_read_timeout=3600",
                "SET @@session.innodb_lock_wait_timeout=3600",
            ],
        },
    },
    "additionalProperties": False,
}


# ── Sync logic ───────────────────────────────────────────────────────────────


def _run_session_sqls(conn, config: dict[str, Any]) -> None:
    defaults = [
        "SET @@session.time_zone='+0:00'",
        "SET @@session.wait_timeout=28800",
        "SET @@session.net_read_timeout=3600",
        "SET @@session.innodb_lock_wait_timeout=3600",
    ]
    sqls = config.get("session_sqls", defaults)
    with conn.cursor() as cur:
        for sql in sqls:
            try:
                cur.execute(sql)
            except Exception:
                pass  # non-fatal; some statements may not be supported on all MySQL flavours


def _sync_stream(
    stream_entry: dict,
    config: dict[str, Any],
    output_dir: str,
    state: dict,
) -> None:
    stream_name: str = stream_entry["stream"]
    schema: dict = stream_entry["schema"]
    key_properties: list[str] = stream_entry.get("key_properties", [])

    meta = next(
        (m["metadata"] for m in stream_entry.get("metadata", []) if m["breadcrumb"] == []),
        {},
    )
    db: str = meta.get("database-name", "")
    table: str = meta.get("table-name", stream_name)

    _write_schema(stream_name, schema, key_properties)

    batch_size: int = int(config.get("batch_size", 500_000))
    metrics = MetricEmitter(stream_name)
    metrics.start()

    file_index = 0
    current_batches: list[pa.RecordBatch] = []
    current_rows = 0
    schema_arrow: pa.Schema | None = None

    def _flush_file() -> None:
        nonlocal file_index, current_batches, current_rows
        assert schema_arrow is not None
        file_path = os.path.join(output_dir, f"{stream_name}_{file_index}.arrow")
        with ipc.new_file(file_path, schema_arrow) as writer:
            for b in current_batches:
                writer.write_batch(b)
        _write_batch(stream_name, [f"file://{file_path}"])
        file_index += 1
        current_batches = []
        current_rows = 0

    try:
        with stream_batches(config, db, table) as reader:
            for batch in reader:
                if batch.num_rows == 0:
                    continue
                if schema_arrow is None:
                    schema_arrow = batch.schema
                current_batches.append(batch)
                current_rows += batch.num_rows
                metrics.add(batch.num_rows)
                if current_rows >= batch_size:
                    _flush_file()

        if current_batches:
            _flush_file()
    finally:
        metrics.stop()

    state.setdefault("bookmarks", {})[stream_name] = {
        "replication_method": "FULL_TABLE",
        "initial_full_table_complete": True,
    }


def sync(config: dict[str, Any], catalog: dict, state: dict) -> None:
    output_dir = config.get("batch_root_dir") or os.getcwd()
    os.makedirs(output_dir, exist_ok=True)

    with connect(config) as conn:
        _run_session_sqls(conn, config)

    for entry in catalog.get("streams", []):
        stream_meta = next(
            (m["metadata"] for m in entry.get("metadata", []) if m["breadcrumb"] == []),
            {},
        )
        if not stream_meta.get("selected", True):
            continue

        _sync_stream(entry, config, output_dir, state)

    _write_state(state)


# ── CLI ──────────────────────────────────────────────────────────────────────


def cli() -> None:
    parser = argparse.ArgumentParser(
        prog="tap-mysql-arrow",
        description="Singer tap for MySQL/MariaDB emitting Arrow BATCH messages via ADBC",
    )
    parser.add_argument("--config", required=True, type=Path, help="Config JSON file")
    parser.add_argument("--catalog", type=Path, help="Singer catalog JSON file")
    parser.add_argument("--discover", action="store_true", help="Run discovery and exit")
    parser.add_argument("--state", type=Path, help="State JSON file")
    args = parser.parse_args()

    config: dict[str, Any] = json.loads(args.config.read_text())

    if args.discover:
        with connect(config) as conn:
            catalog = discover_catalog(conn, config)
        json.dump(catalog, sys.stdout, indent=2)
        sys.stdout.write("\n")
        return

    catalog: dict = json.loads(args.catalog.read_text()) if args.catalog else {"streams": []}
    state: dict = json.loads(args.state.read_text()) if args.state else {}

    sync(config, catalog, state)
