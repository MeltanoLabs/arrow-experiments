"""Fivetran-style Arrow BATCH mapper.

Applies the same field-normalization, _fivetran_id, _fivetran_synced and
_fivetran_deleted transforms as mapper-fivetran, but operates on Arrow IPC
BATCH files instead of RECORD messages.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from typing import Any, Generator

import humps
import pyarrow as pa
import pyarrow.ipc as ipc
from singer_sdk import InlineMapper
from singer_sdk.singerlib import SchemaMessage, StateMessage


# ── Field name normalization (mirrors mapper-fivetran logic) ─────────────────


def _normalize_name(name: str) -> str:
    """Convert any casing convention to snake_case."""
    parts = name.split("_")
    result: list[str] = []
    for part in parts:
        if not part:
            continue
        if part.isupper():
            result.append(part.lower())
        else:
            result.append(humps.decamelize(humps.camelize(part)))
    return "_".join(result).replace(".", "_")


# ── Arrow-level transforms ────────────────────────────────────────────────────


def _find_column(table: pa.Table, name_lower: str) -> int | None:
    """Case-insensitive column lookup; returns index or None."""
    for i, field in enumerate(table.schema):
        if field.name.lower() == name_lower:
            return i
    return None


def _compute_fivetran_ids(table: pa.Table) -> pa.Array:
    """Compute MD5 of each row (all columns serialised as JSON)."""
    ids: list[str] = []
    col_names = table.schema.names
    for row_idx in range(table.num_rows):
        row = {col_names[col_idx]: table.column(col_idx)[row_idx].as_py() for col_idx in range(table.num_columns)}
        digest = hashlib.md5(json.dumps(row, default=str, sort_keys=True).encode()).hexdigest()
        ids.append(digest)
    return pa.array(ids, type=pa.string())


def _transform_table(
    table: pa.Table,
    needs_fivetran_id: bool,
) -> pa.Table:
    """Apply Fivetran transforms to a PyArrow Table."""
    # 1. Rename all columns
    new_names = [_normalize_name(n) for n in table.schema.names]
    table = table.rename_columns(new_names)

    # 2. _fivetran_id — only when the stream had no key properties
    if needs_fivetran_id:
        table = table.append_column(
            pa.field("_fivetran_id", pa.string()),
            _compute_fivetran_ids(table),
        )

    # 3. _fivetran_synced — copy from _sdc_extracted_at if present
    sdc_idx = _find_column(table, "_sdc_extracted_at")
    if sdc_idx is not None:
        synced_col: pa.Array = table.column(sdc_idx).cast(pa.string())
    else:
        now = datetime.now(timezone.utc).isoformat()
        synced_col = pa.array([now] * table.num_rows, type=pa.string())
    table = table.append_column(pa.field("_fivetran_synced", pa.string()), synced_col)

    # 4. _fivetran_deleted — True when _sdc_deleted_at is non-null/non-empty
    del_idx = _find_column(table, "_sdc_deleted_at")
    if del_idx is not None:
        raw = table.column(del_idx)
        deleted_vals = [bool(v.as_py()) for v in raw]
    else:
        deleted_vals = [False] * table.num_rows
    table = table.append_column(
        pa.field("_fivetran_deleted", pa.bool_()),
        pa.array(deleted_vals, type=pa.bool_()),
    )

    return table


def _transform_json_schema(schema: dict, needs_fivetran_id: bool) -> dict:
    """Return a transformed copy of a JSON Schema properties dict."""
    props: dict = {}
    for field_name, field_schema in schema.get("properties", {}).items():
        props[_normalize_name(field_name)] = field_schema

    if needs_fivetran_id:
        props["_fivetran_id"] = {"type": ["null", "string"]}

    props["_fivetran_synced"] = {"type": ["null", "string"], "format": "date-time"}
    props["_fivetran_deleted"] = {"type": ["null", "boolean"]}

    return {**schema, "properties": props}


def _transform_key_properties(key_properties: list[str], needs_fivetran_id: bool) -> list[str]:
    if needs_fivetran_id:
        return ["_fivetran_id"]
    return [_normalize_name(k) for k in key_properties]


class _RawMessage:
    """Wraps a plain dict so the SDK writer can call .to_dict() on it."""

    def __init__(self, d: dict) -> None:
        self._d = d

    def to_dict(self) -> dict:
        return self._d


class _BatchMessage:
    """Minimal Singer BATCH message wrapper (singerlib has no BatchMessage class)."""

    def __init__(self, stream: str, manifest: list[str]) -> None:
        self._d = {
            "type": "BATCH",
            "stream": stream,
            "encoding": {"format": "arrow"},
            "manifest": manifest,
        }

    def to_dict(self) -> dict:
        return self._d


# ── Mapper class ─────────────────────────────────────────────────────────────


class FivetranArrowMapper(InlineMapper):
    """InlineMapper that transforms Arrow BATCH files and Singer SCHEMA messages."""

    name = "mapper-fivetran-arrow"

    config_jsonschema = {
        "type": "object",
        "properties": {
            "batch_root_dir": {
                "type": "string",
                "description": "Directory for transformed Arrow IPC output files",
            },
        },
        "additionalProperties": False,
    }

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        # Tracks whether each stream had no key properties (needs _fivetran_id)
        self._needs_fivetran_id: dict[str, bool] = {}
        # Tracks key_properties per stream from received SCHEMA messages
        self._key_properties: dict[str, list[str]] = {}

    @property
    def _output_dir(self) -> str:
        d = self.config.get("batch_root_dir") or tempfile.mkdtemp(prefix="mapper-fivetran-arrow-")
        os.makedirs(d, exist_ok=True)
        return d

    # ── SCHEMA ────────────────────────────────────────────────────────────────

    def map_schema_message(self, message_dict: dict) -> Generator:
        stream = message_dict["stream"]
        key_props: list[str] = message_dict.get("key_properties", [])
        self._key_properties[stream] = key_props
        needs_id = len(key_props) == 0
        self._needs_fivetran_id[stream] = needs_id

        new_schema = _transform_json_schema(message_dict["schema"], needs_id)
        new_keys = _transform_key_properties(key_props, needs_id)

        yield SchemaMessage(
            stream=stream,
            schema=new_schema,
            key_properties=new_keys,
            bookmark_properties=message_dict.get("bookmark_properties"),
        )

    # ── BATCH ─────────────────────────────────────────────────────────────────

    def map_batch_message(self, message_dict: dict) -> Generator:
        stream: str = message_dict["stream"]
        encoding: dict = message_dict.get("encoding", {})
        manifest: list[str] = message_dict.get("manifest", [])

        if encoding.get("format") != "arrow":
            raise ValueError(
                f"mapper-fivetran-arrow only handles 'arrow' BATCH encoding, got '{encoding.get('format')}'"
            )

        needs_id = self._needs_fivetran_id.get(stream, True)
        output_dir = self._output_dir
        new_manifest: list[str] = []

        for i, file_uri in enumerate(manifest):
            src_path = file_uri.removeprefix("file://")
            with ipc.open_file(src_path) as reader:
                table = reader.read_all()

            transformed = _transform_table(table, needs_id)

            out_path = os.path.join(output_dir, f"{stream}_mapped_{i}.arrow")
            with ipc.new_file(out_path, transformed.schema) as writer:
                for batch in transformed.to_batches():
                    writer.write_batch(batch)

            new_manifest.append(f"file://{out_path}")

        yield _BatchMessage(stream=stream, manifest=new_manifest)

    # ── RECORD (passthrough — tap should not be sending these) ────────────────

    def map_record_message(self, message_dict: dict) -> Generator:
        yield _RawMessage(message_dict)

    # ── STATE (passthrough) ───────────────────────────────────────────────────

    def map_state_message(self, message_dict: dict) -> Generator:
        yield StateMessage(value=message_dict["value"])

    # ── ACTIVATE_VERSION (passthrough) ────────────────────────────────────────

    def map_activate_version_message(self, message_dict: dict) -> Generator:
        yield _RawMessage(message_dict)
