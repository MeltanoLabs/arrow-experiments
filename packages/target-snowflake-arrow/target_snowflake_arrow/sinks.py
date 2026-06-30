"""Snowflake Arrow sink: loads Arrow IPC BATCH files via ADBC adbc_ingest."""

from __future__ import annotations

import os
from typing import Any, Sequence

import pyarrow as pa
import pyarrow.ipc as ipc
from singer_sdk import metrics as sdk_metrics
from singer_sdk.sinks import Sink


def _coerce_types(table: pa.Table) -> pa.Table:
    """Cast Arrow types unsupported by the Snowflake ADBC driver to equivalents.

    The MySQL ADBC driver emits Decimal32/Decimal64 types for DECIMAL columns.
    Snowflake ADBC only understands Decimal128, so we upcast them here.
    """
    new_columns = []
    new_fields = []
    for i, field in enumerate(table.schema):
        col = table.column(i)
        if pa.types.is_decimal(field.type) and field.type.bit_width < 128:
            target = pa.decimal128(field.type.precision, field.type.scale)
            col = col.cast(target)
            field = field.with_type(target)
        new_columns.append(col)
        new_fields.append(field)
    return pa.table(new_columns, schema=pa.schema(new_fields))


class SnowflakeArrowSink(Sink):
    """Accepts Arrow BATCH messages and loads them into Snowflake via ADBC."""

    # Disable the SDK's built-in record buffering; we handle batches ourselves.
    max_size = 0

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._conn = None
        self._initialized = False  # True after first adbc_ingest for this stream

    # ── Connection lifecycle ──────────────────────────────────────────────────

    def _get_connection(self):
        if self._conn is None:
            self._conn = _build_connection(self.config)
        return self._conn

    def clean_up(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    # ── Table name resolution ─────────────────────────────────────────────────

    @property
    def _table_name(self) -> str:
        """Derive Snowflake table name from the stream name."""
        name = self.stream_name
        # stream names from the tap are "db-table"; use only the table part
        if "-" in name:
            name = name.split("-", 1)[1]
        return name.lower().replace("-", "_").replace(".", "_")

    @property
    def _schema_name(self) -> str | None:
        return self.config.get("schema") or None

    @property
    def _database_name(self) -> str | None:
        return self.config.get("database") or None

    # ── Singer SDK hooks ──────────────────────────────────────────────────────

    def process_record(self, _record: dict, _context: dict) -> None:
        raise NotImplementedError(
            "target-snowflake-arrow only handles Arrow BATCH messages. "
            "Connect it after tap-mysql-arrow or mapper-fivetran-arrow."
        )

    def process_batch(self, _context: dict) -> None:
        pass

    def process_batch_files(
        self,
        _encoding: Any,
        files: Sequence[str],
    ) -> None:
        """Load each Arrow IPC file from the BATCH manifest into Snowflake."""
        for file_uri in files:
            file_path = file_uri.removeprefix("file://")
            with ipc.open_file(file_path) as reader:
                table: pa.Table = reader.read_all()
            os.remove(file_path)

            if table.num_rows == 0:
                continue

            table = _coerce_types(table)
            self._ingest(table)

    def _ingest(self, table: pa.Table, max_retries: int = 3) -> None:
        mode = "replace" if not self._initialized else "append"
        for attempt in range(1, max_retries + 1):
            try:
                conn = self._get_connection()
                with conn.cursor() as cur:
                    cur.adbc_ingest(
                        self._table_name,
                        table,
                        mode=mode,
                        db_schema_name=self._schema_name,
                        catalog_name=self._database_name,
                    )
                    conn.commit()
                break
            except Exception as exc:
                self.logger.warning("adbc_ingest attempt %d failed: %s", attempt, exc)
                self.clean_up()  # drop the connection so _get_connection rebuilds it
                if attempt == max_retries:
                    raise

        self._initialized = True
        self.logger.info(
            "Loaded %d rows into %s (mode=%s)",
            table.num_rows,
            self._table_name,
            mode,
        )
        sdk_metrics.log(
            self.logger,
            sdk_metrics.Point(
                "counter",
                sdk_metrics.Metric.RECORD_COUNT,
                table.num_rows,
                {"stream": self.stream_name, "table": self._table_name},
            ),
        )


# ── Snowflake ADBC connection builder ─────────────────────────────────────────


def _build_connection(config: dict[str, Any]):
    """Open and return a Snowflake ADBC connection."""
    from adbc_driver_manager import dbapi as adbc_dbapi

    account: str = config["account"]
    user: str = config["user"]
    database: str = config.get("database", "")
    schema: str = config.get("schema", "PUBLIC")
    warehouse: str = config.get("warehouse", "")
    role: str = config.get("role", "")

    db_kwargs: dict[str, str] = {
        "username": user,
        "adbc.snowflake.sql.account": account,
    }
    if database:
        db_kwargs["adbc.snowflake.sql.db"] = database
    if schema:
        db_kwargs["adbc.snowflake.sql.schema"] = schema
    if warehouse:
        db_kwargs["adbc.snowflake.sql.warehouse"] = warehouse
    if role:
        db_kwargs["adbc.snowflake.sql.role"] = role

    # Authentication
    private_key_path = config.get("private_key_path")
    private_key_pem = config.get("private_key")
    passphrase = config.get("private_key_passphrase")

    if private_key_path or private_key_pem:
        db_kwargs["adbc.snowflake.sql.auth_type"] = "auth_jwt"
        # Resolve key bytes: file path > inline PEM > base64-encoded PEM/DER
        if private_key_path:
            key_bytes = open(private_key_path, "rb").read()
        elif private_key_pem.lstrip().startswith("-----BEGIN"):  # type: ignore[union-attr]
            key_bytes = private_key_pem.encode()  # type: ignore[union-attr]
        else:
            import base64 as _b64
            key_bytes = _b64.b64decode(private_key_pem)  # type: ignore[arg-type]
        # Load via cryptography and re-serialize as unencrypted PKCS8 PEM,
        # then write to a temp file for the ADBC driver.
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.serialization import Encoding, NoEncryption, PrivateFormat
        import tempfile
        passphrase_bytes = passphrase.encode() if passphrase else None
        p_key = serialization.load_pem_private_key(key_bytes, password=passphrase_bytes)
        pem = p_key.private_bytes(Encoding.PEM, PrivateFormat.PKCS8, NoEncryption())
        tmp = tempfile.NamedTemporaryFile(suffix=".p8", delete=False, mode="wb")
        tmp.write(pem)
        tmp.close()
        db_kwargs["adbc.snowflake.sql.client_option.jwt_private_key"] = tmp.name
    elif config.get("use_browser_authentication"):
        db_kwargs["adbc.snowflake.sql.auth_type"] = "auth_ext_browser"
    else:
        db_kwargs["adbc.snowflake.sql.auth_type"] = "auth_snowflake"
        db_kwargs["password"] = config.get("password", "")

    return adbc_dbapi.connect(driver="snowflake", db_kwargs=db_kwargs)
