"""MySQL ADBC connection with optional SSH tunnel."""

from __future__ import annotations

import contextlib
from typing import Any
from urllib.parse import quote_plus

from adbc_driver_manager import dbapi as adbc_dbapi


def _build_uri(host: str, port: int, user: str, password: str, database: str | None) -> str:
    encoded_pass = quote_plus(password)
    db_part = f"/{database}" if database else "/"
    return f"mysql://{user}:{encoded_pass}@{host}:{port}{db_part}"


def _build_db_kwargs(config: dict[str, Any]) -> dict[str, str]:
    kwargs: dict[str, str] = {}
    if config.get("ssl_ca"):
        kwargs["adbc.mysql.connect_string.tls_ca"] = config["ssl_ca"]
    if config.get("ssl_cert"):
        kwargs["adbc.mysql.connect_string.tls_cert"] = config["ssl_cert"]
    if config.get("ssl_key"):
        kwargs["adbc.mysql.connect_string.tls_key"] = config["ssl_key"]
    return kwargs


@contextlib.contextmanager
def _tunnel_and_kwargs(config: dict[str, Any]):
    """Yield db_kwargs dict, starting an SSH tunnel when configured."""
    tunnel = None
    connect_host = config["host"]
    connect_port = int(config.get("port", 3306))

    if config.get("ssh_host"):
        from sshtunnel import SSHTunnelForwarder

        ssh_kwargs: dict[str, Any] = {
            "ssh_address_or_host": (
                config["ssh_host"],
                int(config.get("ssh_port", 22)),
            ),
            "remote_bind_address": (config["host"], connect_port),
        }
        if config.get("ssh_username"):
            ssh_kwargs["ssh_username"] = config["ssh_username"]
        if config.get("ssh_private_key"):
            ssh_kwargs["ssh_pkey"] = config["ssh_private_key"]
            if config.get("ssh_private_key_password"):
                ssh_kwargs["ssh_private_key_password"] = config["ssh_private_key_password"]
        elif config.get("ssh_password"):
            ssh_kwargs["ssh_password"] = config["ssh_password"]

        tunnel = SSHTunnelForwarder(**ssh_kwargs)
        tunnel.start()
        connect_host = "127.0.0.1"
        connect_port = tunnel.local_bind_port

    db_kwargs = _build_db_kwargs(config)
    db_kwargs["uri"] = _build_uri(
        connect_host,
        connect_port,
        config["user"],
        config["password"],
        config.get("database"),
    )

    try:
        yield db_kwargs
    finally:
        if tunnel is not None:
            tunnel.stop()


@contextlib.contextmanager
def connect(config: dict[str, Any]):
    """Yield a DBAPI connection (used for small queries like discovery)."""
    with _tunnel_and_kwargs(config) as db_kwargs:
        conn = adbc_dbapi.connect(driver="mysql", db_kwargs=db_kwargs)
        try:
            yield conn
        finally:
            conn.close()


@contextlib.contextmanager
def stream_batches(config: dict[str, Any], db_name: str, table: str):
    """Yield a streaming Arrow RecordBatchReader for a table.

    Uses cursor.fetch_record_batch() so MySQL streams record batches to the
    caller as they arrive, avoiding loading the whole table into memory first.
    """
    with _tunnel_and_kwargs(config) as db_kwargs:
        conn = adbc_dbapi.connect(driver="mysql", db_kwargs=db_kwargs)
        try:
            with conn.cursor() as cur:
                qualified = f"`{db_name}`.`{table}`" if db_name else f"`{table}`"
                cur.execute(f"SELECT * FROM {qualified}")
                yield cur.fetch_record_batch()
        finally:
            conn.close()
