"""MySQL ADBC connection with optional SSH tunnel."""

from __future__ import annotations

import contextlib
import io
import socket
import threading
from contextlib import suppress
from typing import Any
from urllib.parse import quote_plus

import paramiko
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


def _load_private_key(key_data: str, passphrase: str | None = None) -> paramiko.PKey:
    """Try each supported key type in turn; raise if none match.

    Note: DSS keys are not supported — they were removed in paramiko 4.0
    due to being cryptographically weak.
    """
    key_file = io.StringIO(key_data)
    for key_class in (paramiko.RSAKey, paramiko.ECDSAKey, paramiko.Ed25519Key):
        with suppress(paramiko.SSHException):
            key_file.seek(0)
            return key_class.from_private_key(key_file, password=passphrase)
    raise ValueError(
        "Could not determine the SSH key type. Supported types: RSA, ECDSA, Ed25519."
    )


class _SSHTunnel:
    """Minimal SSH port-forward tunnel built on paramiko (no sshtunnel dependency)."""

    def __init__(
        self,
        ssh_host: str,
        ssh_port: int,
        ssh_username: str,
        remote_host: str,
        remote_port: int,
        ssh_pkey: paramiko.PKey | None = None,
        ssh_password: str | None = None,
    ) -> None:
        self._ssh_host = ssh_host
        self._ssh_port = ssh_port
        self._ssh_username = ssh_username
        self._ssh_pkey = ssh_pkey
        self._ssh_password = ssh_password
        self._remote_host = remote_host
        self._remote_port = remote_port

        self._client: paramiko.SSHClient | None = None
        self._server: socket.socket | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.local_bind_port: int | None = None

    def start(self) -> None:
        self._client = paramiko.SSHClient()
        self._client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self._client.connect(
            hostname=self._ssh_host,
            port=self._ssh_port,
            username=self._ssh_username,
            pkey=self._ssh_pkey,
            password=self._ssh_password,
        )

        self._server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server.bind(("127.0.0.1", 0))
        self._server.listen(5)
        self.local_bind_port = self._server.getsockname()[1]

        self._thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._thread.start()

    def _accept_loop(self) -> None:
        assert self._server is not None
        while not self._stop.is_set():
            self._server.settimeout(1.0)
            try:
                local_sock, _ = self._server.accept()
            except TimeoutError:
                continue
            except OSError:
                if not self._stop.is_set():
                    break
                return
            threading.Thread(target=self._forward, args=(local_sock,), daemon=True).start()

    def _forward(self, local_sock: socket.socket) -> None:
        assert self._client is not None
        transport = self._client.get_transport()
        if transport is None:
            local_sock.close()
            return
        try:
            channel = transport.open_channel(
                "direct-tcpip",
                (self._remote_host, self._remote_port),
                local_sock.getpeername(),
            )
        except Exception:
            local_sock.close()
            return

        def _pipe(src: Any, dst: Any) -> None:
            try:
                while True:
                    data = src.recv(65536)
                    if not data:
                        break
                    dst.send(data)
            except OSError:
                pass
            finally:
                with suppress(OSError):
                    src.close()
                with suppress(OSError):
                    dst.close()

        threading.Thread(target=_pipe, args=(local_sock, channel), daemon=True).start()
        threading.Thread(target=_pipe, args=(channel, local_sock), daemon=True).start()

    def stop(self) -> None:
        self._stop.set()
        with suppress(OSError):
            if self._server:
                self._server.close()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)
        if self._client:
            self._client.close()


@contextlib.contextmanager
def _tunnel_and_kwargs(config: dict[str, Any]):
    """Yield db_kwargs dict, starting an SSH tunnel when configured."""
    tunnel = None
    connect_host = config["host"]
    connect_port = int(config.get("port", 3306))

    if config.get("ssh_host"):
        pkey = None
        if config.get("ssh_private_key"):
            pkey = _load_private_key(
                config["ssh_private_key"],
                passphrase=config.get("ssh_private_key_password"),
            )

        tunnel = _SSHTunnel(
            ssh_host=config["ssh_host"],
            ssh_port=int(config.get("ssh_port", 22)),
            ssh_username=config.get("ssh_username", ""),
            remote_host=config["host"],
            remote_port=connect_port,
            ssh_pkey=pkey,
            ssh_password=config.get("ssh_password"),
        )
        tunnel.start()
        assert tunnel.local_bind_port is not None
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
