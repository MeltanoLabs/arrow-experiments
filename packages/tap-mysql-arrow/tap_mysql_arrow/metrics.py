"""Background thread that emits Singer METRIC messages every 60 seconds."""

from __future__ import annotations

import json
import sys
import threading


class MetricEmitter:
    """Emits record_count metrics to stderr on a fixed interval."""

    def __init__(self, stream_name: str, table: str, interval: float = 60.0) -> None:
        self._stream = stream_name
        self._table = table
        self._interval = interval
        self._count = 0
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def add(self, n: int) -> None:
        with self._lock:
            self._count += n

    def stop(self) -> None:
        """Stop the background thread and emit a final metric."""
        self._stop.set()
        self._thread.join()
        self._flush()

    def _flush(self) -> None:
        with self._lock:
            count = self._count
            self._count = 0
        msg = {
            "type": "METRIC",
            "metric_type": "counter",
            "metric": "record_count",
            "value": count,
            "tags": {"stream": self._stream, "table": self._table},
        }
        sys.stderr.write(json.dumps(msg) + "\n")
        sys.stderr.flush()

    def _loop(self) -> None:
        while not self._stop.wait(self._interval):
            self._flush()
