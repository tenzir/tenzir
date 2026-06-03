# runner: python
"""Verify that a failed split request stops the remaining split sends."""

from __future__ import annotations

import os
import shlex
import shutil
import subprocess
import tempfile
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


def _resolve_tenzir_binary() -> tuple[str, ...]:
    env_val = os.environ.get("TENZIR_BINARY")
    if env_val:
        return tuple(shlex.split(env_val))
    which_result = shutil.which("tenzir")
    if which_result:
        return (which_result,)
    raise RuntimeError("tenzir executable not found (set TENZIR_BINARY or add to PATH)")


def main() -> None:
    requests: list[bytes] = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            length = int(self.headers.get("Content-Length", "0"))
            requests.append(self.rfile.read(length))
            self.send_response(HTTPStatus.BAD_REQUEST)
            body = b"bad first split"
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_: object) -> None:
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    try:
        port = server.server_address[1]
        pipeline = f"""
from {{
  metric: "split_order_metric",
  value: 4,
  timestamp: 2026-05-15T10:00:09Z,
}}, {{
  metric: "split_order_metric",
  value: 1,
  timestamp: 2026-05-15T10:00:06Z,
}}, {{
  metric: "split_order_metric",
  value: 3,
  timestamp: 2026-05-15T10:00:08Z,
}}, {{
  metric: "split_order_metric",
  value: 2,
  timestamp: 2026-05-15T10:00:07Z,
}}
to_prometheus "http://127.0.0.1:{port}/write",
              flush_interval=10s,
              max_uncompressed_bytes=80
"""
        with tempfile.TemporaryDirectory(
            prefix="to-prometheus-split-failure-"
        ) as tmpdir:
            pipe_path = Path(tmpdir) / "pipeline.tql"
            pipe_path.write_text(pipeline, encoding="utf-8")
            result = subprocess.run(
                [*_resolve_tenzir_binary(), "-f", str(pipe_path)],
                capture_output=True,
                text=True,
                timeout=60,
            )
        combined = result.stdout + "\n" + result.stderr
        assert result.returncode != 0, "pipeline should fail on the HTTP 400 response"
        assert "HTTP request returned status 400" in combined, combined
        assert "Cancelled" not in combined, combined
        assert len(requests) == 1, f"expected one failed send, got {len(requests)}"
        print("ok")
    finally:
        server.shutdown()
        worker.join()
        server.server_close()


if __name__ == "__main__":
    main()
