# runner: python
"""A commit whose success response is lost must not duplicate or lose rows.

A relay between the operator and the REST catalog delivers the first
commit POST upstream and then closes the connection without returning
the response. The server has applied the commit; the client sees a
transport failure and must resolve the unknown outcome by inspecting the
reloaded table instead of blindly retrying (which would duplicate rows)
or dropping the staged files (which would lose them).
"""

import json
import os
import re
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

# /// script
# dependencies = ["pyiceberg[pyarrow]", "requests"]
# ///

import requests
from pyiceberg.catalog.rest import RestCatalog

TABLE = "commitns.uncertain"
COMMIT_PATH = re.compile(r".*/v1/namespaces/[^/]+/tables/[^/]+$")


def main() -> None:
    upstream = os.environ["ICEBERG_REST_URI"]
    state = {"dropped": 0}

    class Relay(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def _relay(self) -> None:
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length) if length else b""
            headers = {
                key: value
                for key, value in self.headers.items()
                if key.lower()
                not in ("host", "content-length", "connection", "accept-encoding")
            }
            response = requests.request(
                self.command,
                upstream + self.path,
                data=body,
                headers=headers,
                timeout=60,
            )
            is_commit = self.command == "POST" and COMMIT_PATH.fullmatch(
                urlparse(self.path).path
            )
            if is_commit and state["dropped"] == 0:
                # The server has applied the commit; vanish without a reply.
                state["dropped"] += 1
                self.close_connection = True
                return
            payload = response.content
            self.send_response(response.status_code)
            for key, value in response.headers.items():
                if key.lower() in (
                    "transfer-encoding",
                    "content-encoding",
                    "content-length",
                    "connection",
                    "keep-alive",
                ):
                    continue
                self.send_header(key, value)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        do_GET = do_POST = do_PUT = do_DELETE = do_HEAD = _relay

        def log_message(self, *args: object) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Relay)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    proxy_uri = f"http://127.0.0.1:{server.server_address[1]}"

    pipeline = f"""
from_stdin {{
  read_ndjson
}}
to_iceberg "{TABLE}", catalog="{proxy_uri}", mode="create_append", max_size=1
"""
    writer = subprocess.Popen(
        ["tenzir", pipeline],
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert writer.stdin is not None
    catalog = RestCatalog("test", uri=upstream)

    def rows() -> list[int]:
        try:
            scanned = catalog.load_table(TABLE).scan().to_arrow().to_pylist()
        except Exception:
            return []
        return sorted(row["id"] for row in scanned)

    def await_rows(expected: list[int]) -> None:
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            if rows() == expected:
                return
            time.sleep(0.5)
        raise RuntimeError(f"table never reached rows {expected}, got {rows()}")

    writer.stdin.write(json.dumps({"id": 1}) + "\n")
    writer.stdin.flush()
    await_rows([1])
    writer.stdin.write(json.dumps({"id": 2}) + "\n")
    _, stderr = writer.communicate(timeout=120)
    print(f"writer exited with {writer.returncode}")
    if writer.returncode != 0:
        raise RuntimeError(stderr)
    server.shutdown()
    print(f"commit responses dropped: {state['dropped']}")
    print(f"rows: {rows()}")


if __name__ == "__main__":
    main()
