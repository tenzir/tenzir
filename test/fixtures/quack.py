"""Loopback Quack server using the bundled DuckDB CLI, without downloads.

QUACK_URI and QUACK_TOKEN connect to the server. QUACK_TLS_URI and QUACK_CA
provide a TLS-terminating proxy when requested. All state and the CLI's home
are temporary. The query hook verifies server state independently of Tenzir.
"""

from __future__ import annotations

import http.client
import json
import os
import queue
import shutil
import ssl
import subprocess
import tempfile
import threading
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from tenzir_test import FixtureHandle, current_options, fixture
from tenzir_test.fixtures import FixtureUnavailable

from ._utils import find_free_port, generate_self_signed_cert


@dataclass(frozen=True)
class QuackOptions:
    tls: bool = False
    extra_sql: str = ""


@dataclass(frozen=True)
class QuackAssertions:
    sql: str = ""
    rows: list[dict[str, Any]] = field(default_factory=list)


def quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


@fixture(options=QuackOptions, assertions=QuackAssertions)
def quack() -> FixtureHandle:
    cli = shutil.which("duckdb")
    if cli is None:
        raise FixtureUnavailable("The quack fixture requires the DuckDB CLI.")
    opts = current_options("quack")
    if isinstance(opts, dict):
        opts = QuackOptions(**opts)
    if opts.tls and shutil.which("openssl") is None:
        raise FixtureUnavailable("The quack TLS fixture requires openssl.")
    directory = tempfile.TemporaryDirectory(prefix="tenzir-test-quack-")
    root = Path(directory.name).resolve()
    (root / "server-only.json").write_text('{"source":"server","n":42}\n')
    env = {**os.environ, "HOME": str(root)}
    port = find_free_port()
    uri = f"quack:127.0.0.1:{port}"
    # Include a quote to exercise SQL literal escaping in secret creation.
    token = "tenzir-test-quack's-token"
    process = None
    proxy = None
    proxy_thread = None
    reader = None

    def teardown() -> None:
        if proxy is not None:
            proxy.shutdown()
            proxy.server_close()
        if proxy_thread is not None:
            proxy_thread.join(timeout=5)
        if process is not None:
            if process.stdin is not None:
                process.stdin.close()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
            if process.stdout is not None:
                process.stdout.close()
        if reader is not None:
            reader.join(timeout=5)
        directory.cleanup()

    try:
        # Fail loudly if the installed CLI lacks the production extensions.
        subprocess.run(
            [cli, "-batch", "-bail", "-c", "LOAD httpfs; LOAD quack;"],
            env=env,
            check=True,
            capture_output=True,
            text=True,
            timeout=15,
        )
        process = subprocess.Popen(
            [cli, "-batch", "-bail", "-csv", "-noheader", str(root / "server.duckdb")],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
            cwd=root,
        )
        lines: queue.Queue[str | None] = queue.Queue()

        def read_output() -> None:
            assert process is not None and process.stdout is not None
            for line in process.stdout:
                lines.put(line.rstrip())
            lines.put(None)

        reader = threading.Thread(target=read_output, daemon=True)
        reader.start()
        assert process.stdin is not None
        process.stdin.write(
            "LOAD httpfs; LOAD quack;\n"
            "SET autoinstall_known_extensions=false; SET threads=2;\n"
            "CREATE TABLE events (id BIGINT PRIMARY KEY, name VARCHAR, "
            "note VARCHAR DEFAULT 'default');\n"
            "INSERT INTO events VALUES (1, 'one', 'seed'), (2, NULL, 'seed');\n"
            "CREATE TABLE live_events (id BIGINT PRIMARY KEY, name VARCHAR);\n"
            "INSERT INTO live_events VALUES (1, 'seed'), (2, 'seed');\n"
            "CREATE TABLE many AS SELECT range AS n FROM range(50000);\n"
            f"{opts.extra_sql}\n"
            f"CALL quack_serve({quote(uri)}, token={quote(token)});\n"
            "SELECT 'TENZIR_QUACK_READY';\n"
        )
        process.stdin.flush()
        startup = []
        while True:
            line = lines.get(timeout=20)
            if line == "TENZIR_QUACK_READY":
                break
            if line is None:
                raise RuntimeError(f"Quack server failed to start: {startup}")
            startup.append(line)
        exposed = {"QUACK_URI": uri, "QUACK_TOKEN": token}
        if opts.tls:
            cert, key, ca, _ = generate_self_signed_cert(
                root, common_name="localhost", san_entries=["DNS:localhost"]
            )
            untrusted_root = root / "untrusted"
            untrusted_root.mkdir()
            _, _, untrusted_ca, _ = generate_self_signed_cert(untrusted_root)

            class Handler(BaseHTTPRequestHandler):
                def handle(self) -> None:
                    try:
                        super().handle()
                    except ConnectionError:
                        # Clients rejecting the certificate may close early.
                        pass

                def do_POST(self) -> None:
                    body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                    connection = http.client.HTTPConnection(
                        "127.0.0.1", port, timeout=15
                    )
                    try:
                        connection.request("POST", self.path, body, dict(self.headers))
                        response = connection.getresponse()
                        content = response.read()
                        self.send_response(response.status)
                        self.send_header("Content-Length", str(len(content)))
                        self.end_headers()
                        self.wfile.write(content)
                    finally:
                        connection.close()

                def log_message(self, *_: object) -> None:
                    pass

            proxy = ThreadingHTTPServer(("127.0.0.1", find_free_port()), Handler)
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.load_cert_chain(cert, key)
            proxy.socket = context.wrap_socket(proxy.socket, server_side=True)
            proxy_thread = threading.Thread(target=proxy.serve_forever, daemon=True)
            proxy_thread.start()
            exposed.update(
                QUACK_TLS_URI=f"quack://localhost:{proxy.server_port}",
                QUACK_CA=str(ca),
                QUACK_UNTRUSTED_CA=str(untrusted_ca),
            )

        def query(sql: str) -> list[dict[str, Any]]:
            result = subprocess.run(
                [
                    cli,
                    "-batch",
                    "-bail",
                    "-json",
                    "-c",
                    "LOAD httpfs; LOAD quack; "
                    f"CREATE TEMPORARY SECRET (TYPE quack, TOKEN {quote(token)}, "
                    f"SCOPE {quote(uri)}); ATTACH {quote(uri)} AS remote; "
                    f"SELECT * FROM remote.query({quote(sql)});",
                ],
                check=True,
                capture_output=True,
                text=True,
                env=env,
                timeout=20,
            )
            # CREATE SECRET has a result of its own; the query is last.
            remaining = result.stdout.strip()
            rows = []
            decoder = json.JSONDecoder()
            while remaining:
                rows, consumed = decoder.raw_decode(remaining)
                remaining = remaining[consumed:].lstrip()
            return rows

        def assert_test(
            *, assertions: QuackAssertions | dict[str, Any], **_: Any
        ) -> None:
            if isinstance(assertions, dict):
                assertions = QuackAssertions(**assertions)
            if assertions.sql:
                observed = query(assertions.sql)
                assert observed == assertions.rows, (observed, assertions.rows)

        return FixtureHandle(
            env=exposed,
            teardown=teardown,
            hooks={"query": query, "assert_test": assert_test},
        )
    except BaseException:
        teardown()
        raise
