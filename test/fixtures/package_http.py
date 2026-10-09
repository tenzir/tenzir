"""Serve repository archives, including GitLab-style wrapper directories.

The archives themselves live under `test/inputs/packages` and are regenerated
by `tools/generate_package_sources.py`. Diagnostics name the package source
verbatim, so tests that read one from disk reach it through `TENZIR_INPUTS`,
which keeps the path reproducible. Checking the archives in also means the
fixture never writes below the project root, which CI mounts read-only.
"""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from urllib.parse import urlsplit

from tenzir_test import fixture

# Deliberately not resolved: CI reaches the project root through a symlink, and
# resolving here would only matter for reading bytes anyway. Tests name these
# files through `TENZIR_INPUTS`.
SOURCES = Path(__file__).parent.parent / "inputs" / "packages"

# URL paths that serve an archive under a different name than its file.
ALIASES = {
    "/api/v4/projects/group%2Frepo/repository/archive": "archive.tar.gz",
}


def archive_responses():
    """Return the URL path to body mapping that the fixture serves."""
    responses = {
        f"/{path.name}": path.read_bytes()
        for path in sorted(SOURCES.iterdir())
        if path.is_file()
    }
    for url, name in ALIASES.items():
        responses[url] = responses[f"/{name}"]
    return responses


@fixture(name="package_http")
def run():
    responses = archive_responses()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_GET(self):
            path = urlsplit(self.path).path
            if path == "/redirect":
                body = b"<html>Moved</html>"
                self.send_response(302)
                self.send_header("Location", "/archive.tar.gz")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            body = responses.get(path)
            if body is None:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield {"PACKAGE_HTTP_URL": f"http://127.0.0.1:{server.server_port}"}
    finally:
        server.shutdown()
        thread.join()
        server.server_close()
