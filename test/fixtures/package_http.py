"""Serve repository archives, including GitLab-style wrapper directories."""

import gzip
import io
import tarfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread
from urllib.parse import urlsplit

from tenzir_test import fixture


def make_archive(files, *, compressed=True, extra=None):
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz" if compressed else "w") as archive:
        for name, contents in files.items():
            data = contents.encode()
            entry = tarfile.TarInfo(name)
            entry.size = len(data)
            archive.addfile(entry, io.BytesIO(data))
        if extra is not None:
            archive.addfile(extra)
    return buffer.getvalue()


def archive_responses():
    files = {
        "package.yaml": (
            "id: remote_split\nname: Remote split package\n"
            "inputs:\n  message:\n    name: Message\n    type: string\n"
        ),
        "config.yaml": "inputs:\n  message: configured\n",
        "pipelines/nested/hello.tql": (
            'from {message: "{{ inputs.message }}"}\ndiscard\n'
        ),
        "operators/nested/answer.tql": "from {answer: 42}\n",
        "examples/example.tql": "version\n",
        "constants.tql": "let $answer = 42\n",
        "README.md": "Ignored by the package loader.\n",
    }
    wrapped = {
        f"repo-main-abcdef/packages/example/{name}": body
        for name, body in files.items()
    }
    derived = {
        f"derived_package/{name}": body.removeprefix("id: remote_split\n")
        for name, body in files.items()
    }
    symlink = tarfile.TarInfo("link")
    symlink.type = tarfile.SYMTYPE
    symlink.linkname = "../../outside"
    hardlink = tarfile.TarInfo("hardlink")
    hardlink.type = tarfile.LNKTYPE
    hardlink.linkname = "package.yaml"
    fifo = tarfile.TarInfo("fifo")
    fifo.type = tarfile.FIFOTYPE
    oversized = tarfile.TarInfo("large")
    oversized.size = 65 * 1024 * 1024
    valid = make_archive(wrapped)
    multiple = {
        **{f"packages/example/{name}": body for name, body in files.items()},
        "packages/other/package.yaml": "id: other\nname: Other package\n",
        "README.md": "Repository with multiple packages.\n",
    }
    return {
        "/warning.tar.gz": make_archive(
            {
                "package.yaml": "id: warning-package\nname: Warning package\n",
            }
        ),
        "/warning-error.tar.gz": make_archive(
            {
                "package.yaml": "id: warning-package\nname: Warning package\n",
                "pipelines/bad.tql": "---\nname: [\n---\nversion\n",
            }
        ),
        "/archive.tar.gz": valid,
        "/api/v4/projects/group%2Frepo/repository/archive": valid,
        "/archive.tar": make_archive(files, compressed=False),
        "/single-root.tar.gz": make_archive(files),
        "/multiple.tar.gz": make_archive(multiple),
        "/multiple-wrapped.tar.gz": make_archive(
            {f"repo-main-abcdef/{name}": body for name, body in multiple.items()}
        ),
        "/derived.tgz": make_archive(derived),
        "/traversal.tar.gz": make_archive({"../outside": "unsafe"}),
        "/absolute.tar.gz": make_archive({"/outside": "unsafe"}),
        "/symlink.tar.gz": make_archive(files, extra=symlink),
        "/hardlink.tar.gz": make_archive(files, extra=hardlink),
        "/fifo.tar.gz": make_archive(files, extra=fifo),
        "/oversized.tar.gz": gzip.compress(oversized.tobuf()),
        "/duplicate.tar.gz": make_archive(files, extra=tarfile.TarInfo("package.yaml")),
        "/missing.tar.gz": make_archive({"README.md": "no package"}),
        "/ambiguous.tar.gz": make_archive(
            {
                "a/package.yaml": files["package.yaml"],
                "b/package.yaml": files["package.yaml"],
            }
        ),
        "/corrupt.tar.gz": b"not an archive",
        "/truncated.tar.gz": valid[: len(valid) // 2],
    }


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
        with TemporaryDirectory(prefix="tenzir-package-sources-") as temporary:
            root = Path(temporary)
            for path, body in responses.items():
                (root / Path(path).name).write_bytes(body)
            directory = root / "directory"
            directory.mkdir()
            (directory / "package.yaml").write_text(
                "id: warning-package\nname: Warning package\n"
            )
            yield {
                "PACKAGE_HTTP_URL": f"http://127.0.0.1:{server.server_port}",
                "PACKAGE_LOCAL_DIR": temporary,
            }
    finally:
        server.shutdown()
        thread.join()
        server.server_close()
