"""Minimal mock S3 fixture for testing stream close behavior.

Provides a bare-bones S3-compatible HTTP server that tracks multipart upload
lifecycle.  Arrow's S3FileSystem uses multipart uploads for OutputStream:
  - InitiateMultipartUpload (POST ?uploads)
  - UploadPart (PUT ?partNumber&uploadId)
  - CompleteMultipartUpload (POST ?uploadId)

If Close() is never called, CompleteMultipartUpload is never sent and the
object does not appear in the bucket.  The fixture's assert_test hook verifies
that every initiated upload was completed.

Environment variables yielded:
- MOCK_S3_ENDPOINT: http://127.0.0.1:PORT
- MOCK_S3_BUCKET: bucket name
- MOCK_S3_JOURNAL: path to the JSON journal written at teardown

Assertions payload accepted under ``assertions.fixtures.mock_s3``:
- all_uploads_completed: true  (assert every upload was completed)
"""

from __future__ import annotations

import json
import logging
import os
import re
import tempfile
import threading
import uuid
from dataclasses import dataclass, field
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, parse_qs, unquote

from tenzir_test import FixtureHandle, fixture
from ._utils import find_free_port

logger = logging.getLogger(__name__)

BUCKET = "tenzir-mock"


class _State:
    """Shared mutable state for the mock server."""

    def __init__(self, data_dir: Path) -> None:
        self.data_dir = data_dir
        self.lock = threading.Lock()
        # upload_id → {bucket, key, parts: {partNum: bytes}}
        self.uploads: dict[str, dict[str, Any]] = {}
        self.completed: list[str] = []  # keys that were completed
        self.aborted: list[str] = []


class _Handler(BaseHTTPRequestHandler):
    state: _State

    def log_message(self, format: str, *args: Any) -> None:
        logger.debug("mock_s3: %s", format % args)

    def end_headers(self) -> None:
        self.send_header("x-amz-request-id", uuid.uuid4().hex)
        super().end_headers()

    def handle_one_request(self) -> None:
        """Wrap the base handler so exceptions return 500 instead of killing
        the connection and corrupting Arrow's HTTP client state."""
        try:
            super().handle_one_request()
        except Exception:
            logger.debug("mock_s3: handler exception", exc_info=True)
            try:
                self.send_error(500, "Internal mock error")
            except Exception:
                pass

    def _parse(self) -> tuple[str, str, dict[str, list[str]]]:
        parsed = urlparse(self.path)
        # Path: /bucket/key. S3 transports keys percent-encoded on the wire
        # but stores them as raw bytes; decode so our on-disk layout matches.
        path = parsed.path.lstrip("/")
        slash = path.find("/")
        if slash == -1:
            bucket, key = unquote(path), ""
        else:
            bucket, key = unquote(path[:slash]), unquote(path[slash + 1 :])
        # Preserve blank-value flags like `?uploads` (InitiateMultipartUpload)
        # and `?delete`. Without keep_blank_values, parse_qs drops them, and
        # the mock would mis-route multipart POSTs as regular puts.
        qs = parse_qs(parsed.query, keep_blank_values=True)
        return bucket, key, qs

    def do_HEAD(self) -> None:
        bucket, key, qs = self._parse()
        if not key:
            # Bucket head check
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        # Check if object exists
        obj_path = self.state.data_dir / bucket / key
        if obj_path.exists():
            self.send_response(200)
            self.send_header("Content-Length", str(obj_path.stat().st_size))
            self.end_headers()
        else:
            self.send_response(404)
            self.end_headers()

    def do_GET(self) -> None:
        bucket, key, qs = self._parse()
        if not key:
            # ListBucket - return empty
            self.send_response(200)
            body = f'<?xml version="1.0"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>{bucket}</Name></ListBucketResult>'
            self.send_header("Content-Type", "application/xml")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body.encode())
            return
        obj_path = self.state.data_dir / bucket / key
        if obj_path.exists():
            data = obj_path.read_bytes()
            self.send_response(200)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        else:
            self.send_response(404)
            self.end_headers()

    def _read_body(self) -> bytes:
        if self.headers.get("Expect", "").lower() == "100-continue":
            self.send_response_only(100)
            self.end_headers()
        length = int(self.headers.get("Content-Length", 0))
        return self.rfile.read(length) if length else b""

    def do_PUT(self) -> None:
        bucket, key, qs = self._parse()
        # Preserve trailing slash to detect S3 "directory markers" (Arrow
        # creates these via CreateDir to model a directory — a zero-byte
        # key ending in `/`). Writing them as files would later block
        # children from being created under the same path.
        is_dir_marker = urlparse(self.path).path.endswith("/")
        body = self._read_body()

        if "partNumber" in qs and "uploadId" in qs:
            # UploadPart
            upload_id = qs["uploadId"][0]
            part_num = qs["partNumber"][0]
            with self.state.lock:
                if upload_id in self.state.uploads:
                    self.state.uploads[upload_id]["parts"][part_num] = body
            self.send_response(200)
            self.send_header("ETag", f'"{uuid.uuid4().hex}"')
            self.send_header("Content-Length", "0")
            self.end_headers()
            return

        if is_dir_marker:
            # Create the directory; do not write a file.
            (self.state.data_dir / bucket / key).mkdir(parents=True, exist_ok=True)
            self.send_response(200)
            self.end_headers()
            return

        # Simple PUT (non-multipart single-request upload)
        obj_path = self.state.data_dir / bucket / key
        try:
            obj_path.parent.mkdir(parents=True, exist_ok=True)
            obj_path.write_bytes(body)
        except OSError as exc:
            logger.debug("mock_s3: simple PUT write failed: %s", exc)
            self.send_error(500, str(exc))
            return
        self.send_response(200)
        self.end_headers()

    def do_POST(self) -> None:
        bucket, key, qs = self._parse()
        body = self._read_body()

        if "uploads" in qs:
            # InitiateMultipartUpload
            upload_id = uuid.uuid4().hex
            with self.state.lock:
                self.state.uploads[upload_id] = {
                    "bucket": bucket,
                    "key": key,
                    "parts": {},
                }
            resp = (
                f'<?xml version="1.0"?>'
                f"<InitiateMultipartUploadResult>"
                f"<Bucket>{bucket}</Bucket>"
                f"<Key>{key}</Key>"
                f"<UploadId>{upload_id}</UploadId>"
                f"</InitiateMultipartUploadResult>"
            )
            self.send_response(200)
            self.send_header("Content-Type", "application/xml")
            self.send_header("Content-Length", str(len(resp)))
            self.end_headers()
            self.wfile.write(resp.encode())
            return

        if "uploadId" in qs:
            # CompleteMultipartUpload
            upload_id = qs["uploadId"][0]
            with self.state.lock:
                upload = self.state.uploads.pop(upload_id, None)
            if upload:
                # Assemble parts in order and write the object
                parts_sorted = sorted(upload["parts"].items(), key=lambda x: int(x[0]))
                assembled = b"".join(data for _, data in parts_sorted)
                obj_path = self.state.data_dir / upload["bucket"] / upload["key"]
                try:
                    obj_path.parent.mkdir(parents=True, exist_ok=True)
                except (FileExistsError, NotADirectoryError):
                    pass
                obj_path.write_bytes(assembled)
                with self.state.lock:
                    self.state.completed.append(upload["key"])
                resp = (
                    f'<?xml version="1.0"?>'
                    f"<CompleteMultipartUploadResult>"
                    f"<Location>http://localhost/{upload['bucket']}/{upload['key']}</Location>"
                    f"<Bucket>{upload['bucket']}</Bucket>"
                    f"<Key>{upload['key']}</Key>"
                    f"</CompleteMultipartUploadResult>"
                )
                self.send_response(200)
                self.send_header("Content-Type", "application/xml")
                self.send_header("Content-Length", str(len(resp)))
                self.end_headers()
                self.wfile.write(resp.encode())
            else:
                self.send_response(404)
                self.end_headers()
            return

        # CreateBucket or other POST
        self.send_response(200)
        self.end_headers()

    def do_DELETE(self) -> None:
        bucket, key, qs = self._parse()
        if "uploadId" in qs:
            # AbortMultipartUpload
            upload_id = qs["uploadId"][0]
            with self.state.lock:
                upload = self.state.uploads.pop(upload_id, None)
                if upload:
                    self.state.aborted.append(upload["key"])
            self.send_response(204)
            self.end_headers()
            return
        obj_path = self.state.data_dir / bucket / key
        if obj_path.exists():
            obj_path.unlink()
        self.send_response(204)
        self.end_headers()


@dataclass(frozen=True)
class MockS3Assertions:
    all_uploads_completed: bool = True


@fixture(assertions=MockS3Assertions)
def mock_s3() -> FixtureHandle:
    port = find_free_port()
    data_dir = Path(tempfile.mkdtemp(prefix="tenzir-mock-s3-data-"))
    journal_path = data_dir / "_journal.json"
    state = _State(data_dir)

    # Create the bucket directory
    (data_dir / BUCKET).mkdir()

    handler_class = type("Handler", (_Handler,), {"state": state})
    server = HTTPServer(("127.0.0.1", port), handler_class)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logger.info("mock_s3 listening on 127.0.0.1:%d", port)

    def _assert_test(
        *,
        test: Path,
        assertions: MockS3Assertions | dict[str, Any],
        **_: Any,
    ) -> None:
        if isinstance(assertions, dict):
            assertions = MockS3Assertions(**assertions)
        # Write journal for debugging
        with state.lock:
            journal = {
                "completed": list(state.completed),
                "aborted": list(state.aborted),
                "pending": {uid: info["key"] for uid, info in state.uploads.items()},
            }
        journal_path.write_text(json.dumps(journal, indent=2))
        if assertions.all_uploads_completed:
            with state.lock:
                pending = {uid: info["key"] for uid, info in state.uploads.items()}
                aborted = list(state.aborted)
            problems = []
            if pending:
                problems.append(
                    f"{len(pending)} upload(s) still pending (never completed): "
                    + ", ".join(sorted(pending.values()))
                )
            if aborted:
                problems.append(
                    f"{len(aborted)} upload(s) aborted (Close() not called): "
                    + ", ".join(sorted(aborted))
                )
            if problems:
                raise AssertionError(
                    f"{test.name}: stream close verification failed:\n"
                    + "\n".join(f"  - {p}" for p in problems)
                )

    def _teardown() -> None:
        server.shutdown()
        thread.join(timeout=5)
        import shutil

        shutil.rmtree(data_dir, ignore_errors=True)
        logger.info("mock_s3 shut down")

    return FixtureHandle(
        env={
            "MOCK_S3_ENDPOINT": f"127.0.0.1:{port}",
            "MOCK_S3_BUCKET": BUCKET,
            "MOCK_S3_JOURNAL": str(journal_path),
        },
        teardown=_teardown,
        hooks={"assert_test": _assert_test},
    )
