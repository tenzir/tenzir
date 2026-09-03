"""Microsoft 365 Management Activity API fixture."""

from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Iterator, cast
from urllib.parse import parse_qs, urlsplit

from tenzir_test import fixture

from ._utils import find_free_port

_HOST = "127.0.0.1"
_TENANT_ID = "11111111-2222-3333-4444-555555555555"
_CLIENT_ID = "activity-client"
_CLIENT_SECRET = "activity-secret"
_ACCESS_TOKEN = "activity-token"
_PUBLISHER_ID = "publisher-tenant"


def _json_response(
    handler: BaseHTTPRequestHandler,
    code: int,
    value: object,
    headers: list[tuple[str, str]] | None = None,
) -> None:
    body = json.dumps(value).encode()
    handler.send_response(code)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(body)))
    for name, header_value in headers or []:
        handler.send_header(name, header_value)
    handler.end_headers()
    handler.wfile.write(body)


def _parse_api_time(value: str) -> datetime:
    """Parse a timestamp in the only formats the Activity API documents."""
    for pattern in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, pattern).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    raise ValueError(f"unsupported Activity API timestamp: {value}")


class _ActivityServer(ThreadingHTTPServer):
    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)
        self.lock = threading.Lock()
        self.requests: dict[str, int] = {}

    def record_request(self, key: str) -> int:
        with self.lock:
            count = self.requests.get(key, 0) + 1
            self.requests[key] = count
            return count


class _TokenHandler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:  # noqa: N802
        if self.path != f"/{_TENANT_ID}/oauth2/v2.0/token":
            _json_response(self, HTTPStatus.NOT_FOUND, {"error": "not found"})
            return
        length = int(self.headers.get("Content-Length", "0"))
        form = parse_qs(self.rfile.read(length).decode(), keep_blank_values=True)
        expected = {
            "client_id": [_CLIENT_ID],
            "client_secret": [_CLIENT_SECRET],
            "grant_type": ["client_credentials"],
            "scope": ["https://manage.office.com/.default"],
        }
        if any(form.get(key) != value for key, value in expected.items()):
            _json_response(self, HTTPStatus.BAD_REQUEST, {"error": "bad token request"})
            return
        _json_response(
            self,
            HTTPStatus.OK,
            {"expires_in": 3600, "access_token": _ACCESS_TOKEN},
        )

    def log_message(self, *_: object) -> None:
        return


class _ActivityHandler(BaseHTTPRequestHandler):
    def _request(self) -> tuple[str, dict[str, list[str]]] | None:
        if self.headers.get("Authorization") != f"Bearer {_ACCESS_TOKEN}":
            _json_response(self, HTTPStatus.UNAUTHORIZED, {"error": "unauthorized"})
            return None
        parts = urlsplit(self.path)
        return parts.path, parse_qs(parts.query, keep_blank_values=True)

    def _check_publisher(self, query: dict[str, list[str]]) -> bool:
        if query.get("PublisherIdentifier") not in (
            [_PUBLISHER_ID],
            ["retry"],
            ["untrusted"],
            ["continuous"],
        ):
            _json_response(self, HTTPStatus.BAD_REQUEST, {"error": "publisher missing"})
            return False
        return True

    def do_GET(self) -> None:  # noqa: N802
        request = self._request()
        if not request:
            return
        path, query = request
        if not self._check_publisher(query):
            return
        root = f"/api/v1.0/{_TENANT_ID}/activity/feed"
        base_url = f"http://{_HOST}:{self.server.server_port}"
        if path == f"{root}/subscriptions/list":
            server = cast(_ActivityServer, self.server)
            if query.get("PublisherIdentifier") == ["retry"]:
                if server.record_request("retry") == 1:
                    _json_response(
                        self,
                        HTTPStatus.TOO_MANY_REQUESTS,
                        {"error": "throttled"},
                        [("Retry-After", "0")],
                    )
                    return
            _json_response(
                self,
                HTTPStatus.OK,
                [{"contentType": "Audit.General", "status": "enabled"}],
            )
            return
        if path == f"{root}/subscriptions/content":
            if not query.get("startTime") or not query.get("endTime"):
                _json_response(self, HTTPStatus.BAD_REQUEST, {"error": "range missing"})
                return
            try:
                start = _parse_api_time(query["startTime"][0])
                end = _parse_api_time(query["endTime"][0])
            except ValueError as error:
                _json_response(
                    self,
                    HTTPStatus.BAD_REQUEST,
                    {"error": "bad time", "detail": str(error)},
                )
                return
            if end <= start or (end - start).total_seconds() > 24 * 60 * 60:
                _json_response(
                    self,
                    HTTPStatus.BAD_REQUEST,
                    {
                        "error": "bad range",
                        "startTime": query["startTime"][0],
                        "endTime": query["endTime"][0],
                    },
                )
                return
            if query.get("PublisherIdentifier") == ["untrusted"]:
                _json_response(
                    self,
                    HTTPStatus.OK,
                    [],
                    [("NextPageUri", "https://example.invalid/next")],
                )
                return
            if query.get("PublisherIdentifier") == ["continuous"]:
                server = cast(_ActivityServer, self.server)
                count = server.record_request("continuous-content")
                seconds = (end - start).total_seconds()
                if (count == 1 and not 60 * 119 <= seconds <= 60 * 121) or (
                    count > 1 and seconds >= 60
                ):
                    _json_response(
                        self,
                        HTTPStatus.BAD_REQUEST,
                        {"error": "unexpected continuous range"},
                    )
                    return
                blobs = [
                    {
                        "contentType": "Audit.General",
                        "contentId": "blob-1",
                        "contentUri": f"{base_url}{root}/audit/blob-1",
                        "contentCreated": "2026-09-02T10:00:00Z",
                        "contentExpiration": "2099-09-09T10:10:00Z",
                    }
                ]
                if count > 1:
                    blobs.append(
                        {
                            "contentType": "Audit.General",
                            "contentId": "blob-2",
                            "contentUri": f"{base_url}{root}/audit/blob-2",
                            "contentCreated": "2026-09-02T10:05:00Z",
                            "contentExpiration": "2099-09-09T10:05:00Z",
                        }
                    )
                _json_response(self, HTTPStatus.OK, blobs)
                return
            if query.get("nextPage") == ["2"]:
                _json_response(
                    self,
                    HTTPStatus.OK,
                    [
                        {
                            "contentType": "Audit.General",
                            "contentId": "blob-2",
                            "contentUri": f"{base_url}{root}/audit/blob-2",
                            "contentCreated": "2026-09-02T10:05:00Z",
                            "contentExpiration": "2099-09-09T10:05:00Z",
                        }
                    ],
                )
                return
            next_url = f"{base_url}{root}/subscriptions/content?"
            next_url += "contentType=Audit.General&startTime="
            next_url += query["startTime"][0]
            next_url += "&endTime=" + query["endTime"][0]
            next_url += "&nextPage=2&PublisherIdentifier="
            next_url += query["PublisherIdentifier"][0]
            _json_response(
                self,
                HTTPStatus.OK,
                [
                    {
                        "contentType": "Audit.General",
                        "contentId": "blob-1",
                        "contentUri": f"{base_url}{root}/audit/blob-1",
                        "contentCreated": "2026-09-02T10:00:00Z",
                        "contentExpiration": "2099-09-09T10:10:00Z",
                    }
                ],
                [("NextPageUri", next_url)],
            )
            return
        if path == f"{root}/audit/blob-1":
            _json_response(
                self,
                HTTPStatus.OK,
                [{"Id": "event-1", "CreationTime": "2026-09-02T09:55:00Z"}],
            )
            return
        if path == f"{root}/audit/blob-2":
            _json_response(
                self,
                HTTPStatus.OK,
                [{"Id": "event-2", "Operation": "UserLoggedIn"}],
            )
            return
        _json_response(
            self, HTTPStatus.NOT_FOUND, {"error": "not found", "path": self.path}
        )

    def do_POST(self) -> None:  # noqa: N802
        request = self._request()
        if not request:
            return
        path, query = request
        if not self._check_publisher(query):
            return
        root = f"/api/v1.0/{_TENANT_ID}/activity/feed"
        if path != f"{root}/subscriptions/start":
            _json_response(self, HTTPStatus.NOT_FOUND, {"error": "not found"})
            return
        if query.get("contentType") not in (["Audit.Exchange"], ["Audit.SharePoint"]):
            _json_response(self, HTTPStatus.BAD_REQUEST, {"error": "bad type"})
            return
        _json_response(
            self,
            HTTPStatus.OK,
            {"contentType": "Audit.Exchange", "status": "enabled"},
        )

    def log_message(self, *_: object) -> None:
        return


def _plugin_env() -> dict[str, str]:
    import os
    from pathlib import Path

    env: dict[str, str] = {}
    if binary := os.environ.get("TENZIR_BINARY"):
        build_dir = Path(binary).resolve().parent.parent
        plugin_dir = Path(
            os.environ.get(
                "MS_ACTIVITY_FIXTURE_PLUGIN_DIR",
                str(build_dir / "lib" / "tenzir" / "plugins"),
            )
        )
        if plugin_dir.is_dir():
            env["TENZIR_PLUGIN_DIRS"] = str(plugin_dir)
            env["TENZIR_PLUGINS"] = "microsoft_graph"
        lib_dir = build_dir / "lib"
        if lib_dir.is_dir():
            env["DYLD_LIBRARY_PATH"] = str(lib_dir)
    return env


@fixture(name="microsoft_365_activity")
def run() -> Iterator[dict[str, str]]:
    token_server = ThreadingHTTPServer((_HOST, 0), _TokenHandler)
    activity_server = _ActivityServer((_HOST, find_free_port()), _ActivityHandler)
    token_thread = threading.Thread(target=token_server.serve_forever, daemon=True)
    activity_thread = threading.Thread(
        target=activity_server.serve_forever, daemon=True
    )
    token_thread.start()
    activity_thread.start()
    try:
        env = {
            "MS_ACTIVITY_FIXTURE_BASE_URL": (
                f"http://{_HOST}:{activity_server.server_port}"
            ),
            "MS_ACTIVITY_FIXTURE_AUTHORITY": (
                f"http://{_HOST}:{token_server.server_port}"
            ),
            "MS_ACTIVITY_FIXTURE_TENANT_ID": _TENANT_ID,
            "MS_ACTIVITY_FIXTURE_CLIENT_ID": _CLIENT_ID,
            "MS_ACTIVITY_FIXTURE_CLIENT_SECRET": _CLIENT_SECRET,
            "MS_ACTIVITY_FIXTURE_PUBLISHER_ID": _PUBLISHER_ID,
        }
        env.update(_plugin_env())
        yield env
    finally:
        token_server.shutdown()
        activity_server.shutdown()
        token_thread.join(timeout=2)
        activity_thread.join(timeout=2)
