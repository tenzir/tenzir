"""Microsoft Graph fixture for integration testing.

The Graph API side runs through Dev Proxy with mocked Microsoft Graph
responses. The auth side emulates the Microsoft identity platform client
credentials token endpoint used by app-only Graph access.

Environment variables yielded:
- HTTPS_PROXY: Dev Proxy HTTP proxy URL.
- NO_PROXY: bypass list for local token-server traffic.
- MS_GRAPH_FIXTURE_TLS_CACERT: Dev Proxy root CA certificate.
- MS_GRAPH_FIXTURE_AUTHORITY: authority URL for auth.authority.
- MS_GRAPH_FIXTURE_TENANT_ID
- MS_GRAPH_FIXTURE_CLIENT_ID
- MS_GRAPH_FIXTURE_CLIENT_SECRET
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import threading
import urllib.error
import urllib.request
import uuid
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Iterator, cast
from urllib.parse import parse_qs, urlsplit

from tenzir_test import fixture
from tenzir_test.fixtures import FixtureUnavailable
from tenzir_test.fixtures.container_runtime import (
    ContainerReadinessTimeout,
    ManagedContainer,
    RuntimeSpec,
    detect_runtime,
    start_detached,
    wait_until_ready,
)

from ._utils import find_free_port

logger = logging.getLogger(__name__)

DEV_PROXY_IMAGE = "ghcr.io/dotnet/dev-proxy:0.26.0"
DEV_PROXY_PORT = 8000
DEV_PROXY_API_PORT = 8897
STARTUP_TIMEOUT = 60
HEALTH_CHECK_INTERVAL = 1

_HOST = "127.0.0.1"
_TENANT_ID = "test-tenant"
_CLIENT_ID = "test-client"
_CLIENT_SECRET = "test-secret"
_ACCESS_TOKEN = "test-ms-graph-token"
_SKIPTOKEN = "page-2"
_FIRST_PAGE_DELAY_SECONDS = 2100


def _json_response(handler: BaseHTTPRequestHandler, code: int, obj: object) -> None:
    body = json.dumps(obj).encode()
    handler.send_response(code)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


class _TokenServer(ThreadingHTTPServer):
    token_requests: int = 0


class _TokenHandler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:  # noqa: N802
        parts = urlsplit(self.path)
        expected_path = f"/{_TENANT_ID}/oauth2/v2.0/token"
        if parts.path != expected_path:
            _json_response(self, HTTPStatus.NOT_FOUND, {"error": "not found"})
            return
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length).decode()
        form = parse_qs(body, keep_blank_values=True)
        expected = {
            "client_id": [_CLIENT_ID],
            "client_secret": [_CLIENT_SECRET],
            "grant_type": ["client_credentials"],
            "scope": ["https://graph.microsoft.com/.default"],
        }
        for key, value in expected.items():
            if form.get(key) != value:
                _json_response(
                    self,
                    HTTPStatus.BAD_REQUEST,
                    {"error": f"unexpected {key}: {form.get(key)!r}"},
                )
                return
        server = cast(_TokenServer, self.server)
        server.token_requests += 1
        if server.token_requests > 1:
            _json_response(
                self,
                HTTPStatus.SERVICE_UNAVAILABLE,
                {"error": "temporary auth outage"},
            )
            return
        _json_response(
            self,
            HTTPStatus.OK,
            {
                "token_type": "Bearer",
                "expires_in": 4,
                "access_token": _ACCESS_TOKEN,
            },
        )

    def log_message(self, *_: object) -> None:
        return


def _mock_headers() -> list[dict[str, str]]:
    return [{"name": "Content-Type", "value": "application/json"}]


def _write_config(config_dir: Path) -> None:
    base = "https://graph.microsoft.com"
    first_page = {
        "@odata.context": f"{base}/v1.0/$metadata#users",
        "@odata.nextLink": f"{base}/v1.0/users?$skiptoken={_SKIPTOKEN}",
        "value": [
            {
                "@odata.etag": 'W/"user-1"',
                "id": "user-1",
                "displayName": "Ada Lovelace",
                "userPrincipalName": "ada@example.com",
                "manager": {
                    "@odata.type": "#microsoft.graph.user",
                    "id": "manager-1",
                },
            },
            {
                "@odata.type": "#microsoft.graph.user",
                "id": "user-2",
                "displayName": "Grace Hopper",
                "userPrincipalName": "grace@example.com",
            },
        ],
    }
    second_page = {
        "@odata.context": f"{base}/v1.0/$metadata#users",
        "value": [
            {
                "@odata.etag": 'W/"user-3"',
                "id": "user-3",
                "displayName": "Katherine Johnson",
                "userPrincipalName": "katherine@example.com",
            }
        ],
    }
    mocks = {
        "$schema": (
            "https://raw.githubusercontent.com/dotnet/dev-proxy/main/"
            "schemas/v2.4.0/mockresponseplugin.mocksfile.schema.json"
        ),
        "mocks": [
            {
                "request": {
                    "url": f"{base}/v1.0/users?$skiptoken={_SKIPTOKEN}",
                    "method": "GET",
                },
                "response": {
                    "statusCode": 200,
                    "headers": _mock_headers(),
                    "body": second_page,
                },
            },
            {
                "request": {"url": f"{base}/v1.0/users?*", "method": "GET"},
                "response": {
                    "statusCode": 200,
                    "headers": _mock_headers(),
                    "body": first_page,
                },
            },
            {
                "request": {"url": f"{base}/v1.0/groups", "method": "GET"},
                "response": {
                    "statusCode": 200,
                    "headers": _mock_headers(),
                    "body": {
                        "@odata.context": f"{base}/v1.0/$metadata#groups",
                        "value": [{"id": "group-1", "displayName": "Security"}],
                    },
                },
            },
            {
                "request": {"url": f"{base}/v1.0/groups?*", "method": "GET"},
                "response": {
                    "statusCode": 200,
                    "headers": _mock_headers(),
                    "body": {
                        "@odata.context": f"{base}/v1.0/$metadata#groups",
                        "value": [
                            {
                                "id": "group-1",
                                "displayName": "Security",
                                "securityEnabled": True,
                            }
                        ],
                    },
                },
            },
            {
                "request": {"url": f"{base}/beta/users?*", "method": "GET"},
                "response": {
                    "statusCode": 200,
                    "headers": _mock_headers(),
                    "body": {
                        "@odata.context": f"{base}/beta/$metadata#users",
                        "value": [
                            {
                                "id": "beta-user-1",
                                "displayName": "Beta User",
                                "signInActivity": {
                                    "lastSignInDateTime": "2026-05-13T12:00:00Z"
                                },
                            }
                        ],
                    },
                },
            },
        ],
    }
    devproxyrc = {
        "$schema": (
            "https://raw.githubusercontent.com/dotnet/dev-proxy/main/"
            "schemas/v2.4.0/rc.schema.json"
        ),
        "plugins": [
            {
                "name": "GraphMockResponsePlugin",
                "enabled": True,
                "pluginPath": "~appFolder/plugins/dev-proxy-plugins.dll",
                "configSection": "mocksPlugin",
            },
            {
                "name": "LatencyPlugin",
                "enabled": True,
                "pluginPath": "~appFolder/plugins/dev-proxy-plugins.dll",
                "configSection": "latencyPlugin",
            },
        ],
        "urlsToWatch": ["https://graph.microsoft.com/*"],
        "mocksPlugin": {
            "$schema": (
                "https://raw.githubusercontent.com/dotnet/dev-proxy/main/"
                "schemas/v2.4.0/mockresponseplugin.schema.json"
            ),
            "mocksFile": "/config/mocks.json",
            "blockUnmockedRequests": True,
        },
        "latencyPlugin": {
            "$schema": (
                "https://raw.githubusercontent.com/dotnet/dev-proxy/main/"
                "schemas/v2.4.0/latencyplugin.schema.json"
            ),
            "minMs": _FIRST_PAGE_DELAY_SECONDS,
            "maxMs": _FIRST_PAGE_DELAY_SECONDS,
        },
        "logLevel": "information",
        "newVersionNotification": "none",
        "showSkipMessages": False,
    }
    (config_dir / "mocks.json").write_text(json.dumps(mocks), encoding="utf-8")
    (config_dir / "devproxyrc.json").write_text(
        json.dumps(devproxyrc), encoding="utf-8"
    )


def _start_dev_proxy(
    runtime: RuntimeSpec,
    *,
    proxy_port: int,
    api_port: int,
    config_dir: Path,
) -> ManagedContainer:
    container_name = f"tenzir-test-dev-proxy-{uuid.uuid4().hex[:8]}"
    run_args = [
        "--rm",
        "--name",
        container_name,
        "-p",
        f"{proxy_port}:{DEV_PROXY_PORT}",
        "-p",
        f"{api_port}:{DEV_PROXY_API_PORT}",
        "-v",
        f"{config_dir}:/config:ro",
        DEV_PROXY_IMAGE,
        "--config-file",
        "/config/devproxyrc.json",
    ]
    logger.info("Starting Dev Proxy with %s", runtime.binary)
    container = start_detached(runtime, run_args)
    logger.info("Dev Proxy started: %s", container.container_id[:12])
    return container


def _stop_dev_proxy(container: ManagedContainer) -> None:
    logger.info("Stopping Dev Proxy container: %s", container.container_id[:12])
    result = container.stop()
    if result.returncode != 0:
        logger.warning(
            "Failed to stop Dev Proxy container %s: %s",
            container.container_id[:12],
            (result.stderr or result.stdout or "").strip() or "no output",
        )


def _wait_for_dev_proxy(api_port: int) -> None:
    url = f"http://127.0.0.1:{api_port}/proxy/rootCertificate?format=crt"

    def _probe() -> tuple[bool, dict[str, str]]:
        try:
            with urllib.request.urlopen(url, timeout=3) as response:
                response.read()
            return True, {"api": "ready"}
        except (urllib.error.URLError, OSError) as exc:
            return False, {"error": str(exc)}

    try:
        wait_until_ready(
            _probe,
            timeout_seconds=STARTUP_TIMEOUT,
            poll_interval_seconds=HEALTH_CHECK_INTERVAL,
            timeout_context="Dev Proxy startup",
        )
    except ContainerReadinessTimeout as exc:
        raise RuntimeError(str(exc)) from exc
    logger.info("Dev Proxy is ready")


def _download_root_certificate(api_port: int, dest: Path) -> None:
    url = f"http://127.0.0.1:{api_port}/proxy/rootCertificate?format=crt"
    with urllib.request.urlopen(url, timeout=10) as response:
        dest.write_bytes(response.read())


def _plugin_env() -> dict[str, str]:
    env: dict[str, str] = {}
    if binary := os.environ.get("TENZIR_BINARY"):
        build_dir = Path(binary).resolve().parent.parent
        plugin_dir = build_dir / "lib" / "tenzir" / "plugins"
        if plugin_dir.is_dir():
            env["TENZIR_PLUGIN_DIRS"] = str(plugin_dir)
            env["TENZIR_PLUGINS"] = "all"
        lib_dir = build_dir / "lib"
        if lib_dir.is_dir():
            env["DYLD_LIBRARY_PATH"] = str(lib_dir)
    return env


@fixture(name="microsoft_graph")
def run() -> Iterator[dict[str, str]]:
    runtime = detect_runtime()
    if runtime is None:
        raise FixtureUnavailable(
            "container runtime (docker/podman) required but not found"
        )
    token_server = _TokenServer((_HOST, 0), _TokenHandler)
    token_thread = threading.Thread(target=token_server.serve_forever, daemon=True)
    proxy_port = find_free_port()
    api_port = find_free_port()
    container: ManagedContainer | None = None
    with tempfile.TemporaryDirectory(prefix="tenzir-ms-graph-") as temp:
        temp_dir = Path(temp)
        config_dir = temp_dir / "config"
        config_dir.mkdir()
        cert_path = temp_dir / "dev-proxy-ca.crt"
        _write_config(config_dir)
        token_thread.start()
        try:
            container = _start_dev_proxy(
                runtime,
                proxy_port=proxy_port,
                api_port=api_port,
                config_dir=config_dir,
            )
            _wait_for_dev_proxy(api_port)
            _download_root_certificate(api_port, cert_path)
            env = {
                "HTTPS_PROXY": f"http://127.0.0.1:{proxy_port}",
                "https_proxy": f"http://127.0.0.1:{proxy_port}",
                "NO_PROXY": "127.0.0.1,localhost",
                "no_proxy": "127.0.0.1,localhost",
                "MS_GRAPH_FIXTURE_TLS_CACERT": str(cert_path),
                "MS_GRAPH_FIXTURE_AUTHORITY": (
                    f"http://{_HOST}:{token_server.server_port}"
                ),
                "MS_GRAPH_FIXTURE_TENANT_ID": _TENANT_ID,
                "MS_GRAPH_FIXTURE_CLIENT_ID": _CLIENT_ID,
                "MS_GRAPH_FIXTURE_CLIENT_SECRET": _CLIENT_SECRET,
            }
            env.update(_plugin_env())
            yield env
        finally:
            token_server.shutdown()
            token_thread.join(timeout=2)
            if container is not None:
                _stop_dev_proxy(container)
