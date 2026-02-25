"""Mock OAuth and ingestion servers for to_google_secops integration tests."""

from __future__ import annotations

import base64
import gzip
import json
import os
import subprocess
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Iterator
from urllib.parse import parse_qs

from tenzir_test import fixture
from tenzir_test.fixtures import FixtureUnavailable

_HOST = "127.0.0.1"
_EXPECTED_TOKEN = "test-token-12345"
_EXPECTED_SCOPE = "https://www.googleapis.com/auth/malachite-ingestion"
_EXPECTED_AUD = "https://oauth2.googleapis.com/token"


def _generate_rsa_keypair() -> tuple[str, bytes]:
    """Generate an RSA key pair. Returns (private_key_pem, public_key_der)."""
    try:
        result = subprocess.run(
            ["openssl", "genrsa", "2048"],
            capture_output=True,
            check=True,
        )
        private_pem = result.stdout.decode()
        result = subprocess.run(
            ["openssl", "rsa", "-pubout", "-outform", "DER"],
            input=result.stdout,
            capture_output=True,
            check=True,
        )
    except (FileNotFoundError, subprocess.CalledProcessError) as e:
        raise FixtureUnavailable(f"openssl unavailable: {e}")
    public_der = result.stdout
    return private_pem, public_der


def _b64url_decode(data: str) -> bytes:
    """Decode base64url without padding."""
    padding = 4 - len(data) % 4
    if padding != 4:
        data += "=" * padding
    return base64.urlsafe_b64decode(data)


def _verify_rs256(public_der: bytes, message: bytes, signature: bytes) -> bool:
    """Verify an RS256 signature using openssl."""
    try:
        with (
            tempfile.NamedTemporaryFile(suffix=".der") as pub_file,
            tempfile.NamedTemporaryFile(suffix=".sig") as sig_file,
        ):
            pub_file.write(public_der)
            pub_file.flush()
            sig_file.write(signature)
            sig_file.flush()
            result = subprocess.run(
                [
                    "openssl",
                    "dgst",
                    "-sha256",
                    "-verify",
                    pub_file.name,
                    "-keyform",
                    "DER",
                    "-signature",
                    sig_file.name,
                ],
                input=message,
                capture_output=True,
            )
            return result.returncode == 0
    except Exception:
        return False


def _make_oauth_handler(
    client_email: str,
    public_der: bytes,
) -> type[BaseHTTPRequestHandler]:
    class OAuthHandler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: object) -> None:
            pass

        def do_POST(self) -> None:
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length).decode()
            params = parse_qs(body)
            grant_type = params.get("grant_type", [None])[0]
            expected_grant_type = "urn:ietf:params:oauth:grant-type:jwt-bearer"
            if grant_type != expected_grant_type:
                self._error(
                    400, f"expected grant_type={expected_grant_type}, got {grant_type}"
                )
                return
            assertion = params.get("assertion", [None])[0]
            if not assertion:
                self._error(400, "missing assertion parameter")
                return
            parts = assertion.split(".")
            if len(parts) != 3:
                self._error(400, "JWT must have 3 parts")
                return
            try:
                header = json.loads(_b64url_decode(parts[0]))
                claims = json.loads(_b64url_decode(parts[1]))
                sig = _b64url_decode(parts[2])
            except Exception as e:
                self._error(400, f"failed to decode JWT: {e}")
                return
            if header.get("alg") != "RS256" or header.get("typ") != "JWT":
                self._error(400, f"bad JWT header: {header}")
                return
            for field in ("iss", "scope", "aud", "iat", "exp"):
                if field not in claims:
                    self._error(400, f"missing claim: {field}")
                    return
            if claims["iss"] != client_email:
                self._error(400, f"iss mismatch: {claims['iss']}")
                return
            if claims["scope"] != _EXPECTED_SCOPE:
                self._error(400, f"scope mismatch: {claims['scope']}")
                return
            if claims["aud"] != _EXPECTED_AUD:
                self._error(400, f"aud mismatch: {claims['aud']}")
                return
            if not isinstance(claims["iat"], int) or not isinstance(claims["exp"], int):
                self._error(400, "iat/exp must be integers")
                return
            if claims["exp"] <= claims["iat"]:
                self._error(400, "exp must be > iat")
                return
            now = int(time.time())
            if claims["exp"] <= now:
                self._error(400, f"token has expired: exp={claims['exp']}, now={now}")
                return
            message = f"{parts[0]}.{parts[1]}".encode()
            if not _verify_rs256(public_der, message, sig):
                self._error(400, "invalid JWT signature")
                return
            self._json_response(
                200,
                {
                    "access_token": _EXPECTED_TOKEN,
                    "token_type": "Bearer",
                    "expires_in": 3600,
                },
            )

        def _error(self, code: int, msg: str) -> None:
            self._json_response(code, {"error": msg})

        def _json_response(self, code: int, obj: dict) -> None:  # type: ignore[type-arg]
            body = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    return OAuthHandler


def _make_ingestion_handler(
    capture_path: str,
) -> type[BaseHTTPRequestHandler]:
    lock = threading.Lock()

    class IngestionHandler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: object) -> None:
            pass

        def do_POST(self) -> None:
            auth = self.headers.get("Authorization", "")
            if auth != f"Bearer {_EXPECTED_TOKEN}":
                self._json_response(401, {"error": "unauthorized"})
                return
            content_length = int(self.headers.get("Content-Length", 0))
            raw = self.rfile.read(content_length)
            encoding = self.headers.get("Content-Encoding", "")
            if encoding == "gzip":
                try:
                    raw = gzip.decompress(raw)
                except Exception as e:
                    self._json_response(400, {"error": f"gzip error: {e}"})
                    return
            try:
                payload = json.loads(raw)
            except Exception as e:
                self._json_response(400, {"error": f"JSON parse error: {e}"})
                return
            err = _validate_payload(payload)
            if err:
                self._json_response(400, {"error": err})
                return
            with lock:
                with open(capture_path, "a") as f:
                    f.write(json.dumps(payload, sort_keys=True) + "\n")
            self._json_response(200, {})

        def _json_response(self, code: int, obj: dict) -> None:  # type: ignore[type-arg]
            body = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    return IngestionHandler


def _validate_payload(payload: object) -> str | None:
    if not isinstance(payload, dict):
        return "payload must be a JSON object"
    for key in ("customer_id", "log_type", "namespace", "entries"):
        if key not in payload:
            return f"missing key: {key}"
    if not isinstance(payload["customer_id"], str):
        return "customer_id must be a string"
    if not isinstance(payload["log_type"], str):
        return "log_type must be a string"
    if not isinstance(payload["namespace"], str):
        return "namespace must be a string"
    if not isinstance(payload["entries"], list):
        return "entries must be an array"
    if len(payload["entries"]) == 0:
        return "entries must be non-empty"
    for i, entry in enumerate(payload["entries"]):
        if not isinstance(entry, dict):
            return f"entries[{i}] must be an object"
        if "log_text" not in entry:
            return f"entries[{i}] missing log_text"
        if not isinstance(entry["log_text"], str):
            return f"entries[{i}].log_text must be a string"
    return None


@fixture()
def google_secops() -> Iterator[dict[str, str]]:
    client_email = "test@example.iam.gserviceaccount.com"
    private_pem, public_der = _generate_rsa_keypair()
    fd, capture_path = tempfile.mkstemp(prefix="secops-capture-", suffix=".jsonl")
    os.close(fd)
    oauth_server = None
    ingestion_server = None
    oauth_thread = None
    ingestion_thread = None
    try:
        oauth_handler = _make_oauth_handler(client_email, public_der)
        ingestion_handler = _make_ingestion_handler(capture_path)
        oauth_server = HTTPServer((_HOST, 0), oauth_handler)
        ingestion_server = HTTPServer((_HOST, 0), ingestion_handler)
        oauth_port = oauth_server.server_port
        ingestion_port = ingestion_server.server_port
        oauth_thread = threading.Thread(target=oauth_server.serve_forever, daemon=True)
        ingestion_thread = threading.Thread(
            target=ingestion_server.serve_forever,
            daemon=True,
        )
        oauth_thread.start()
        ingestion_thread.start()
        env = {
            "GOOGLE_SECOPS_TOKEN_URL": f"http://{_HOST}:{oauth_port}/token",
            "GOOGLE_SECOPS_INGESTION_URL": (
                f"http://{_HOST}:{ingestion_port}/v2/unstructuredlogentries:batchCreate"
            ),
            "GOOGLE_SECOPS_CAPTURE_FILE": capture_path,
            "GOOGLE_SECOPS_PRIVATE_KEY": private_pem,
            "GOOGLE_SECOPS_CLIENT_EMAIL": client_email,
        }
        yield env
    finally:
        if oauth_server is not None:
            oauth_server.shutdown()
        if ingestion_server is not None:
            ingestion_server.shutdown()
        if oauth_thread is not None:
            oauth_thread.join(timeout=2)
        if ingestion_thread is not None:
            ingestion_thread.join(timeout=2)
        if os.path.exists(capture_path):
            os.remove(capture_path)
