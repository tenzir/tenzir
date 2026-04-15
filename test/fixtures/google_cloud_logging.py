"""Mock OAuth and gRPC servers for to_google_cloud_logging integration tests."""

from __future__ import annotations

import base64
import json
import os
import subprocess
import sys
import tempfile
import threading
from concurrent import futures
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Iterator
from urllib.parse import parse_qs

from tenzir_test import fixture
from tenzir_test.fixtures import FixtureUnavailable

try:
    import grpc
    from grpc_tools import protoc as grpc_protoc
except ImportError:
    grpc = None  # type: ignore[assignment]

try:
    import trustme
except ImportError:
    trustme = None  # type: ignore[assignment]

_HOST = "127.0.0.1"
_EXPECTED_TOKEN = "test-token-gcl-12345"
_EXPECTED_SCOPE = "https://www.googleapis.com/auth/logging.write"


# ==============================================================================
# Proto Compilation
# ==============================================================================

# We bundle the logging-specific proto files in test/fixtures/protos/ and rely
# on grpc_tools for the common google/api/* and google/protobuf/* protos. At
# fixture startup we compile them into a temp directory and import the generated
# stubs dynamically.


def _compile_protos(output_dir: str) -> None:
    """Compile the bundled logging protos into Python gRPC stubs."""
    proto_dir = str(Path(__file__).parent / "protos")
    # grpc_tools ships its own copy of well-known protos and
    # googleapis-common-protos. We find that include path so the compiler
    # can resolve imports like google/api/annotations.proto.
    import grpc_tools

    grpc_tools_include = str(Path(grpc_tools.__file__).parent / "_proto")
    args = [
        "grpc_tools.protoc",
        f"--proto_path={proto_dir}",
        f"--proto_path={grpc_tools_include}",
        f"--python_out={output_dir}",
        f"--grpc_python_out={output_dir}",
        f"{proto_dir}/google/logging/v2/logging.proto",
        f"{proto_dir}/google/logging/v2/log_entry.proto",
        f"{proto_dir}/google/logging/type/log_severity.proto",
        f"{proto_dir}/google/logging/type/http_request.proto",
    ]
    rc = grpc_protoc.main(args)
    if rc != 0:
        raise RuntimeError(f"protoc failed with exit code {rc}")


def _import_logging_stubs(stub_dir: str):  # type: ignore[no-untyped-def]
    """Import the compiled logging gRPC stubs from stub_dir.

    Returns (logging_pb2, logging_pb2_grpc, log_entry_pb2).
    """
    # The generated stubs use standard imports like
    #   from google.logging.v2 import log_entry_pb2
    # so we need them importable as real packages. The `google` package is
    # already a namespace package (from protobuf, googleapis-common-protos,
    # etc.), so we extend its __path__ to also search our stub directory
    # rather than creating __init__.py files that would shadow google.protobuf.
    for subdir in [
        os.path.join(stub_dir, "google", "logging"),
        os.path.join(stub_dir, "google", "logging", "v2"),
        os.path.join(stub_dir, "google", "logging", "type"),
    ]:
        init = os.path.join(subdir, "__init__.py")
        if not os.path.exists(init):
            os.makedirs(subdir, exist_ok=True)
            with open(init, "w") as f:
                pass  # empty init for sub-packages

    # Extend the existing google namespace to include our stubs.
    import google

    stub_google = os.path.join(stub_dir, "google")
    if stub_google not in list(google.__path__):
        google.__path__ = [stub_google] + list(google.__path__)

    from google.logging.type import log_severity_pb2 as _ls  # noqa: F401
    from google.logging.type import http_request_pb2 as _hr  # noqa: F401
    from google.logging.v2 import log_entry_pb2
    from google.logging.v2 import logging_pb2
    from google.logging.v2 import logging_pb2_grpc

    return logging_pb2, logging_pb2_grpc, log_entry_pb2


# ==============================================================================
# TLS Certificate Generation
# ==============================================================================


def _generate_tls_material(temp_dir: Path) -> dict[str, Path]:
    """Generate a CA and server cert for localhost using trustme."""
    ca = trustme.CA()
    server_cert = ca.issue_cert(_HOST, "localhost")
    ca_cert_path = temp_dir / "ca-cert.pem"
    server_cert_path = temp_dir / "server-cert.pem"
    server_key_path = temp_dir / "server-key.pem"
    ca.cert_pem.write_to_path(str(ca_cert_path))
    server_cert.cert_chain_pems[0].write_to_path(str(server_cert_path))
    server_cert.private_key_pem.write_to_path(str(server_key_path))
    return {
        "ca_cert": ca_cert_path,
        "server_cert": server_cert_path,
        "server_key": server_key_path,
    }


# ==============================================================================
# RSA Keypair and Service Account JSON
# ==============================================================================


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


def _build_service_account_json(
    private_key_pem: str,
    client_email: str,
    token_uri: str,
) -> str:
    """Build a fake service account JSON with a custom token_uri."""
    return json.dumps(
        {
            "type": "service_account",
            "project_id": "test-project",
            "private_key_id": "test-key-id",
            "private_key": private_key_pem,
            "client_email": client_email,
            "client_id": "123456789",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": token_uri,
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": f"https://www.googleapis.com/robot/v1/metadata/x509/{client_email}",
        }
    )


# ==============================================================================
# Mock OAuth HTTP Server
# ==============================================================================


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
            if self.path != "/token":
                self._error(404, f"unknown path: {self.path}")
                return
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length).decode()
            params = parse_qs(body)
            grant_type = params.get("grant_type", [None])[0]
            expected = "urn:ietf:params:oauth:grant-type:jwt-bearer"
            if grant_type != expected:
                self._error(400, f"expected grant_type={expected}, got {grant_type}")
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
            import time

            iat = claims.get("iat")
            exp = claims.get("exp")
            if not isinstance(iat, (int, float)) or not isinstance(exp, (int, float)):
                self._error(400, f"iat/exp must be numeric: iat={iat}, exp={exp}")
                return
            if exp <= iat:
                self._error(400, f"exp ({exp}) must be after iat ({iat})")
                return
            # Google allows up to 1 hour lifetime; we add a generous buffer.
            max_lifetime = 3700
            if exp - iat > max_lifetime:
                self._error(400, f"token lifetime {exp - iat}s exceeds {max_lifetime}s")
                return
            now = time.time()
            # Allow 5 minutes of clock skew.
            if iat > now + 300:
                self._error(400, f"iat ({iat}) is too far in the future (now={now})")
                return
            if exp < now - 300:
                self._error(400, f"token expired: exp={exp}, now={now}")
                return
            if claims["scope"] != _EXPECTED_SCOPE:
                self._error(400, f"scope mismatch: {claims['scope']}")
                return
            expected_aud = f"http://{_HOST}:{self.server.server_port}/token"
            if claims["aud"] != expected_aud:
                self._error(400, f"aud mismatch: {claims['aud']}")
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


# ==============================================================================
# Mock gRPC Server
# ==============================================================================


def _make_grpc_servicer(
    capture_path: str, public_der: bytes, logging_pb2, logging_pb2_grpc
):  # type: ignore[no-untyped-def]
    """Create a LoggingServiceV2 servicer that captures WriteLogEntries requests."""
    from google.protobuf import json_format

    lock = threading.Lock()

    class MockLoggingServicer(logging_pb2_grpc.LoggingServiceV2Servicer):
        def WriteLogEntries(self, request, context):  # type: ignore[no-untyped-def]
            # Reject requests without a valid bearer token, matching the
            # behavior of the real Google Cloud Logging endpoint. The C++
            # client uses self-signed JWTs rather than exchanged OAuth
            # tokens, so we verify the JWT signature using the service
            # account's public key.
            metadata = dict(context.invocation_metadata())
            auth = metadata.get("authorization", "")
            if not auth.startswith("Bearer "):
                context.abort(grpc.StatusCode.UNAUTHENTICATED, "missing bearer token")
                return logging_pb2.WriteLogEntriesResponse()
            token = auth[len("Bearer ") :]
            parts = token.split(".")
            if len(parts) != 3:
                context.abort(grpc.StatusCode.UNAUTHENTICATED, "malformed JWT")
                return logging_pb2.WriteLogEntriesResponse()
            message = f"{parts[0]}.{parts[1]}".encode()
            sig = _b64url_decode(parts[2])
            if not _verify_rs256(public_der, message, sig):
                context.abort(grpc.StatusCode.UNAUTHENTICATED, "invalid JWT signature")
                return logging_pb2.WriteLogEntriesResponse()
            # Convert the protobuf request to a JSON-serializable dict and
            # append it as one line to the capture file. We use
            # preserving_proto_field_name=True so field names match the proto
            # definition (snake_case) rather than camelCase.
            entry = json_format.MessageToDict(
                request,
                preserving_proto_field_name=True,
            )
            with lock:
                with open(capture_path, "a") as f:
                    f.write(json.dumps(entry, sort_keys=True) + "\n")
            return logging_pb2.WriteLogEntriesResponse()

    return MockLoggingServicer()


def _start_grpc_server(
    servicer,  # type: ignore[no-untyped-def]
    logging_pb2_grpc,  # type: ignore[no-untyped-def]
    tls_material: dict[str, Path],
) -> tuple[grpc.Server, int]:  # type: ignore[name-defined]
    """Start a gRPC server with TLS on an ephemeral port."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    logging_pb2_grpc.add_LoggingServiceV2Servicer_to_server(servicer, server)

    server_cert = tls_material["server_cert"].read_bytes()
    server_key = tls_material["server_key"].read_bytes()
    ca_cert = tls_material["ca_cert"].read_bytes()
    credentials = grpc.ssl_server_credentials(
        [(server_key, server_cert)],
        root_certificates=ca_cert,
        require_client_auth=False,
    )
    port = server.add_secure_port(f"{_HOST}:0", credentials)
    server.start()
    return server, port


# ==============================================================================
# Fixture Entry Point
# ==============================================================================


@fixture()
def google_cloud_logging() -> Iterator[dict[str, str]]:
    if grpc is None:
        raise FixtureUnavailable(
            "grpcio/grpcio-tools not installed; install with: "
            "pip install grpcio grpcio-tools googleapis-common-protos"
        )
    if trustme is None:
        raise FixtureUnavailable(
            "trustme not installed; install with: pip install trustme"
        )

    client_email = "test-gcl@test-project.iam.gserviceaccount.com"
    temp_dir = Path(tempfile.mkdtemp(prefix="gcl-test-"))
    capture_path = str(temp_dir / "capture.jsonl")
    # Touch the capture file so verification tests can read it even if no
    # requests were sent.
    Path(capture_path).touch()

    oauth_server = None
    oauth_thread = None
    grpc_server = None

    try:
        # --- Proto compilation ---
        stub_dir = str(temp_dir / "stubs")
        os.makedirs(stub_dir, exist_ok=True)
        try:
            _compile_protos(stub_dir)
            logging_pb2, logging_pb2_grpc, _ = _import_logging_stubs(stub_dir)
        except ModuleNotFoundError as e:
            raise FixtureUnavailable(
                f"missing proto dependency: {e}; "
                "install with: pip install googleapis-common-protos"
            )

        # --- RSA keypair + service account JSON ---
        private_pem, public_der = _generate_rsa_keypair()

        # --- TLS certificates ---
        tls = _generate_tls_material(temp_dir)

        # --- OAuth HTTP server ---
        # Bind with port 0 directly to avoid a TOCTOU race where another
        # process could claim the port between discovery and rebind.
        oauth_handler = _make_oauth_handler(client_email, public_der)
        oauth_server = HTTPServer((_HOST, 0), oauth_handler)
        oauth_port = oauth_server.server_port

        token_uri = f"http://{_HOST}:{oauth_port}/token"
        sa_json = _build_service_account_json(private_pem, client_email, token_uri)
        sa_path = str(temp_dir / "service-account.json")
        with open(sa_path, "w") as f:
            f.write(sa_json)

        oauth_thread = threading.Thread(target=oauth_server.serve_forever, daemon=True)
        oauth_thread.start()

        # --- gRPC server ---
        servicer = _make_grpc_servicer(
            capture_path, public_der, logging_pb2, logging_pb2_grpc
        )
        grpc_server, grpc_port = _start_grpc_server(servicer, logging_pb2_grpc, tls)

        env = {
            "GOOGLE_CLOUD_CPP_LOGGING_SERVICE_V2_ENDPOINT": f"localhost:{grpc_port}",
            "GOOGLE_CLOUD_CPP_LOGGING_SERVICE_V2_AUTHORITY": "localhost",
            "GRPC_DEFAULT_SSL_ROOTS_FILE_PATH": str(tls["ca_cert"]),
            "GCL_CAPTURE_FILE": capture_path,
            "GCL_SERVICE_CREDENTIALS": sa_path,
            "GOOGLE_CLOUD_PROJECT": "test-project",
        }
        yield env

    finally:
        if grpc_server is not None:
            grpc_server.stop(grace=2)
        if oauth_server is not None:
            oauth_server.shutdown()
        if oauth_thread is not None:
            oauth_thread.join(timeout=2)
        # Leave temp_dir for debugging; the OS cleans /tmp eventually.
