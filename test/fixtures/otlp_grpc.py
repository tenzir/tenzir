from __future__ import annotations

import importlib
import shutil
import sys
import tempfile
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from tenzir_test import FixtureHandle, fixture
from tenzir_test.fixtures import FixtureUnavailable, current_options

from ._utils import find_free_port, generate_self_signed_cert

try:
    import grpc
    import grpc_tools
    from google.protobuf import json_format
    from grpc_tools import protoc
except ImportError:
    grpc = None  # type: ignore[assignment]

_HOST = "127.0.0.1"


@dataclass(frozen=True)
class OtlpGrpcOptions:
    requests: list[dict[str, Any]] = field(default_factory=list)
    initial_delay: float = 0.5
    retry_delay: float = 0.1
    request_timeout: float = 2.0
    max_attempts_per_request: int = 30
    tls: bool = False
    mtls: bool = False


def _compile_stubs(output_dir: Path) -> None:
    proto_root = Path(__file__).parents[2] / "libtenzir/aux/opentelemetry-proto"
    grpc_tools_include = Path(grpc_tools.__file__).parent / "_proto"
    protos = [
        proto_root / "opentelemetry/proto/common/v1/common.proto",
        proto_root / "opentelemetry/proto/resource/v1/resource.proto",
        proto_root / "opentelemetry/proto/logs/v1/logs.proto",
        proto_root / "opentelemetry/proto/trace/v1/trace.proto",
        proto_root / "opentelemetry/proto/metrics/v1/metrics.proto",
        proto_root / "opentelemetry/proto/collector/logs/v1/logs_service.proto",
        proto_root / "opentelemetry/proto/collector/metrics/v1/metrics_service.proto",
        proto_root / "opentelemetry/proto/collector/trace/v1/trace_service.proto",
    ]
    args = [
        "grpc_tools.protoc",
        f"--proto_path={proto_root}",
        f"--proto_path={grpc_tools_include}",
        f"--python_out={output_dir}",
        f"--grpc_python_out={output_dir}",
        *(str(proto) for proto in protos),
    ]
    if protoc.main(args) != 0:
        raise RuntimeError("failed to compile OTLP gRPC test stubs")


def _load_stubs(stub_dir: Path) -> dict[str, tuple[Any, Any, Any]]:
    sys.path.insert(0, str(stub_dir))
    importlib.invalidate_caches()
    from opentelemetry.proto.collector.logs.v1 import logs_service_pb2
    from opentelemetry.proto.collector.logs.v1 import logs_service_pb2_grpc
    from opentelemetry.proto.collector.metrics.v1 import metrics_service_pb2
    from opentelemetry.proto.collector.metrics.v1 import metrics_service_pb2_grpc
    from opentelemetry.proto.collector.trace.v1 import trace_service_pb2
    from opentelemetry.proto.collector.trace.v1 import trace_service_pb2_grpc

    return {
        "logs": (
            logs_service_pb2.ExportLogsServiceRequest,
            logs_service_pb2_grpc.LogsServiceStub,
            "Export",
        ),
        "metrics": (
            metrics_service_pb2.ExportMetricsServiceRequest,
            metrics_service_pb2_grpc.MetricsServiceStub,
            "Export",
        ),
        "traces": (
            trace_service_pb2.ExportTraceServiceRequest,
            trace_service_pb2_grpc.TraceServiceStub,
            "Export",
        ),
    }


@fixture(name="otlp_grpc", options=OtlpGrpcOptions)
def otlp_grpc() -> FixtureHandle:
    if grpc is None:
        raise FixtureUnavailable(
            "grpcio/grpcio-tools not installed; install with: "
            "pip install grpcio grpcio-tools"
        )
    opts = current_options("otlp_grpc")
    if not isinstance(opts, OtlpGrpcOptions):
        raise RuntimeError("otlp_grpc fixture options failed to parse")
    if opts.mtls and not opts.tls:
        raise RuntimeError("otlp_grpc.mtls requires otlp_grpc.tls")

    temp_dir = Path(tempfile.mkdtemp(prefix="otlp-grpc-"))
    stub_dir = temp_dir / "stubs"
    stub_dir.mkdir()
    _compile_stubs(stub_dir)
    stubs = _load_stubs(stub_dir)

    port = find_free_port()
    endpoint = f"{_HOST}:{port}"
    server_material: tuple[Path, Path, Path, Path] | None = None
    client_material: tuple[Path, Path, Path, Path] | None = None
    if opts.tls:
        server_dir = temp_dir / "server"
        server_dir.mkdir()
        server_material = generate_self_signed_cert(
            server_dir, common_name=_HOST, san_entries=[f"IP:{_HOST}"]
        )
    if opts.mtls:
        client_dir = temp_dir / "client"
        client_dir.mkdir()
        client_material = generate_self_signed_cert(
            client_dir,
            common_name="otlp-test-client",
            san_entries=["DNS:otlp-test-client"],
        )

    errors: list[str] = []
    sent_count = [0]
    stop_event = threading.Event()

    def _channel() -> grpc.Channel:  # type: ignore[name-defined]
        if not opts.tls:
            return grpc.insecure_channel(endpoint)
        assert server_material is not None
        root_certificates = server_material[2].read_bytes()
        private_key = None
        certificate_chain = None
        if client_material is not None:
            private_key = client_material[1].read_bytes()
            certificate_chain = client_material[0].read_bytes()
        credentials = grpc.ssl_channel_credentials(
            root_certificates=root_certificates,
            private_key=private_key,
            certificate_chain=certificate_chain,
        )
        return grpc.secure_channel(endpoint, credentials)

    def _send(channel: grpc.Channel, index: int, spec: dict[str, Any]) -> None:
        signal = str(spec.get("signal", "logs"))
        expected = str(spec.get("expected_status", "OK"))
        if signal not in stubs:
            errors.append(f"request {index}: unsupported signal {signal!r}")
            return
        request_type, stub_type, method_name = stubs[signal]
        request = request_type()
        try:
            json_format.ParseDict(spec.get("body", {}), request)
            body_string_size = int(spec.get("body_string_size", 0))
            if body_string_size:
                request.resource_logs[0].scope_logs[0].log_records[
                    0
                ].body.string_value = "x" * body_string_size
            serialized_size = int(spec.get("serialized_size", 0))
            if serialized_size:
                value = request.resource_logs[0].resource.attributes[0].value
                low = 0
                high = serialized_size
                while low <= high:
                    size = (low + high) // 2
                    value.string_value = "x" * size
                    actual_size = request.ByteSize()
                    if actual_size == serialized_size:
                        break
                    if actual_size < serialized_size:
                        low = size + 1
                    else:
                        high = size - 1
                else:
                    raise ValueError(
                        f"cannot construct a request of exactly {serialized_size} bytes"
                    )
            resource_attribute_string_size = int(
                spec.get("resource_attribute_string_size", 0)
            )
            if resource_attribute_string_size:
                request.resource_logs[0].resource.attributes[0].value.string_value = (
                    "x" * resource_attribute_string_size
                )
            log_record_count = int(spec.get("log_record_count", 0))
            if log_record_count:
                records = request.resource_logs[0].scope_logs[0].log_records
                prototype = type(records[0])()
                prototype.CopyFrom(records[0])
                while len(records) < log_record_count:
                    records.add().CopyFrom(prototype)
        except Exception as exc:
            errors.append(f"request {index}: invalid fixture body: {exc}")
            return
        metadata = [
            (
                str(item[0]),
                str(item[1]).encode() if str(item[0]).endswith("-bin") else item[1],
            )
            for item in spec.get("metadata", [])
        ]
        compression = (
            grpc.Compression.Gzip if spec.get("compression") == "gzip" else None
        )
        last_error: grpc.RpcError | None = None
        for _ in range(max(opts.max_attempts_per_request, 1)):
            if stop_event.is_set():
                return
            last_error = None
            try:
                method = getattr(stub_type(channel), method_name)
                method(
                    request,
                    timeout=float(spec.get("timeout", opts.request_timeout)),
                    metadata=metadata,
                    compression=compression,
                )
                actual = "OK"
            except grpc.RpcError as exc:
                actual = exc.code().name
                last_error = exc
            connection_failure = last_error and any(
                marker in last_error.details().lower()
                for marker in ("failed to connect", "connection refused")
            )
            if actual == "UNAVAILABLE" and connection_failure:
                stop_event.wait(opts.retry_delay)
                continue
            if actual != expected:
                details = last_error.details() if last_error else ""
                errors.append(
                    f"request {index}: expected gRPC status {expected}, "
                    f"got {actual}: {details}"
                )
            sent_count[0] += 1
            return
        errors.append(
            f"request {index}: receiver remained unavailable after "
            f"{opts.max_attempts_per_request} attempts"
        )

    def _worker() -> None:
        if stop_event.wait(opts.initial_delay):
            return
        channel = _channel()
        try:
            try:
                grpc.channel_ready_future(channel).result(
                    timeout=opts.max_attempts_per_request * opts.retry_delay
                )
            except grpc.FutureTimeoutError:
                errors.append("OTLP/gRPC receiver remained unavailable")
                return
            for index, spec in enumerate(opts.requests):
                _send(channel, index, spec)
        finally:
            channel.close()

    worker = threading.Thread(target=_worker, daemon=True)
    worker.start()

    def _assert_test(
        *, test: Path, assertions: dict[str, Any] | None = None, **_: Any
    ) -> None:
        _ = (test, assertions)

    def _teardown() -> None:
        stop_event.set()
        worker.join(timeout=5)
        try:
            if worker.is_alive():
                raise RuntimeError("otlp_grpc fixture worker did not stop")
            if errors:
                raise AssertionError("; ".join(errors))
            if sent_count[0] < len(opts.requests):
                raise AssertionError(
                    f"expected to send {len(opts.requests)} requests, "
                    f"sent {sent_count[0]}"
                )
        finally:
            if str(stub_dir) in sys.path:
                sys.path.remove(str(stub_dir))
            shutil.rmtree(temp_dir, ignore_errors=True)

    env = {"OTLP_GRPC_ENDPOINT": endpoint}
    if server_material is not None:
        env["OTLP_GRPC_TLS_CERTFILE"] = str(server_material[3])
    if client_material is not None:
        env["OTLP_GRPC_TLS_CLIENT_CA"] = str(client_material[0])

    return FixtureHandle(
        env=env,
        teardown=_teardown,
        hooks={"assert_test": _assert_test},
    )
