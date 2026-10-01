"""A mock Velociraptor API server.

The server expects mutual TLS and answers `Query` calls with the stream that is
registered for the VQL of the request. The VQL `echo` returns the parameters of
the request instead, and a `subscribe` query returns the subscribed artifact.

It binds a free port and writes it to the ready file. It runs in a process of
its own because gRPC installs fork handlers that write to stderr whenever the
test runner starts a process.
"""

from __future__ import annotations

import argparse
import importlib
import json
import re
import signal
import sys
import tempfile
import threading
from concurrent import futures
from pathlib import Path
from typing import Any

import grpc
import grpc_tools
from grpc_tools import protoc

_HOST = "127.0.0.1"

_TIMESTAMP = 1704067200000000  # 2024-01-01T00:00:00Z in microseconds.


def _data(rows: Any, *, part: int = 0) -> dict[str, Any]:
    """A data message: a JSON array of rows, or arbitrary text."""
    return {
        "Response": rows if isinstance(rows, str) else json.dumps(rows),
        "part": part,
        "timestamp": _TIMESTAMP + part,
        "total_rows": len(rows) if isinstance(rows, list) else 0,
    }


def _log(message: str, *, part: int = 0) -> dict[str, Any]:
    """A control message: a log line without a response payload."""
    return {"log": message, "part": part, "timestamp": _TIMESTAMP + part}


# Predefined streams keyed by the VQL that the operator sends. Every entry is
# the list of messages the server streams back before it ends the call.
_STATIC_RESPONSES: dict[str, list[dict[str, Any]]] = {
    # Scalars of every JSON type.
    "select_basic": [
        _data(
            [
                {"user": "alice", "id": 1, "score": 1.5, "admin": True, "note": None},
                {"user": "bob", "id": 2, "score": -2.25, "admin": False, "note": "x"},
            ]
        ),
    ],
    # A column whose type changes from row to row, next to rows with
    # different sets of fields.
    "select_mixed_column": [
        _data(
            [
                {"value": 1},
                {"value": "two"},
                {"value": None},
                {"value": 2.5},
                {"other": True},
                {"value": [1, "a"], "other": False},
            ]
        ),
    ],
    # Records and lists nested in the response, including a list that mixes
    # types and a list of records with different fields.
    "select_nested": [
        _data(
            [
                {
                    "host": {"name": "web-01", "os": {"family": "linux"}},
                    "tags": ["a", "b"],
                    "procs": [{"pid": 1}, {"pid": 2, "name": "init"}],
                    "mixed": [1, "two", None, {"k": "v"}],
                    "empty_list": [],
                    "empty_record": {},
                },
            ]
        ),
    ],
    # A control message.
    "select_log": [_log("Starting query")],
    # A stream of several messages. The empty data message yields no events.
    "select_parts": [
        _data([{"n": 1}, {"n": 2}], part=0),
        _log("Halfway there", part=1),
        _data([], part=2),
        _data([{"n": 3}], part=3),
    ],
    # Messages that cannot be parsed are skipped with a warning. Identical
    # warnings are only shown once, so each kind has a stream of its own.
    "select_invalid_json": [
        _data("this is not JSON", part=0),
        _data([{"n": 1}], part=1),
    ],
    "select_invalid_array": [
        _data({"n": 0}, part=0),
        _data([{"n": 1}], part=1),
    ],
    "select_invalid_element": [
        _data([1], part=0),
        _data([{"n": 1}], part=1),
    ],
    "select_invalid_empty": [
        {"part": 0, "timestamp": _TIMESTAMP},
        _data([{"n": 1}], part=1),
    ],
    # The call fails.
    "select_denied": [],
    # An empty stream.
    "select_empty": [],
}

_DENIED = "select_denied"

_SUBSCRIBE = re.compile(r'LET subscribe_artifact = "([^"]*)"')


def _compile_stubs(output_dir: Path) -> None:
    proto_root = Path(__file__).resolve().parents[3] / "plugins/from_velociraptor"
    grpc_tools_include = Path(grpc_tools.__file__).parent / "_proto"
    args = [
        "grpc_tools.protoc",
        f"--proto_path={proto_root}",
        f"--proto_path={grpc_tools_include}",
        f"--python_out={output_dir}",
        f"--grpc_python_out={output_dir}",
        str(proto_root / "velociraptor.proto"),
    ]
    if protoc.main(args) != 0:
        raise RuntimeError("failed to compile the Velociraptor gRPC stubs")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--server-key", type=Path, required=True)
    parser.add_argument("--server-cert", type=Path, required=True)
    parser.add_argument("--client-cert", type=Path, required=True)
    parser.add_argument("--ready-file", type=Path, required=True)
    args = parser.parse_args()

    with tempfile.TemporaryDirectory(prefix="velociraptor-stubs-") as stub_dir:
        _compile_stubs(Path(stub_dir))
        sys.path.insert(0, stub_dir)
        pb2 = importlib.import_module("velociraptor_pb2")
        pb2_grpc = importlib.import_module("velociraptor_pb2_grpc")

        class Servicer(pb2_grpc.APIServicer):
            def Query(self, request: Any, context: Any) -> Any:
                for query in request.Query:
                    for spec in self._responses(request, query, context):
                        yield pb2.VQLResponse(
                            Query=pb2.VQLRequest(Name=query.Name, VQL=query.VQL),
                            query_id=1,
                            **spec,
                        )
                    if query.VQL == _DENIED:
                        context.abort(
                            grpc.StatusCode.PERMISSION_DENIED, "access denied"
                        )

            def _responses(
                self, request: Any, query: Any, context: Any
            ) -> list[dict[str, Any]]:
                vql = query.VQL
                if vql == "echo":
                    return [
                        _data(
                            [
                                {
                                    "name": query.Name,
                                    "max_row": request.max_row,
                                    "max_wait": request.max_wait,
                                    "org_id": request.org_id,
                                    "queries": len(request.Query),
                                }
                            ]
                        )
                    ]
                if subscribed := _SUBSCRIBE.search(vql):
                    return [_data([{"artifact": subscribed.group(1)}])]
                if vql not in _STATIC_RESPONSES:
                    context.abort(
                        grpc.StatusCode.INVALID_ARGUMENT, f"unknown VQL: {vql!r}"
                    )
                return _STATIC_RESPONSES[vql]

        credentials = grpc.ssl_server_credentials(
            [(args.server_key.read_bytes(), args.server_cert.read_bytes())],
            root_certificates=args.client_cert.read_bytes(),
            require_client_auth=True,
        )
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
        pb2_grpc.add_APIServicer_to_server(Servicer(), server)
        port = server.add_secure_port(f"{_HOST}:0", credentials)
        if port == 0:
            print(f"cannot bind a port on {_HOST}", file=sys.stderr)
            return 1
        server.start()
        # Publish the port atomically so that readers never see a partial file.
        partial = args.ready_file.with_name(args.ready_file.name + ".partial")
        partial.write_text(str(port))
        partial.replace(args.ready_file)

        stop = threading.Event()
        signal.signal(signal.SIGTERM, lambda *_: stop.set())
        signal.signal(signal.SIGINT, lambda *_: stop.set())
        stop.wait()
        server.stop(grace=1).wait(timeout=5)
    return 0


if __name__ == "__main__":
    sys.exit(main())
