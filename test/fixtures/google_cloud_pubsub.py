"""Exercise the production Pub/Sub client against a local gRPC service."""

from __future__ import annotations

import importlib
import json
import sys
import tempfile
import threading
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from tenzir_test import fixture
from tenzir_test.fixtures import FixtureUnavailable


@fixture
def google_cloud_pubsub() -> Iterator[dict[str, str]]:
    try:
        import grpc
        import grpc_tools
        from google.protobuf.empty_pb2 import Empty
        from grpc_tools import protoc
    except ImportError as error:
        raise FixtureUnavailable("grpcio/grpcio-tools not installed") from error

    with tempfile.TemporaryDirectory(prefix="tenzir-pubsub-") as directory:
        root = Path(directory)
        proto = Path(__file__).parent / "proto" / "pubsub_test.proto"
        if protoc.main(
            [
                "grpc_tools.protoc",
                f"-I{proto.parent}",
                f"-I{Path(grpc_tools.__file__).parent / '_proto'}",
                f"--python_out={root}",
                f"--grpc_python_out={root}",
                str(proto),
            ]
        ):
            raise RuntimeError("failed to compile Pub/Sub fixture protocol")
        sys.path.insert(0, directory)
        try:
            pb = importlib.import_module("pubsub_test_pb2")
            rpc = importlib.import_module("pubsub_test_pb2_grpc")
        finally:
            sys.path.remove(directory)
        capture = root / "published.jsonl"
        capture.touch()
        lock = threading.Lock()

        class Subscriber(rpc.SubscriberServicer):
            def GetSubscription(self, request, context):
                return pb.Subscription(
                    name=request.subscription,
                    topic="projects/project/topics/topic",
                    ack_deadline_seconds=60,
                    enable_message_ordering=True,
                )

            def StreamingPull(self, requests, context):
                closed = threading.Event()
                context.add_callback(closed.set)
                next(requests)
                response = pb.StreamingPullResponse()
                for index, data in enumerate([b"first", b"", b"third"]):
                    received = response.received_messages.add(ack_id=str(index))
                    received.message.data = data
                    received.message.message_id = str(index)
                    received.message.publish_time.seconds = 1704067200 + index
                    received.message.ordering_key = "ordered"
                    received.message.attributes["literal.key"] = "42"
                yield response
                closed.wait()

            def Acknowledge(self, request, context):
                return Empty()

            def ModifyAckDeadline(self, request, context):
                return Empty()

        class Publisher(rpc.PublisherServicer):
            def Publish(self, request, context):
                with lock, capture.open("a") as output:
                    for message in request.messages:
                        output.write(
                            json.dumps(
                                {
                                    "topic": request.topic,
                                    "message": message.data.decode(),
                                }
                            )
                            + "\n"
                        )
                return pb.PublishResponse(
                    message_ids=[str(i) for i in range(len(request.messages))]
                )

        with ThreadPoolExecutor(max_workers=4) as executor:
            server = grpc.server(executor)
            rpc.add_SubscriberServicer_to_server(Subscriber(), server)
            rpc.add_PublisherServicer_to_server(Publisher(), server)
            port = server.add_insecure_port("127.0.0.1:0")
            server.start()
            try:
                yield {
                    "PUBSUB_EMULATOR_HOST": f"127.0.0.1:{port}",
                    "PUBSUB_CAPTURE": str(capture),
                }
            finally:
                server.stop(0).wait()
