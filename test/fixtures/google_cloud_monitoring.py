"""Mock Cloud Monitoring API for to_google_cloud_monitoring integration tests.

The server implements the parts of the Cloud Monitoring v3 REST API that the
operator uses, an OAuth token endpoint, and the GCE metadata server. Metric
types whose path contains a marker segment trigger special behavior:

- `/reject/`: `timeSeries.create` rejects these points with a partial-success
  `CreateTimeSeriesSummary`.
- `/flaky/`: the first `timeSeries.create` attempt fails with HTTP 503.
- `/race/`: descriptor creation answers HTTP 409, as if another writer won.
- `/grow/`: right after creating the descriptor, another writer adds the label
  `extra` to it.
- `/full/`: the descriptor starts with the 29 labels `l01` to `l29`, and right
  after the first lookup another writer adds `extra` as the 30th label.
- `/broken/`: descriptor lookups fail with HTTP 500.
- `/quota/`: descriptor creation fails with HTTP 429, as with an exhausted
  descriptor quota.

Descriptor creation also fails with HTTP 400 for the unit `invalid-unit`.

Like Cloud Monitoring, the mock rejects time series that lack a label of their
metric descriptor and adds new labels of written time series to it. It also
rejects time series whose resource `project_id` differs from the project in the
request. Besides the project of the GCE instance, it accepts writes to
`other-project`.
"""

from __future__ import annotations

import json
import re
import shutil
import tempfile
import threading
from dataclasses import dataclass, field as dataclass_field
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from tenzir_test import FixtureHandle, fixture

from .google_secops import _service_credentials

_HOST = "127.0.0.1"
_TOKEN = "test-monitoring-token"
_PROJECT = "test-project"
_INSTANCE_ID = "1234567890"
_ZONE = "projects/123456/zones/europe-west1-b"
_PROJECTS = {_PROJECT, "other-project"}
_PROJECT_PATH = re.compile(r"^/v3/projects/([^/]+)/(.+)$")
_PRESEEDED_DESCRIPTORS = {
    "custom.googleapis.com/tenzir/existing_int": {
        "metricKind": "GAUGE",
        "valueType": "INT64",
        "labels": [{"key": "region", "valueType": "STRING"}],
    },
    "custom.googleapis.com/tenzir/existing_dotted": {
        "metricKind": "GAUGE",
        "valueType": "DOUBLE",
        "labels": [{"key": "service.name", "valueType": "STRING"}],
    },
    "custom.googleapis.com/tenzir/existing_cumulative": {
        "metricKind": "CUMULATIVE",
        "valueType": "DOUBLE",
    },
}


@dataclass(frozen=True)
class GoogleCloudMonitoringAssertions:
    # Full requests per test, in arrival order.
    requests: dict[str, list[dict[str, object]]] = dataclass_field(default_factory=dict)
    # Number of time series per `timeSeries.create` request, per test.
    series_counts: dict[str, list[int]] = dataclass_field(default_factory=dict)
    # Number of time series that the mock wrote, per test.
    written_series: dict[str, int] = dataclass_field(default_factory=dict)


@dataclass
class State:
    capture: Path
    lock: threading.Lock = dataclass_field(default_factory=threading.Lock)
    descriptors: dict[str, dict[str, object]] = dataclass_field(default_factory=dict)
    flaky_attempts: dict[str, int] = dataclass_field(default_factory=dict)
    race_attempts: set[str] = dataclass_field(default_factory=set)
    full_grown: set[str] = dataclass_field(default_factory=set)
    written: int = 0

    def record(self, entry: dict[str, object]) -> None:
        with self.lock, self.capture.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry, sort_keys=True) + "\n")


def _make_handler(state: State):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_: object) -> None:
            return

        def _send(self, status: int, body: object, text: bool = False) -> None:
            payload = (body if text else json.dumps(body)).encode()
            self.send_response(status)
            self.send_header(
                "Content-Type", "text/plain" if text else "application/json"
            )
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def _error(self, status: int, message: str, details=None) -> None:
            error: dict[str, object] = {
                "code": status,
                "message": message,
                "status": HTTPStatus(status).phrase.upper().replace(" ", "_"),
            }
            if details is not None:
                error["details"] = details
            self._send(status, {"error": error})

        def _authorized(self) -> bool:
            if self.headers.get("Authorization") == f"Bearer {_TOKEN}":
                return True
            self._error(401, "Request had invalid authentication credentials.")
            return False

        def _body(self) -> bytes:
            return self.rfile.read(int(self.headers.get("Content-Length", "0")))

        def do_GET(self) -> None:  # noqa: N802
            if self.path.startswith("/computeMetadata/v1/"):
                self._metadata(self.path.removeprefix("/computeMetadata/v1/"))
                return
            if not self._authorized():
                return
            match = _PROJECT_PATH.match(self.path)
            prefix = "metricDescriptors/"
            if (
                match is None
                or match[1] not in _PROJECTS
                or not match[2].startswith(prefix)
            ):
                self._error(404, f"unknown path {self.path}")
                return
            metric_type = match[2].removeprefix(prefix)
            state.record({"method": "GET", "path": self.path})
            if "/broken/" in metric_type:
                self._error(500, "Internal error")
                return
            with state.lock:
                descriptor = state.descriptors.get(metric_type)
                if "/full/" in metric_type and metric_type not in state.full_grown:
                    labels = [
                        {"key": f"l{i:02}", "valueType": "STRING"} for i in range(1, 30)
                    ]
                    descriptor = {
                        "type": metric_type,
                        "metricKind": "GAUGE",
                        "valueType": "DOUBLE",
                        "labels": labels,
                    }
                    state.full_grown.add(metric_type)
                    state.descriptors[metric_type] = {
                        **descriptor,
                        "labels": [*labels, {"key": "extra", "valueType": "STRING"}],
                    }
            if descriptor is None:
                self._error(404, f"Could not find descriptor for {metric_type}")
                return
            self._send(200, descriptor)

        def do_POST(self) -> None:  # noqa: N802
            body = self._body()
            if self.path == "/token":
                self._send(
                    200,
                    {
                        "access_token": _TOKEN,
                        "expires_in": 3600,
                        "token_type": "Bearer",
                    },
                )
                return
            if not self._authorized():
                return
            if self.headers.get("Content-Type") != "application/json":
                self._error(400, "expected application/json")
                return
            payload = json.loads(body)
            state.record({"method": "POST", "path": self.path, "body": payload})
            match = _PROJECT_PATH.match(self.path)
            if match is None or match[1] not in _PROJECTS:
                self._error(404, f"Project not found: {self.path}")
                return
            resource = match[2]
            if resource == "metricDescriptors":
                self._create_descriptor(payload)
            elif resource == "timeSeries":
                self._create_time_series(match[1], payload)
            else:
                self._error(404, f"unknown path {self.path}")

        def _metadata(self, path: str) -> None:
            if self.headers.get("Metadata-Flavor") != "Google":
                self._error(403, "missing Metadata-Flavor header")
                return
            values = {
                "project/project-id": _PROJECT,
                "instance/id": _INSTANCE_ID,
                "instance/zone": _ZONE,
            }
            if path not in values:
                self._error(404, f"unknown metadata {path}")
                return
            self._send(200, values[path], text=True)

        def _create_descriptor(self, payload: dict[str, Any]) -> None:
            metric_type = payload.get("type", "")
            if "/quota/" in metric_type:
                self._error(429, "Quota exceeded for metric descriptors")
                return
            if payload.get("unit") == "invalid-unit":
                self._error(400, "The unit `invalid-unit` is invalid")
                return
            with state.lock:
                if "/race/" in metric_type and metric_type not in state.race_attempts:
                    # Another writer creates the descriptor first.
                    state.race_attempts.add(metric_type)
                    state.descriptors[metric_type] = payload
                    conflict = True
                else:
                    conflict = metric_type in state.descriptors
                    if not conflict:
                        state.descriptors[metric_type] = payload
                        if "/grow/" in metric_type:
                            state.descriptors[metric_type] = {
                                **payload,
                                "labels": [{"key": "extra", "valueType": "STRING"}],
                            }
            if conflict:
                self._error(409, f"descriptor {metric_type} already exists")
                return
            self._send(200, payload)

        def _create_time_series(self, project: str, payload: dict[str, Any]) -> None:
            series = payload.get("timeSeries", [])
            if not 0 < len(series) <= 200:
                self._error(400, f"expected 1 to 200 time series, got {len(series)}")
                return
            for entry in series:
                if len(entry.get("points", [])) != 1:
                    self._error(400, "each time series needs exactly one point")
                    return
            types = [entry["metric"]["type"] for entry in series]
            key = json.dumps(payload, sort_keys=True)
            if any("/flaky/" in t for t in types):
                with state.lock:
                    attempts = state.flaky_attempts.get(key, 0)
                    state.flaky_attempts[key] = attempts + 1
                if attempts == 0:
                    self._error(503, "The service is currently unavailable.")
                    return
            with state.lock:
                missing_labels = [
                    not self._has_descriptor_labels(entry)
                    or entry["resource"]["labels"].get("project_id", project) != project
                    for entry in series
                ]
                for entry, missing in zip(series, missing_labels):
                    if not missing:
                        self._add_descriptor_labels(entry)
            rejected = sum(
                "/reject/" in t or missing for t, missing in zip(types, missing_labels)
            )
            with state.lock:
                state.written += len(series) - rejected
            if rejected:
                summary = {
                    "@type": (
                        "type.googleapis.com/"
                        "google.monitoring.v3.CreateTimeSeriesSummary"
                    ),
                    "totalPointCount": len(series),
                    "successPointCount": len(series) - rejected,
                    "errors": [
                        {"status": {"code": 3}, "pointCount": rejected},
                    ],
                }
                self._error(
                    400,
                    "One or more TimeSeries could not be written",
                    details=[summary],
                )
                return
            self._send(200, {})

        @staticmethod
        def _has_descriptor_labels(entry: dict[str, Any]) -> bool:
            metric = entry["metric"]
            descriptor = state.descriptors.get(metric["type"], {})
            keys = {label["key"] for label in descriptor.get("labels", [])}
            labels = set(metric.get("labels", {}))
            return keys <= labels and len(keys | labels) <= 30

        @staticmethod
        def _add_descriptor_labels(entry: dict[str, Any]) -> None:
            metric = entry["metric"]
            descriptor = state.descriptors.get(metric["type"])
            if descriptor is None:
                return
            labels = descriptor.setdefault("labels", [])
            known = {label["key"] for label in labels}
            for key in sorted(set(metric.get("labels", {})) - known):
                labels.append({"key": key, "valueType": "STRING"})

    return Handler


@fixture(name="google_cloud_monitoring", assertions=GoogleCloudMonitoringAssertions)
def run() -> FixtureHandle:
    temp_dir = Path(tempfile.mkdtemp(prefix="google-cloud-monitoring-"))
    state = State(capture=temp_dir / "requests.jsonl")
    state.capture.touch()
    state.descriptors.update(
        {
            name: {"type": name, **descriptor}
            for name, descriptor in _PRESEEDED_DESCRIPTORS.items()
        }
    )
    server = ThreadingHTTPServer((_HOST, 0), _make_handler(state))
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    url = f"http://{_HOST}:{server.server_address[1]}"

    def teardown() -> None:
        server.shutdown()
        worker.join()
        server.server_close()
        shutil.rmtree(temp_dir, ignore_errors=True)

    def assert_test(
        *, test: Path, assertions: GoogleCloudMonitoringAssertions, **_: Any
    ) -> None:
        if not isinstance(assertions, GoogleCloudMonitoringAssertions):
            assertions = GoogleCloudMonitoringAssertions(**(assertions or {}))
        with state.lock:
            observed = [
                json.loads(line) for line in state.capture.read_text().splitlines()
            ]
            state.capture.write_text("")
            written = state.written
            state.written = 0
        expected = assertions.requests.get(test.name)
        if expected is not None and observed != expected:
            raise AssertionError(
                "Cloud Monitoring request mismatch\n"
                f"expected:\n{json.dumps(expected, indent=2, sort_keys=True)}\n"
                f"actual:\n{json.dumps(observed, indent=2, sort_keys=True)}"
            )
        expected_written = assertions.written_series.get(test.name)
        if expected_written is not None and written != expected_written:
            raise AssertionError(
                "Cloud Monitoring written series mismatch\n"
                f"expected: {expected_written}\nactual: {written}"
            )
        expected_counts = assertions.series_counts.get(test.name)
        if expected_counts is not None:
            counts = [
                len(entry["body"]["timeSeries"])
                for entry in observed
                if entry["path"].endswith("/timeSeries")
            ]
            if counts != expected_counts:
                raise AssertionError(
                    "Cloud Monitoring series count mismatch\n"
                    f"expected: {expected_counts}\nactual: {counts}"
                )

    return FixtureHandle(
        env={
            "GOOGLE_CLOUD_MONITORING_URL": url,
            "GOOGLE_CLOUD_MONITORING_METADATA_URL": url,
            "GOOGLE_CLOUD_MONITORING_CREDENTIALS": _service_credentials(f"{url}/token"),
            # Exchange the service account key for a token at the mock token
            # endpoint instead of signing the token locally.
            "GOOGLE_CLOUD_CPP_EXPERIMENTAL_DISABLE_SELF_SIGNED_JWT": "1",
        },
        teardown=teardown,
        hooks={"assert_test": assert_test},
    )
