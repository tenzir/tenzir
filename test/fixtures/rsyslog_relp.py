from __future__ import annotations

import shutil
import socket
import tempfile
import threading
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

from tenzir_test import fixture
from tenzir_test.fixtures import FixtureUnavailable, current_options
from tenzir_test.fixtures.container_runtime import (
    ContainerCommandError,
    ManagedContainer,
    RuntimeSpec,
    detect_runtime,
    start_detached,
)

from ._utils import find_free_port

_DEFAULT_IMAGE = "docker.io/rsyslog/rsyslog-collector:2026-04"


@dataclass(frozen=True)
class RsyslogRelpOptions:
    image: str = _DEFAULT_IMAGE
    message: str = "<165>1 2026-08-16T12:00:00Z sender app 42 - - relp fixture"


def _write_config(directory: Path, target_port: int) -> Path:
    config = directory / "rsyslog.conf"
    config.write_text(
        f'''module(load="imtcp")
module(load="omrelp")

template(name="relp_raw" type="string" string="%rawmsg%")

ruleset(name="forward_relp") {{
  action(
    type="omrelp"
    target="host.docker.internal"
    port="{target_port}"
    template="relp_raw"
    action.resumeRetryCount="-1"
    queue.type="linkedList"
  )
}}

input(type="imtcp" port="1514" ruleset="forward_relp")
''',
        encoding="utf-8",
    )
    return config


def _start(
    runtime: RuntimeSpec, image: str, config: Path, input_port: int
) -> ManagedContainer:
    name = f"tenzir-test-rsyslog-relp-{uuid.uuid4().hex[:8]}"
    return start_detached(
        runtime,
        [
            "--rm",
            "--name",
            name,
            "--add-host",
            "host.docker.internal:host-gateway",
            "-p",
            f"{input_port}:1514",
            "-v",
            f"{config}:/etc/rsyslog.conf:ro",
            image,
        ],
    )


def _probe_host_connectivity(container: ManagedContainer, port: int) -> None:
    """Verify that the container can reach the host on the RELP port.

    Host firewalls commonly drop container-to-host traffic. omrelp retries
    failed connections silently and indefinitely, so without this probe the
    test would idle until its timeout instead of skipping. Bind the RELP port
    on the host for the duration of the probe and attempt one connection from
    inside the container. Only a probe that times out is conclusive: it means
    the host silently drops the traffic. Inconclusive probes (missing tools in
    the image, port unavailable) let the fixture proceed.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            listener.bind(("0.0.0.0", port))
        except OSError:
            return
        listener.listen(1)
        try:
            result = container.exec(
                [
                    "timeout",
                    "3",
                    "bash",
                    "-c",
                    f"exec 3<>/dev/tcp/host.docker.internal/{port}",
                ]
            )
        except ContainerCommandError:
            return
    if result.returncode in (124, 143):
        raise FixtureUnavailable(
            f"the rsyslog container cannot reach the host on port {port}; "
            "a host firewall is likely dropping container-to-host traffic"
        )


def _submit(port: int, message: str) -> None:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1) as sock:
                sock.sendall(message.encode() + b"\n")
                return
        except OSError:
            time.sleep(0.05)
    raise FixtureUnavailable("rsyslog container did not open its TCP input")


@fixture(options=RsyslogRelpOptions, tags=("container",))
def rsyslog_relp() -> Iterator[dict[str, str]]:
    options = current_options("rsyslog_relp")
    runtime = detect_runtime()
    if runtime is None:
        raise FixtureUnavailable(
            "a container runtime is required for the rsyslog RELP fixture"
        )
    relp_port = find_free_port()
    input_port = find_free_port()
    directory = Path(tempfile.mkdtemp(prefix="rsyslog-relp-"))
    config = _write_config(directory, relp_port)
    container: ManagedContainer | None = None
    try:
        try:
            container = _start(runtime, options.image, config, input_port)
        except ContainerCommandError as exc:
            raise FixtureUnavailable(
                f"failed to start the rsyslog RELP container: {exc}"
            ) from exc
        _probe_host_connectivity(container, relp_port)

        def _submit_after_start() -> None:
            time.sleep(1)
            _submit(input_port, options.message)

        worker = threading.Thread(target=_submit_after_start, daemon=True)
        worker.start()
        yield {"RSYSLOG_RELP_ENDPOINT": f"0.0.0.0:{relp_port}"}
    finally:
        if "worker" in locals():
            worker.join(timeout=5)
        if container is not None:
            container.stop()
        shutil.rmtree(directory, ignore_errors=True)
