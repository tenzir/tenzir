"""Check availability of packet capture on the Linux loopback interface."""

from __future__ import annotations

import socket

from tenzir_test import FixtureHandle, fixture
from tenzir_test.fixtures import FixtureUnavailable


@fixture(name="nic")
def nic() -> FixtureHandle:
    if not hasattr(socket, "AF_PACKET"):
        raise FixtureUnavailable("loopback capture test requires Linux")
    try:
        with socket.socket(socket.AF_PACKET, socket.SOCK_RAW, socket.htons(3)) as sock:
            sock.bind(("lo", 0))
    except OSError as error:
        raise FixtureUnavailable(f"loopback capture unavailable: {error}") from error
    return FixtureHandle(env={"NIC_INTERFACE": "lo"})
