# runner: python
# timeout: 120

"""Clients with the same name from different CAs keep separate bookmarks.

A collector that trusts several CAs, for example of several tenants, can see
the same host name twice. Each client must resume from its own bookmark.
"""

from __future__ import annotations

import shutil
import signal
import tempfile
from pathlib import Path

from fixtures.wef_client import (
    free_port,
    Pki,
    WefClient,
    collector_pipeline,
    event_xml,
    remove_pki,
    start_collector,
)

NAME = "win10.example.org"


def main() -> None:
    first = Pki.create(Path(tempfile.mkdtemp(prefix="wef-pki-")), "Tenant A CA")
    second = Pki.create(Path(tempfile.mkdtemp(prefix="wef-pki-")), "Tenant B CA")
    state = Path(tempfile.mkdtemp(prefix="wef-state-"))
    port = free_port()
    # Trust both CAs for client certificates.
    bundle = first.directory / "client-cas.pem"
    bundle.write_text(first.ca_cert.read_text() + second.ca_cert.read_text())
    pipeline = collector_pipeline(port, first).replace(
        f'client_ca: "{first.ca_cert}"', f'client_ca: "{bundle}"'
    )
    process = start_collector(pipeline, state)
    tenant_a = WefClient("127.0.0.1", port, first, NAME)
    # The client trusts the server certificate of the first PKI, but presents
    # a certificate of the second CA with the same name.
    tenant_b = WefClient("127.0.0.1", port, first, NAME)
    tenant_b._context.load_cert_chain(*map(str, second.client(NAME)))
    try:
        tenant_a.wait_until_listening(process=process)
        tenant_a.enumerate()
        response = tenant_a.events(
            "security", [event_xml(record=42)], bookmark={"Security": 42}
        )
        assert response.status == 200, response.status
        tenant_b.enumerate()
        print("other tenant:", tenant_b.subscriptions["security"].bookmark)
        tenant_a.enumerate()
        print("same tenant:", tenant_a.subscriptions["security"].bookmark)
        process.send_signal(signal.SIGTERM)
        stdout, stderr = process.communicate(timeout=30)
        assert process.returncode == 0, stderr
    finally:
        tenant_a.close()
        tenant_b.close()
        if process.poll() is None:
            process.kill()
            process.wait()
        remove_pki(first)
        remove_pki(second)
        shutil.rmtree(state, ignore_errors=True)


if __name__ == "__main__":
    main()
