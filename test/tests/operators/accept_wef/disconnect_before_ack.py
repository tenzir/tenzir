# runner: python
# timeout: 60

"""A batch whose ack the client never sees arrives twice, not zero times.

The client hangs up only after the collector processed the batch, which shows
in the bookmark that Enumerate offers. The pipeline ends with the resent batch,
so its ack may get lost as well.
"""

from __future__ import annotations

import http.client
import shutil
import socket
import ssl
import tempfile
import time
from pathlib import Path
from xml.etree import ElementTree

from fixtures.wef_client import (
    free_port,
    ACTION_EVENTS,
    WefClient,
    bookmark_xml,
    collector_pipeline,
    encode_body,
    envelope,
    event_xml,
    remove_pki,
    start_collector,
    temporary_pki,
)


def canonical(xml: str | None) -> str | None:
    if xml is None:
        return None
    return ElementTree.canonicalize(xml_data=xml, strip_text=True)


def wait_for_bookmark(client: WefClient, bookmark: str, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        client.enumerate()
        offered = client.subscriptions["security"].bookmark
        if canonical(offered) == canonical(bookmark):
            return
        time.sleep(0.05)
    raise AssertionError("the collector did not process the batch")


def main() -> None:
    pki = temporary_pki()
    state = Path(tempfile.mkdtemp(prefix="wef-state-"))
    port = free_port()
    pipeline = collector_pipeline(
        port,
        pki,
        rest="head 2\nselect record=data.parse_winlog().System.EventRecordID",
    )
    process = start_collector(pipeline, state)
    client = WefClient("127.0.0.1", port, pki)
    try:
        client.wait_until_listening(process=process)
        client.enumerate()
        subscription = client.subscriptions["security"]
        bookmark = bookmark_xml({"Security": 7})
        _, xml = envelope(
            ACTION_EVENTS,
            subscription.address,
            headers=(
                f"<e:Identifier>{subscription.identifier}</e:Identifier>"
                f"<Version>{subscription.version}</Version><w:AckRequested/>"
                f"<w:Bookmark>{bookmark}</w:Bookmark>"
            ),
            body=f"<w:Events><w:Event><![CDATA[{event_xml(record=7)}]]></w:Event></w:Events>",
        )
        body = encode_body(xml)
        # Send the batch, wait until the collector processed it, and hang up
        # without reading the ack.
        cert, key = pki.client(client.name)
        context = ssl.create_default_context(cafile=str(pki.ca_cert))
        context.load_cert_chain(str(cert), str(key))
        with socket.create_connection(("127.0.0.1", port)) as raw:
            with context.wrap_socket(raw, server_hostname="localhost") as tls:
                tls.sendall(
                    (
                        f"POST {subscription.path} HTTP/1.1\r\n"
                        "Host: localhost\r\n"
                        "Content-Type: application/soap+xml;charset=UTF-16\r\n"
                        f"Content-Length: {len(body)}\r\n\r\n"
                    ).encode()
                    + body
                )
                wait_for_bookmark(client, bookmark, timeout=30)
        # Like Windows, send the batch again because it was never acked. The
        # pipeline ends once the collector emits it, which may cut the ack off.
        try:
            client.events("security", [event_xml(record=7)])
        except (OSError, http.client.HTTPException):
            pass
        stdout, stderr = process.communicate(timeout=30)
        assert process.returncode == 0, stderr
        print(stdout.strip())
    finally:
        client.close()
        if process.poll() is None:
            process.kill()
            process.wait()
        remove_pki(pki)
        shutil.rmtree(state, ignore_errors=True)


if __name__ == "__main__":
    main()
