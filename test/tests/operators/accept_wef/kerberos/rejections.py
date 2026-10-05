# runner: python
# timeout: 120
# fixtures: [{wef: {kerberos: true, steps: []}}]

"""The collector rejects clients that do not authenticate with Kerberos."""

from __future__ import annotations

import http.client
import os
import shutil
import tempfile
from pathlib import Path

from fixtures import wef_kerberos
from fixtures.wef_client import (
    WefClient,
    encode_body,
    event_xml,
    free_port,
    kerberos_collector_pipeline,
    start_collector,
)


def post(
    port: int, headers: dict[str, str], body: bytes = b""
) -> http.client.HTTPResponse:
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    connection.request(
        "POST", "/wsman/SubscriptionManager/WEC", body=body, headers=headers
    )
    response = connection.getresponse()
    response.read()
    connection.close()
    return response


def main() -> None:
    # The fixture runs the KDC and exports the keytab of the collector.
    kdc = wef_kerberos.shared_kdc()
    directory = Path(tempfile.mkdtemp(prefix="wef-kerberos-"))
    keytab = Path(os.environ["WEF_KEYTAB"])
    port = free_port()
    pipeline = kerberos_collector_pipeline(
        port,
        keytab,
        rest="head 1\nselect client=wef.client",
    )
    process = start_collector(pipeline, directory / "state")
    client = WefClient("127.0.0.1", port, None, "WIN10$", kerberos="kerberos")
    try:
        client.wait_until_listening(process=process)
        # Without credentials, the collector asks for Kerberos or SPNEGO.
        response = post(port, {"Content-Length": "0"})
        print(
            "unauthenticated:",
            response.status,
            response.headers.get_all("WWW-Authenticate"),
        )
        response = post(port, {"Authorization": "Kerberos AAAA", "Content-Length": "0"})
        print("invalid token:", response.status)
        response = post(
            port, {"Authorization": "Basic d2luOnBhc3M=", "Content-Length": "0"}
        )
        print("other scheme:", response.status)
        # A ticket for another service does not decrypt with the keytab.
        other_keytab = kdc.keytab(
            wef_kerberos.principal_of("WIN10$"), directory / "client.keytab"
        )
        kdc.keytab(f"HTTP/other@{wef_kerberos.REALM}", directory / "other.keytab")
        other = wef_kerberos.KerberosClient(
            wef_kerberos.principal_of("WIN10$"),
            other_keytab,
            service=f"HTTP/other@{wef_kerberos.REALM}",
        )
        response = post(
            port, {"Authorization": other.authorization(), "Content-Length": "0"}
        )
        print("ticket for another service:", response.status)
        other.close()
        # An authenticated connection must still encrypt its messages.
        response = client.enumerate()
        print("enumerate:", response.status)
        plain = client.post(
            "/wsman/SubscriptionManager/WEC",
            encode_body("<x/>"),
            encrypt=False,
        )
        print("unencrypted body:", plain.status)
        try:
            client.events("security", [event_xml(record=1)])
        except OSError:
            pass
        stdout, stderr = process.communicate(timeout=30)
        assert process.returncode == 0, stderr
        print(stdout.strip())
    finally:
        client.close()
        if process.poll() is None:
            process.kill()
            process.wait()
        shutil.rmtree(directory, ignore_errors=True)


if __name__ == "__main__":
    main()
