# runner: python
# timeout: 300

"""Client certificates must come from `client_ca`, not from the default CAs.

Tenzir defaults its CA certificates to `SSL_CERT_FILE` or the CA bundle of the
system, which contains public CAs. Trusting those for clients would let any
holder of a public certificate authenticate. A default that names a missing
file, as in Nix builds, must not stop the collector either.
"""

from __future__ import annotations

import http.client
import shutil
import ssl
import tempfile
from pathlib import Path

from fixtures.wef_client import (
    Pki,
    WefClient,
    collector_pipeline,
    free_port,
    remove_pki,
    start_collector,
)


def run(pki: Pki, other: Pki, default_cacert: str) -> None:
    state = Path(tempfile.mkdtemp(prefix="wef-state-"))
    port = free_port()
    process = start_collector(
        collector_pipeline(port, pki), state, env={"SSL_CERT_FILE": default_cacert}
    )
    trusted = WefClient("127.0.0.1", port, pki, "win10.example.org")
    stranger = WefClient("127.0.0.1", port, pki, "win10.example.org")
    stranger._context.load_cert_chain(*map(str, other.client("win10.example.org")))
    try:
        trusted.wait_until_listening(deadline=120, process=process)
        print("client_ca:", trusted.enumerate().status)
        try:
            print("default CA:", stranger.enumerate().status)
        except (OSError, ssl.SSLError, http.client.HTTPException):
            print("default CA: rejected")
    finally:
        trusted.close()
        stranger.close()
        process.kill()
        process.wait()
        shutil.rmtree(state, ignore_errors=True)


def main() -> None:
    pki = Pki.create(Path(tempfile.mkdtemp(prefix="wef-pki-")), "Private CA")
    other = Pki.create(Path(tempfile.mkdtemp(prefix="wef-pki-")), "Public CA")
    try:
        print("default bundle with another CA")
        run(pki, other, str(other.ca_cert))
        print("missing default bundle")
        run(pki, other, "/no-cert-file.crt")
    finally:
        remove_pki(pki)
        remove_pki(other)


if __name__ == "__main__":
    main()
