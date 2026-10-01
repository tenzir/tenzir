# runner: python

"""TLS errors contain dynamic ports, so check their cause without a snapshot."""

import os
import shlex
import subprocess

binary = shlex.split(os.environ["TENZIR_NODE_CLIENT_BINARY"])
for connection in [
    'env("QUACK_TLS_URI"), tls=true',
    'env("QUACK_TLS_URI"), tls={cacert: env("QUACK_UNTRUSTED_CA")}',
    'env("QUACK_TLS_URI").replace("localhost", "127.0.0.1"), '
    'tls={cacert: env("QUACK_CA")}',
]:
    result = subprocess.run(
        [
            *binary,
            "--nova=true",
            "--bare-mode",
            "--console-verbosity=warning",
            f'from_duckdb {connection}, token=env("QUACK_TOKEN"), sql="SELECT 1"',
        ],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode != 0, result.stderr
    assert "failed to connect to Quack server" in result.stderr, result.stderr
    # Sandboxes without system roots report "SSL CA cert" instead of
    # "SSL peer certificate". The explicit unrelated CA tests trust rejection
    # independently of the host's default certificate store.
    assert "cert" in result.stderr.lower(), result.stderr
    assert os.environ["QUACK_TOKEN"] not in result.stderr, result.stderr
print("default trust, unrelated CA, and hostname mismatch: rejected")
