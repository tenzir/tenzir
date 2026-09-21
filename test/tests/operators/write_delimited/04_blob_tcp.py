# fixtures: [{tcp: {mode: server, payload: ""}}]
# assertions: {fixtures: {tcp: {server_received_equals_hex: "00666f6fff01626172ff"}}}
# timeout: 60

import os
import shlex
import socket
import subprocess


# Exercise the writer on Nova; the TCP transport has not been ported yet.
result = subprocess.run(
    [
        *shlex.split(os.environ["TENZIR_BINARY"]),
        "--bare-mode",
        "--nova=true",
        r'from {data: b"\x00foo"}, {data: b"\x01bar"} | write_delimited data, b"\xff"',
    ],
    capture_output=True,
    timeout=15,
)
assert result.returncode == 0 and not result.stderr, result.stderr
assert result.stdout == b"\x00foo\xff\x01bar\xff", result.stdout
host, port = os.environ["TCP_ENDPOINT"].rsplit(":", 1)
with socket.create_connection((host, int(port)), timeout=10) as connection:
    connection.sendall(result.stdout)
