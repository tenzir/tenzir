# fixtures: [{tcp: {mode: server, payload: ""}}]
# assertions: {fixtures: {tcp: {server_received_equals_hex: "7b2278223a317d007b2278223a327d00"}}}
# timeout: 60

import os
import shlex
import socket
import subprocess


result = subprocess.run(
    [
        *shlex.split(os.environ["TENZIR_BINARY"]),
        "--bare-mode",
        "--nova=true",
        r'load_stdin | read_lines | write_delimited line, "\x00"',
    ],
    input=b'{"x":1}\n{"x":2}\n',
    capture_output=True,
    timeout=15,
)
assert result.returncode == 0 and not result.stderr, result.stderr
assert result.stdout == b'{"x":1}\x00{"x":2}\x00', result.stdout
host, port = os.environ["TCP_ENDPOINT"].rsplit(":", 1)
with socket.create_connection((host, int(port)), timeout=10) as connection:
    connection.sendall(result.stdout)
