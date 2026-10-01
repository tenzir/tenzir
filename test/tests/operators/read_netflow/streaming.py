# runner: python

import json
import os
import select
import shlex
import struct
import subprocess


binary = [*shlex.split(os.environ["TENZIR_BINARY"]), "--bare-mode", "--nova=true"]
v5 = bytes.fromhex(
    "0005000100002710000003e8000000000000002a01024005"
    "0102030405060708090a0b0c000d000e0000000f00000010"
    "000023280000251c00110012001211140015001617180000"
)
ipfix = bytes.fromhex(
    "000a001c0000000000000000000000070002000c0100000100080004"
    "000a001800000001000000000000000701000008c0000201"
)
v9 = struct.pack("!HHIIII", 9, 2, 100, 1, 1, 7) + bytes.fromhex(
    "0000000c010000010008000401000008c0000201"
)

# Flush both complete messages and length-ambiguous v9 prefixes without EOF.
for reader in ("read_netflow", "read_auto"):
    for version, source, address in (
        (5, v5, "1.2.3.4"),
        (9, v9, "192.0.2.1"),
        (10, ipfix, "192.0.2.1"),
    ):
        process = subprocess.Popen(
            [
                *binary,
                f"load_stdin | split_bytes 1 | {reader} "
                "| select version=netflow.version, source_ipv4_address "
                "| write_ndjson",
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            process.stdin.write(source)
            process.stdin.flush()
            ready, _, _ = select.select([process.stdout], [], [], 10)
            assert ready, f"{reader} did not flush v{version} before EOF"
            assert json.loads(process.stdout.readline()) == {
                "version": version,
                "source_ipv4_address": address,
            }
            stdout, stderr = process.communicate(timeout=10)
            assert process.returncode == 0 and not stderr, stderr
            assert not stdout, stdout
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()

# A full batch must not lose the records on either side of its size boundary.
result = subprocess.run(
    [*binary, "load_stdin | read_netflow | measure | select events | write_ndjson"],
    input=v5 * 8193,
    capture_output=True,
    timeout=20,
)
assert result.returncode == 0 and not result.stderr, result.stderr
sizes = [json.loads(line)["events"] for line in result.stdout.splitlines()]
assert sum(sizes) == 8193 and all(0 < size <= 8192 for size in sizes), sizes

print("streaming deadlines, detection, and batch boundaries: ok")
