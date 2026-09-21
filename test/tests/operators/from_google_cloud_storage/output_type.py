import os
import shlex
import subprocess


BINARY = [*shlex.split(os.environ["TENZIR_BINARY"]), "--nova=true"]
SOURCE = 'from_google_cloud_storage "gs://bucket/file"'


def plan(pipeline):
    return subprocess.run(
        [*BINARY, "--bare-mode", "--dump-ir-plan", pipeline],
        capture_output=True,
        text=True,
        timeout=10,
    )


for reader in ("read_lines", "read_json"):
    result = plan(f"{SOURCE} {{ {reader} }} | write_lines")
    assert result.returncode == 0, result.stderr
    assert not result.stderr, result.stderr
    assert "from_google_cloud_storage" in result.stdout, result.stdout
    assert "write_lines" in result.stdout, result.stdout

for reader in ("pass", "read_lines | write_lines"):
    result = plan(f"{SOURCE} {{ {reader} }}")
    assert result.returncode != 0, result.stdout
    assert "pipeline must not return bytes" in result.stderr, result.stderr

result = plan(f"from {{x: 1}} | {SOURCE} {{ read_lines }} | write_lines")
assert result.returncode != 0, result.stdout
assert "operator does not accept" in result.stderr, result.stderr

print("output types and validation: ok")
