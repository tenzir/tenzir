# runner: python

import os
from pathlib import Path
import shlex
import subprocess

# Keep stdin open: head must stop upstream without waiting for end-of-input.
# measure prevents the head limit from being pushed into the Feather reader.
command = [*shlex.split(os.environ["TENZIR_BINARY"]), "--bare-mode", "--nova=true"]
pipeline = "load_stdin\nread_feather\nmeasure\nselect\nhead 1\nwrite_json compact=true"
source = Path(os.environ["TENZIR_INPUTS"]) / "feather/concatenated_streams.feather"
with subprocess.Popen(
    [*command, pipeline],
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
) as process:
    try:
        process.stdin.write(source.read_bytes())
        process.stdin.flush()
        returncode = process.wait(timeout=10)
        output = process.stdout.read()
        error = process.stderr.read()
        assert returncode == 0, error.decode()
        assert not error, error.decode()
        assert output == b"{}\n", output
        print(output.decode(), end="")
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
