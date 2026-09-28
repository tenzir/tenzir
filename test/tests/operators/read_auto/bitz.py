import os
import shlex
import subprocess
import sys
from pathlib import Path


binary = shlex.split(os.environ["TENZIR_BINARY"])
inputs = Path(os.environ["TENZIR_INPUTS"])


def run(pipeline: str, payload: bytes = b"", *, nova: bool = True) -> bytes:
    result = subprocess.run(
        [*binary, "--bare-mode", f"--nova={str(nova).lower()}", pipeline],
        input=payload,
        capture_output=True,
        timeout=15,
    )
    assert result.returncode == 0 and not result.stderr, result.stderr
    return result.stdout


source = Path(__file__).with_suffix(".stdin").read_bytes()
legacy_frame = run("load_stdin | read_ndjson | write_bitz", source, nova=False)
sys.stdout.buffer.write(run("load_stdin | read_auto | write_json", legacy_frame))
sys.stdout.buffer.write(
    run(
        f'from_file "{inputs / "bitz" / "v2.bitz"}" {{ read_auto }} '
        "| schema = @name | import_time = @import_time | write_json"
    )
)
