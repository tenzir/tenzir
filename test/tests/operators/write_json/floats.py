import os
import shlex
import subprocess
import sys


BINARY = shlex.split(os.environ["TENZIR_BINARY"])
# Prepare non-finite values with Arrow until float() has a Nova implementation.
source = subprocess.run(
    [
        *BINARY,
        "--bare-mode",
        "--nova=false",
        'from {a: 0.0, b: 5483819555176798000.float(), c: "inf".float(), '
        'd: "nan".float(), e: 0.000000001} | write_feather',
    ],
    capture_output=True,
    timeout=15,
)
assert source.returncode == 0 and not source.stderr, source.stderr
result = subprocess.run(
    [*BINARY, "--bare-mode", "--nova=true", "load_stdin | read_feather | write_json"],
    input=source.stdout,
    capture_output=True,
    timeout=15,
)
assert result.returncode == 0 and not result.stderr, result.stderr
sys.stdout.buffer.write(result.stdout)
