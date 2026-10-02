import os
import shlex
import subprocess


result = subprocess.run(
    [
        *shlex.split(os.environ["TENZIR_BINARY"]),
        "--bare-mode",
        "--nova=true",
        "--parallelism=6",
        "--dump-ir-plan",
        "-f",
        os.environ["TENZIR_INPUT"],
    ],
    capture_output=True,
    text=True,
    timeout=10,
)
assert result.returncode == 0, result.stderr
assert not result.stderr, result.stderr
# Keep the full plan assertion, including six replicated sinks, independent
# of queue capacity: Nova routing may render the incoming edge as tiny.
print(result.stdout.replace("╎", "│"), end="")
