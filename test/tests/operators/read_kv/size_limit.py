# runner: python
# timeout: 30

import os
import subprocess


limit = 16 * 1024 * 1024
oversized = b"value=" + b"x" * (limit - 5)
for delimiter in (b"", b"\n", b"\r\n"):
    result = subprocess.run(
        [
            os.environ["TENZIR_BINARY"],
            "--bare-mode",
            "--nova=true",
            "from_stdin { read_kv }\ndiscard",
        ],
        input=b"value=First\nvalue=Second\n" + oversized + delimiter,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        timeout=20,
    )
    assert result.returncode == 1, result.stderr.decode()
    assert (
        result.stderr.count(b"key-value record exceeds maximum 16777216 bytes") == 1
    ), result.stderr.decode()
    assert b"line 3" in result.stderr, result.stderr.decode()
print("oversized records rejected")
