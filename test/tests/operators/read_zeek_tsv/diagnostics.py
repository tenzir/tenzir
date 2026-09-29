# runner: python

import os
import shlex
import subprocess


binary = [*shlex.split(os.environ["TENZIR_BINARY"]), "--bare-mode", "--nova=true"]
cases = [
    (b"#fields\tx\n#types\tstring\na\n", "missing #path", "line 3"),
    (b"#path\ttest\n#types\tstring\na\n", "missing #fields", "line 3"),
    (
        b"#path\ttest\n#fields\tx\ty\n#types\tstring\na\tb\n",
        "mismatching number #fields and #types",
        "line 4",
    ),
    (b"#path\ttest\n#fields\tx\tx\n", "duplicate #field name `x`", "line 2"),
    (
        b"#path\ttest\n#fields\tx\n#types\tbool\ninvalid\n",
        "failed to parse Zeek value at index 0",
        "line 4",
    ),
    (
        b"#path\ttest\n#fields\tx\ty\n#types\tcount\tcount\n1 2\n",
        "failed to parse Zeek separator at index 0",
        "line 4",
    ),
    (
        b"#path\ttest\n#fields\tx\ty\n#types\tcount\tcount\n1\tbad\n",
        "failed to parse Zeek value at index 1",
        "line 4",
    ),
]
for source, message, location in cases:
    result = subprocess.run(
        [*binary, "load_stdin | read_zeek_tsv | write_ndjson"],
        input=source,
        capture_output=True,
        timeout=20,
    )
    assert result.returncode != 0, (message, result.stdout)
    diagnostic = result.stderr.decode()
    assert message in diagnostic and location in diagnostic, diagnostic
    assert not result.stdout, result.stdout
