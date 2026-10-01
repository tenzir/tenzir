# timeout: 60

import json
import os
from pathlib import Path
import shlex
import subprocess


binary = shlex.split(os.environ["TENZIR_BINARY"])
schemas = Path(__file__).parent / "schemas"


def read(events, options, *, builtin=False):
    result = subprocess.run(
        [
            *binary,
            "--bare-mode",
            "--nova=true",
            *([] if builtin else [f"--schema-dirs={schemas}"]),
            f"load_stdin | read_ndjson {options} | write_ndjson",
        ],
        input="".join(json.dumps(event) + "\n" for event in events),
        text=True,
        capture_output=True,
        timeout=20,
    )
    assert result.returncode == 0, result.stderr
    assert not result.stderr, result.stderr
    return [json.loads(line) for line in result.stdout.splitlines()]


alert = {"event_type": "alert", "payload_printable": "2001:0db8::1"}
for events in [[alert], [alert, {"event_type": "dns"}]]:
    for batch_size in [1, 100]:
        rows = read(
            events,
            f'selector="event_type:suricata", _batch_size={batch_size}',
            builtin=True,
        )
        assert rows[0]["payload_printable"] == alert["payload_printable"], rows

first = {
    "kind": "a",
    "common": {"text": "2001:0db8::1", "first": "42", "extra": "1s"},
    "payload": "2001:0db8::1",
    "nested": {"second": "2001:0db8::2"},
}
second = {"kind": "b", "common": {"first": "7"}, "other": "2001:0db8::3"}
for schema_only in [False, True]:
    for batch_size in [1, 100]:
        options = (
            'selector="kind:builder", '
            f"schema_only={str(schema_only).lower()}, _batch_size={batch_size}"
        )
        alone = read([first], options)[0]
        mixed = read([first, second], options)
        assert mixed[0] == alone, mixed
        assert mixed[0]["payload"] == first["payload"], mixed
        assert mixed[0]["nested"]["second"] == first["nested"]["second"], mixed
        assert mixed[0]["common"]["first"] == 42, mixed
        assert mixed[0]["common"]["text"] == first["common"]["text"], mixed
        assert ("extra" in mixed[0]["common"]) != schema_only, mixed
        assert mixed[1]["common"] == {"first": 7, "text": None}, mixed
        assert mixed[1]["other"] == second["other"], mixed
