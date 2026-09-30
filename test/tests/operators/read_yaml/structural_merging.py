# runner: python
# timeout: 120

import json
import os
import shlex
import subprocess


binary = [*shlex.split(os.environ["TENZIR_BINARY"]), "--bare-mode", "--nova=true"]
documents = [
    ("a: {x: 1}\na: {y: 2}\n", {"a": {"x": 1, "y": 2}}),
    (
        "a: {nested: {x: 1}}\na: {nested: {y: 2}}\na: {nested: {x: 3}}\n",
        {"a": {"nested": {"x": [1, 3], "y": 2}}},
    ),
    ("a: 1\na: {b: 2}\n", {"a": {"": 1, "b": 2}}),
    ("a: {b: 2}\na: 1\n", {"a": {"b": 2, "": 1}}),
    ("a: 1\na.b: 2\n", {"a": {"": 1, "b": 2}}),
    ("a.b: 2\na: 1\n", {"a": {"b": 2, "": 1}}),
    ("a: {x: 1}\na.y: 2\n", {"a": {"x": 1, "y": 2}}),
    ("a.x: 1\na: {y: 2}\n", {"a": {"x": 1, "y": 2}}),
    ("a: null\na: {b: 2}\n", {"a": {"": None, "b": 2}}),
    ("a: {b: 2}\na: null\n", {"a": {"b": 2, "": None}}),
    ("a: [1]\na: [2, 3]\n", {"a": [1, 2, 3]}),
    ("a: 1\na: [2, 3]\n", {"a": [1, 2, 3]}),
    ("a: [1, 2]\na: 3\n", {"a": [1, 2, 3]}),
    ("a: {x: 1}\na: {y: 2}\na: 3\n", {"a": {"x": 1, "y": 2, "": 3}}),
    # Collected values retain their types instead of being stringified.
    ("a: {x: 1}\na: {y: 2}\na: text\n", {"a": [{"x": 1, "y": 2}, "text"]}),
    ("a: [1, 2]\na: {b: 3}\n", {"a": [1, 2, {"b": 3}]}),
    ("a: {b: 1}\na: [2, 3]\n", {"a": [{"b": 1}, 2, 3]}),
]
source = "...\n".join(source for source, _ in documents).encode()
expected = [event for _, event in documents]
for batch_size in (1, 7):
    for raw in (False, True):
        result = subprocess.run(
            [
                *binary,
                f'load_stdin | split_bytes 7 | read_yaml unflatten_separator=".", '
                f"_batch_size={batch_size}, raw={str(raw).lower()} | write_ndjson",
            ],
            input=source,
            capture_output=True,
            timeout=20,
        )
        assert result.returncode == 0 and not result.stderr, result.stderr
        actual = [json.loads(line) for line in result.stdout.splitlines()]
        assert actual == expected, (batch_size, raw, actual)
        assert list(actual[3]["a"]) == ["b", ""], actual[3]
        assert list(actual[4]["a"]) == ["", "b"], actual[4]

print("repeated mappings, sequences, and scalar/record collisions: ok")
