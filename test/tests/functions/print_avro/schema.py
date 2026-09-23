# runner: python

import base64
from collections import Counter
import json
import os
import shlex
import shutil
import subprocess


binary = shlex.split(
    os.environ.get("TENZIR_BINARY", shutil.which("tenzir") or "tenzir")
)


def record(**fields):
    return {
        "type": "record",
        "name": "event",
        "fields": [{"name": name, "type": type_} for name, type_ in fields.items()],
    }


def array(items):
    return {"type": "array", "items": items}


def run(mode, expression):
    return subprocess.run(
        [*binary, *mode, "from {} | this = " + expression + " | write_json"],
        # Nova buffers cross the plugin boundary; tracking must use the same
        # allocator state in the plugin and the core library.
        env={**os.environ, "TENZIR_ALLOC_ACTOR_STATS": "1"},
        text=True,
        capture_output=True,
        timeout=30,
    )


def check_batch(mode, cases, schema_argument, *, failure=False):
    fields = ", ".join(
        f"case{i}: print_avro({value}, schema={schema_argument(schema)})"
        for i, (value, schema, _) in enumerate(cases)
    )
    result = run(mode, "{" + fields + "}")
    context = (mode, fields, result.returncode, result.stdout, result.stderr)
    assert result.returncode == 0, context
    assert json.loads(result.stdout) == {
        f"case{i}": None
        if failure
        else base64.b64encode(bytes.fromhex(expected)).decode()
        for i, (_, _, expected) in enumerate(cases)
    }, context
    if failure:
        assert result.stderr.count("failed to print Avro value") == len(cases), context
        for warning, count in Counter(warning for _, _, warning in cases).items():
            assert result.stderr.count(warning) >= count, context
    else:
        assert not result.stderr, context


# Each case has a TQL value, a schema, and hand-derived Avro binary bytes.
# JSON renders blobs as base64, avoiding dependencies on encode_hex or a decoder.
successes = [
    ('{a: 1, b: "x"}', record(b="string", a="long"), "027802"),
    ('{b: "x", a: 1}', record(b="string", a="long"), "027802"),
    ("null", {"type": "null"}, ""),
    ("true", {"type": "boolean"}, "01"),
    ("null", {"type": ["null", "string"]}, "00"),
    ('"hi"', {"type": ["null", "string"]}, "02046869"),
    ("null", {"type": ["string", "null"]}, "02"),
    ('"hi"', {"type": ["string", "null"]}, "00046869"),
    ("{a: null}", record(a=["null", "long"]), "00"),
    ("[null, 1, null]", array(["null", "long"]), "060002020000"),
    ("[]", array("long"), "00"),
    ("[[], [1, 2], []]", array(array("long")), "0600040204000000"),
    ("[{a: 1}, {a: 2}]", array(record(a="long")), "04020400"),
    ('"green"', {"type": "enum", "name": "color", "symbols": ["red", "green"]}, "02"),
    ('b"ab"', {"type": "fixed", "name": "pair", "size": 2}, "6162"),
    ('b"ab"', {"type": "bytes"}, "046162"),
    ("{}", {"type": "map", "values": "long"}, "00"),
    ("{a: 1}", {"type": "map", "values": "long"}, "0202610200"),
    ("{a: null}", {"type": "map", "values": ["null", "long"]}, "0202610000"),
    ("-2147483648", {"type": "int"}, "ffffffff0f"),
    ("2147483647", {"type": "int"}, "feffffff0f"),
    ("-9223372036854775808", {"type": "long"}, "ffffffffffffffffff01"),
    ("9223372036854775807", {"type": "long"}, "feffffffffffffffff01"),
    ("9223372036854775808", {"type": "float"}, "0000005f"),
    ("9223372036854775808", {"type": "double"}, "000000000000e043"),
    ("18446744073709551615", {"type": "float"}, "0000805f"),
    ("18446744073709551615", {"type": "double"}, "000000000000f043"),
    # Validation must skip the long branch and select the floating-point one.
    ("18446744073709551615", {"type": ["long", "float"]}, "020000805f"),
    ("18446744073709551615", {"type": ["long", "double"]}, "02000000000000f043"),
    ("1", {"type": "float"}, "0000803f"),
    ("1.5", {"type": "double"}, "000000000000f83f"),
    # A failed union candidate must not leak its partially encoded fields.
    (
        "{a: 1, b: 2}",
        {
            "type": [
                {
                    "type": "record",
                    "name": "first",
                    "fields": [
                        {"name": "a", "type": "long"},
                        {"name": "b", "type": "string"},
                    ],
                },
                record(a="long", b="long"),
            ]
        },
        "020204",
    ),
]

failures = [
    ("{}", record(a="long"), "missing Avro record field `a`"),
    ("{a: 1, b: 2}", record(a="long"), "unexpected Avro record field `b`"),
    ('{a: "x"}', record(a="long"), "field `a`: value does not match Avro type `long`"),
    ("{a: null}", record(a="long"), "value does not match Avro type `long`"),
    (
        "true",
        {"type": ["null", "string"]},
        "value does not match any Avro union branch",
    ),
    ('[["x"]]', array(array("long")), "value does not match Avro type `long`"),
    (
        '[{a: ["x"]}]',
        array(record(a=array("long"))),
        "field `a`: value does not match Avro type `long`",
    ),
    ("[{b: 1}]", array(record(a="long")), "unexpected Avro record field `b`"),
    ("[{}]", array(record(a="long")), "missing Avro record field `a`"),
    (
        '"blue"',
        {"type": "enum", "name": "color", "symbols": ["red", "green"]},
        "unknown Avro enum symbol `blue`",
    ),
    (
        'b"a"',
        {"type": "fixed", "name": "pair", "size": 2},
        "blob size does not match the Avro fixed size",
    ),
    (
        '"ab"',
        {"type": "fixed", "name": "pair", "size": 2},
        "value does not match Avro type `fixed`",
    ),
    (
        '{a: "x"}',
        {"type": "map", "values": "long"},
        "value does not match Avro type `long`",
    ),
    ("-2147483649", {"type": "int"}, "integer exceeds the Avro `int` range"),
    ("2147483648", {"type": "int"}, "integer exceeds the Avro `int` range"),
    (
        "18446744073709551615",
        {"type": "long"},
        "unsigned integer exceeds the Avro `long` range",
    ),
    (
        "18446744073709551615",
        {"type": "int"},
        "unsigned integer exceeds the Avro `long` range",
    ),
    ("1e100", {"type": "float"}, "number exceeds the Avro `float` range"),
    ("1.5", {"type": "long"}, "value does not match Avro type `long`"),
]

for mode in ([], ["--nova"]):
    label = "nova" if mode else "legacy"
    for as_string in (False, True):

        def schema_argument(schema):
            # A record wrapping a root union is a convenience accepted by the
            # function; a JSON schema string uses the standard bare array.
            if (
                as_string
                and list(schema) == ["type"]
                and isinstance(schema["type"], list)
            ):
                schema = schema["type"]
            result = json.dumps(schema)
            return json.dumps(result) if as_string else result

        check_batch(mode, successes, schema_argument)
        check_batch(mode, failures, schema_argument, failure=True)

    for expression, diagnostic in [
        ("print_avro(1)", "schema"),
        ("print_avro(1, schema=42)", "schema"),
        ('print_avro(1, schema="not json")', "invalid Avro schema"),
        ('print_avro(1, schema={type: "unknown"})', "invalid Avro schema"),
        ('print_avro(1, schema={type: "array"})', "invalid Avro schema"),
    ]:
        result = run(mode, "{encoded: " + expression + "}")
        context = (label, expression, result.returncode, result.stdout, result.stderr)
        assert result.returncode != 0, context
        assert "error:" in result.stderr and diagnostic in result.stderr, context
        assert not result.stdout, context

    print(f"{label} schema-driven Avro encoding: ok")

# Legacy lists can coerce heterogeneous input before the function sees it.
# Nova preserves the individual element types, including nested records.
nova_successes = [
    ('[1, "x", null]', array(["null", "long", "string"]), "0602020402780000"),
    ('[{a: 1}, {a: "x"}]', array(record(a=["long", "string"])), "04000202027800"),
    ('[[], [1], ["x"]]', array(array(["long", "string"])), "060002000200020202780000"),
]
check_batch(["--nova"], nova_successes, json.dumps)

nova_failures = [
    ('[1, "x"]', array("long"), "value does not match Avro type `long`"),
    ('[[], [1], ["x"]]', array(array("long")), "value does not match Avro type `long`"),
    ('[{a: 1}, {a: "x"}]', array(record(a="long")), "field `a`"),
    ("[{a: 1}, {b: 2}]", array(record(a="long")), "unexpected Avro record field `b`"),
    ("[{a: 1}, {}]", array(record(a="long")), "missing Avro record field `a`"),
    (
        "[{a: 1}, {a: 2, b: 3}]",
        array(record(a="long")),
        "unexpected Avro record field `b`",
    ),
]
check_batch(["--nova"], nova_failures, json.dumps, failure=True)

print("nova heterogeneous Avro unions: ok")
