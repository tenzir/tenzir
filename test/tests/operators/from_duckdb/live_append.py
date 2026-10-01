# runner: python
# timeout: 60

"""Live mode picks up rows that another pipeline in the same process appends.

The reader holds the database open for writing so that the writer in its
`fork` branch shares the instance. A reader alone never finishes, so this
test stops it once every expected event has arrived.
"""

import json
import os
import shlex
import subprocess
import time


def run(binary: list[str], pipeline: str) -> None:
    subprocess.run(
        [*binary, "--nova", "--bare-mode", "--console-verbosity=warning", pipeline],
        check=True,
        capture_output=True,
        text=True,
    )


def main() -> None:
    binary = shlex.split(os.environ["TENZIR_NODE_CLIENT_BINARY"])
    db = os.path.join(os.environ["DUCKDB_ROOT"], "live.duckdb")
    run(
        binary,
        f'from {{id: 1, v: "seed"}}, {{id: 2, v: "seed"}}\n'
        f'to_duckdb "{db}", table="t", primary=id',
    )
    # The fork branch turns the two seed rows into two new rows, which the live
    # reader must then see as well. The new rows must not trigger further
    # writes.
    pipeline = (
        f'from_duckdb "{db}", table="t", live=true\n'
        "fork {\n"
        '  where v == "seed"\n'
        "  id = id + 10\n"
        '  v = "live"\n'
        f'  to_duckdb "{db}", table="t"\n'
        "}\n"
        "write_json"
    )
    proc = subprocess.Popen(
        [*binary, "--nova", "--bare-mode", "--console-verbosity=warning", pipeline],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    output = ""
    deadline = time.monotonic() + 30
    try:
        while time.monotonic() < deadline:
            assert proc.stdout is not None
            line = proc.stdout.readline()
            if not line:
                break
            output += line
            # Stop once the fourth event has been written out completely.
            if output.count('"id"') >= 4 and line.strip() == "}":
                break
    finally:
        proc.terminate()
        proc.wait(timeout=20)
    events = []
    decoder = json.JSONDecoder()
    position = 0
    while position < len(output):
        while position < len(output) and output[position].isspace():
            position += 1
        if position >= len(output):
            break
        event, position = decoder.raw_decode(output, position)
        events.append(event)
    for event in sorted(events, key=lambda e: e["id"]):
        print(json.dumps(event, sort_keys=True))


if __name__ == "__main__":
    main()
