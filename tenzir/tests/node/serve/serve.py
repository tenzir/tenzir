# test : python_node

from fixture_support import *
import json


def parse(v: bytes):
    return json.loads(v.decode())


def make_serve_query(serve_id: str, continuation_token: str, max_events: int) -> str:
    return f"""api "/serve", {{serve_id: "{serve_id}", continuation_token: "{continuation_token}", timeout: "5s", max_events: {max_events}, schema: "never" }} | write_ndjson"""


def test(serve_id: str, max_events: int):
    executor = Executor()
    token = ""
    print(f"testing {serve_id} with max {max_events}")
    while True:
        res = executor.run(make_serve_query(serve_id, token, max_events))
        parsed = parse(res.stdout)
        token = parsed.get("next_continuation_token", "")
        events = parsed.get("events")
        for e in events:
            print(e.get("data"))
        if not token:
            break


def main():
    test("serve-1", 1)
    test("serve-2", 5)


if __name__ == "__main__":
    main()
