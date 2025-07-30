# test : python_node

from fixture_support import *
import json


def parse(v: bytes):
    return json.loads(v.decode())


def make_serve_multi_query(continuation_tokens: dict[str, str], max_events: int) -> str:
    res = """api "/serve-multi", { requests: ["""
    for id, token in continuation_tokens.items():
        res += f"""{{ serve_id:"{id}", continuation_token:"{token}" }},"""
    res += f"""], timeout: "5s", max_events: {max_events}, schema: "never" }} | write_ndjson"""
    return res


def test():
    executor = Executor()
    continuation_tokens = {"serve-1": "", "serve-2": ""}
    for i in range(0, 4):
        print(f"query {i}")
        query = make_serve_multi_query(continuation_tokens, 3)
        res = executor.run(query)
        if res.stderr:
            print(res.stderr.decode())
            break
        parsed = parse(res.stdout)
        for id in list(continuation_tokens.keys()):
            print(f"id: {id}")
            data = parsed[id]
            next_token = data.get("next_continuation_token", "")
            if not next_token:
                continuation_tokens.pop(id)
            else:
                continuation_tokens[id] = next_token
            events = data.get("events")
            for e in events:
                print(e.get("data"))
        if not continuation_tokens:
            break
        print()


def main():
    test()


if __name__ == "__main__":
    main()
