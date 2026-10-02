# runner: python
# timeout: 60

"""A chart spec applies to every page and does not count as an event."""

import json
import os
import shlex
import socket
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path


def post(url, path, body=None):
    request = urllib.request.Request(
        f"{url}/api/v0{path}",
        data=json.dumps(body or {}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def main():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    binary = shlex.split(os.environ["TENZIR_NODE_CLIENT_BINARY"])[0]
    endpoint = os.environ["TENZIR_NODE_CLIENT_ENDPOINT"]
    server = subprocess.Popen(
        [
            str(Path(binary).with_name("tenzir-ctl")),
            "--bare-mode",
            "--console-verbosity=warning",
            f"--endpoint={endpoint}",
            "web",
            "server",
            "--mode=dev",
            "--bind=127.0.0.1",
            f"--port={port}",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    url = f"http://127.0.0.1:{port}"
    try:
        for _ in range(150):
            try:
                post(url, "/ping")
                break
            except (urllib.error.URLError, OSError):
                time.sleep(0.2)
        else:
            raise RuntimeError("REST API did not start")
        request = {
            "serve_id": "serve_chart_pages",
            "min_events": 1,
            "max_events": 1,
            "timeout": "5s",
            "schema": "exact",
        }
        first = post(url, "/serve", request)
        retry = post(url, "/serve", request)
        assert retry == first, (first, retry)
        second = post(
            url,
            "/serve",
            {**request, "continuation_token": first["next_continuation_token"]},
        )
        values = [page["events"][0]["data"]["y"] for page in (first, second)]
        assert values == [3.5, 4.01], (first, second)
        assert all(len(page["events"]) == 1 for page in (first, second))
        assert first["schemas"] == second["schemas"]
        grouped_request = {**request, "serve_id": "serve_chart_grouped_pages"}
        grouped_first = post(url, "/serve", grouped_request)
        grouped_second = post(
            url,
            "/serve",
            {
                **grouped_request,
                "continuation_token": grouped_first["next_continuation_token"],
            },
        )
        assert grouped_first["schemas"] == grouped_second["schemas"]
        assert (
            grouped_first["events"][0]["schema_id"]
            == grouped_second["events"][0]["schema_id"]
        )
        null_x_request = {**request, "serve_id": "serve_chart_null_x"}
        null_x_first = post(url, "/serve", null_x_request)
        null_x_second = post(
            url,
            "/serve",
            {
                **null_x_request,
                "continuation_token": null_x_first["next_continuation_token"],
            },
        )
        assert [
            page["events"][0]["data"]["x"] for page in (null_x_first, null_x_second)
        ] == [None, 1.0]
        assert null_x_first["schemas"] == null_x_second["schemas"]
        null_string_request = {**request, "serve_id": "serve_chart_null_string"}
        null_string_first = post(url, "/serve", null_string_request)
        null_string_second = post(
            url,
            "/serve",
            {
                **null_string_request,
                "continuation_token": null_string_first["next_continuation_token"],
            },
        )
        assert [
            page["events"][0]["data"]["x"]
            for page in (null_string_first, null_string_second)
        ] == [None, "a"]
        assert null_string_first["schemas"] == null_string_second["schemas"]
        print("chart metadata persists across pages")
    finally:
        server.terminate()
        try:
            server.wait(timeout=10)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait()


if __name__ == "__main__":
    main()
