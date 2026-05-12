# runner: python
"""Verify from_amqp passes queue declaration arguments to RabbitMQ."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from typing import Any


def _resolve_tenzir_binary() -> str:
    env_val = os.environ.get("TENZIR_BINARY")
    if env_val:
        return env_val
    binary = shutil.which("tenzir")
    if binary:
        return binary
    raise RuntimeError("tenzir executable not found (set TENZIR_BINARY or add to PATH)")


def _management_opener() -> urllib.request.OpenerDirector:
    password_manager = urllib.request.HTTPPasswordMgrWithDefaultRealm()
    password_manager.add_password(
        None,
        os.environ["AMQP_MANAGEMENT_URL"],
        os.environ["AMQP_USER"],
        os.environ["AMQP_PASSWORD"],
    )
    return urllib.request.build_opener(
        urllib.request.HTTPBasicAuthHandler(password_manager)
    )


def _request_json(
    opener: urllib.request.OpenerDirector,
    method: str,
    path: str,
    body: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = None
    headers = {}
    if body is not None:
        payload = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(
        f"{os.environ['AMQP_MANAGEMENT_URL']}{path}",
        data=payload,
        headers=headers,
        method=method,
    )
    with opener.open(request, timeout=5) as response:
        text = response.read().decode("utf-8")
        return json.loads(text) if text else {}


def _queue_path(queue: str) -> str:
    vhost = urllib.parse.quote(os.environ["AMQP_VHOST"], safe="")
    name = urllib.parse.quote(queue, safe="")
    return f"/api/queues/{vhost}/{name}"


def _exchange_publish_path(exchange: str) -> str:
    vhost = urllib.parse.quote(os.environ["AMQP_VHOST"], safe="")
    exchange_name = urllib.parse.quote(exchange, safe="")
    return f"/api/exchanges/{vhost}/{exchange_name}/publish"


def _wait_for_queue(
    opener: urllib.request.OpenerDirector,
    queue: str,
    proc: subprocess.Popen[str],
) -> dict[str, Any]:
    deadline = time.monotonic() + 30
    last_error = ""
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            stdout, stderr = proc.communicate()
            raise RuntimeError(
                "from_amqp exited before declaring the queue\n"
                f"stdout:\n{stdout}\n"
                f"stderr:\n{stderr}"
            )
        try:
            return _request_json(opener, "GET", _queue_path(queue))
        except urllib.error.HTTPError as exc:
            last_error = exc.read().decode("utf-8", errors="replace")
            if exc.code != 404:
                raise
        except urllib.error.URLError as exc:
            last_error = str(exc)
        time.sleep(0.2)
    raise RuntimeError(f"timed out waiting for queue {queue}: {last_error}")


def _terminate(proc: subprocess.Popen[str]) -> None:
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


def _run_case(
    label: str,
    declaration_options: str,
    expected_arguments: dict[str, Any],
    expected_type: str,
) -> None:
    queue = f"from-amqp-{label}-{uuid.uuid4().hex[:8]}"
    exchange = "amq.direct"
    payload = f"{label}-message"
    pipeline = f"""
from_amqp env("AMQP_URL"),
          queue="{queue}",
          exchange="{exchange}",
          routing_key="{queue}",
          {declaration_options}
head 1
select line = string(message)
""".strip()
    proc = subprocess.Popen(
        [
            _resolve_tenzir_binary(),
            "--bare-mode",
            "--console-verbosity=warning",
            "--multi",
            "--neo",
            pipeline,
        ],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    opener = _management_opener()
    try:
        info = _wait_for_queue(opener, queue, proc)
        actual_type = info.get("type")
        if actual_type != expected_type:
            raise RuntimeError(
                f"expected queue type {expected_type}, got {actual_type}"
            )
        arguments = info.get("arguments", {})
        for key, value in expected_arguments.items():
            actual = arguments.get(key)
            if actual != value:
                raise RuntimeError(
                    f"expected {key}={value!r}, got {actual!r}; "
                    f"all arguments: {arguments!r}"
                )
        publish = _request_json(
            opener,
            "POST",
            _exchange_publish_path(exchange),
            {
                "properties": {},
                "routing_key": queue,
                "payload": payload,
                "payload_encoding": "string",
            },
        )
        if not publish.get("routed"):
            raise RuntimeError(f"message was not routed to queue {queue}: {publish!r}")
        stdout, stderr = proc.communicate(timeout=20)
        if proc.returncode != 0:
            raise RuntimeError(
                f"from_amqp failed with exit code {proc.returncode}\n"
                f"stdout:\n{stdout}\n"
                f"stderr:\n{stderr}"
            )
        if payload not in stdout:
            raise RuntimeError(
                "from_amqp did not emit the published message\n"
                f"stdout:\n{stdout}\n"
                f"stderr:\n{stderr}"
            )
        print(f"{label}: {payload}")
    finally:
        _terminate(proc)


def main() -> None:
    _run_case(
        "quorum",
        """
durable=true,
          no_auto_delete=true,
          queue_arguments={
            "x-queue-type": "quorum",
            "x-quorum-initial-group-size": 1
          }
""".strip(),
        {
            "x-queue-type": "quorum",
            "x-quorum-initial-group-size": 1,
        },
        "quorum",
    )
    _run_case(
        "classic",
        """
durable=true,
          no_auto_delete=true,
          queue_arguments={
            "x-max-length": 5,
            "x-message-ttl": 60000,
            "x-single-active-consumer": true
          }
""".strip(),
        {
            "x-max-length": 5,
            "x-message-ttl": 60000,
            "x-single-active-consumer": True,
        },
        "classic",
    )


if __name__ == "__main__":
    main()
