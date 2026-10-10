"""System One API fixture for ai_decide integration tests.

The fixture answers `POST /v1/systemone` with deterministic decisions. It also
answers the Cloudflare Workers AI run path
`POST /client/v4/accounts/test/ai/run/@cf/cloudflare/clef` and wraps that
response in a `result` envelope like Workers AI does. `POST /v1/decisions`
speaks the OpenAI Decisions format: an `input` instead of a `state`, a list of
named questions, and a list of named answers. The fixture checks that format
and answers `mode: refusal` with a refusal of the first question. A state
that mentions "danger" leans toward yes, the last choice option, and the
highest score level; any other state leans the opposite way. Object states can
select a response variant with a `mode` field, delay the response with
`delay_ms`, and hold it with `rendezvous: N` until it has overlapped with N - 1
other requests.
Answers are test data, not model predictions.
"""

from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlsplit

from tenzir_test import FixtureHandle, fixture


WORKERS_AI_PATH = "/client/v4/accounts/test/ai/run/@cf/cloudflare/clef"
OPENAI_PATH = "/v1/decisions"


@dataclass(frozen=True)
class SystemOneAssertions:
    count: int | None = None
    model: str | None = None
    authorization: str | None = None
    questions: dict[str, Any] | None = None
    states: list[Any] | None = None
    max_concurrent_requests: int | None = None


def _distribution(size: int, risky: bool) -> list[float]:
    chosen = size - 1 if risky else 0
    rest = 0.2 / (size - 1)
    return [0.8 if index == chosen else rest for index in range(size)]


def _answer(question: dict[str, Any], risky: bool, mode: str | None) -> dict[str, Any]:
    kind = question["type"]
    if kind == "noul":
        probability = 0.9 if risky else 0.1
        if mode == "databricks":
            return {"type": kind, "probability": probability}
        answer: dict[str, Any] = {"type": kind, "noul": probability}
        if mode == "laya":
            answer["confidence"] = probability if risky else 1 - probability
        return answer
    if kind == "choice":
        options = list(question["criteria"])
        distribution = _distribution(len(options), risky)
        answer = {
            "type": kind,
            "choice": options[-1] if risky else options[0],
            "probabilities": dict(zip(options, distribution)),
            "confidence": 0.5,
        }
    elif kind == "score":
        levels = question["criteria"]
        distribution = _distribution(len(levels), risky)
        answer = {
            "type": kind,
            "score": round(sum(i * p for i, p in enumerate(distribution)), 6),
            "legend": {str(i): level for i, level in enumerate(levels)},
            "probabilities": {str(i): p for i, p in enumerate(distribution)},
            "confidence": 0.5,
        }
    else:
        raise ValueError(f"unknown question type: {kind}")
    if mode == "bare":
        del answer["confidence"]
    if mode == "laya":
        answer["answer_confidence"] = 0.8
        answer["action"] = {"act_probability": 1.0}
    return answer


def _openai_question_errors(question: Any) -> list[str]:
    """Returns what is wrong with a question of the OpenAI Decisions format."""
    if not isinstance(question, dict):
        return [f"question is not an object: {question!r}"]
    errors = []
    if not isinstance(question.get("name"), str) or not question["name"]:
        errors.append(f"question has no name: {question!r}")
    if not isinstance(question.get("instructions"), str):
        errors.append(f"instructions are not a string: {question!r}")
    kind = question.get("type")
    if kind == "choice":
        choices = question.get("choices")
        if not isinstance(choices, list) or not all(
            isinstance(c, dict) and isinstance(c.get("value"), str) for c in choices
        ):
            errors.append(f"choices are not a list of values: {question!r}")
    elif kind == "score":
        levels = question.get("levels")
        if not isinstance(levels, list) or not all(
            isinstance(level, dict) and isinstance(level.get("label"), str)
            for level in levels
        ):
            errors.append(f"levels are not a list of labels: {question!r}")
    elif kind != "predicate":
        errors.append(f"unknown question type: {kind!r}")
    return errors


def _openai_answer(question: dict[str, Any], risky: bool) -> dict[str, Any]:
    kind = question["type"]
    name = question["name"]
    if kind == "predicate":
        return {"type": kind, "name": name, "probability": 0.9 if risky else 0.1}
    if kind == "choice":
        values = [choice["value"] for choice in question["choices"]]
        distribution = _distribution(len(values), risky)
        return {
            "type": kind,
            "name": name,
            "choice": values[-1] if risky else values[0],
            "probabilities": [
                {"value": value, "probability": p}
                for value, p in zip(values, distribution)
            ],
            "confidence": 0.5,
        }
    levels = question["levels"]
    distribution = _distribution(len(levels), risky)
    return {
        "type": kind,
        "name": name,
        "score": round(sum(i * p for i, p in enumerate(distribution)), 6),
        "probabilities": [
            {"value": i, "label": level["label"], "probability": p}
            for i, (level, p) in enumerate(zip(levels, distribution))
        ],
        "confidence": 0.5,
    }


def _make_handler(
    errors: list[str], requests: list[dict[str, Any]], state: dict[str, int]
):
    lock = threading.Condition()

    class SystemOneHandler(BaseHTTPRequestHandler):
        def log_message(self, _format: str, *_args: object) -> None:
            return

        def _reply(self, status: HTTPStatus, payload: dict[str, Any]) -> None:
            body = json.dumps(payload, separators=(",", ":")).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self) -> None:  # noqa: N802
            with lock:
                state["active"] += 1
                state["arrivals"] += 1
                state["max_active"] = max(state["max_active"], state["active"])
                arrival = state["arrivals"]
                overlapping = state["active"] - 1
                lock.notify_all()
            try:
                self._handle(arrival, overlapping)
            finally:
                with lock:
                    state["active"] -= 1

        def _handle(self, arrival: int, overlapping: int) -> None:
            path = urlsplit(self.path).path
            length = int(self.headers.get("Content-Length", "0") or 0)
            raw = self.rfile.read(length) if length > 0 else b""
            workers_ai = path == WORKERS_AI_PATH
            openai = path == OPENAI_PATH
            if path != "/v1/systemone" and not workers_ai and not openai:
                errors.append(f"expected path /v1/systemone, got {path}")
                self._reply(HTTPStatus.NOT_FOUND, {"detail": "not found"})
                return
            content_type = self.headers.get("Content-Type", "")
            if "application/json" not in content_type:
                errors.append(f"expected application/json, got {content_type}")
            try:
                body = json.loads(raw.decode("utf-8"))
            except json.JSONDecodeError as error:
                errors.append(f"invalid request JSON: {error}")
                self._reply(HTTPStatus.BAD_REQUEST, {"detail": "invalid JSON"})
                return
            with lock:
                requests.append(
                    {"body": body, "authorization": self.headers.get("Authorization")}
                )
            if openai:
                if "state" in body or not isinstance(body.get("input"), str):
                    errors.append(f"expected a string input, got {body!r}")
                questions_list = body.get("questions")
                if not isinstance(questions_list, list) or not questions_list:
                    errors.append(f"expected a list of questions, got {body!r}")
                    questions_list = []
                for question in questions_list:
                    errors.extend(_openai_question_errors(question))
                subject = body.get("input")
                try:
                    subject = json.loads(subject) if subject[:1] in "{[" else subject
                except (TypeError, json.JSONDecodeError):
                    pass
            else:
                subject = body.get("state")
            mode = subject.get("mode") if isinstance(subject, dict) else None
            if isinstance(subject, dict) and subject.get("rendezvous"):
                # Every request that arrives while this one waits overlaps with it,
                # even if it finishes before this one wakes up.
                others = int(subject["rendezvous"]) - 1
                with lock:
                    arrived = lock.wait_for(
                        lambda: overlapping + state["arrivals"] - arrival >= others,
                        timeout=10,
                    )
                if not arrived:
                    errors.append("timed out waiting for concurrent requests")
            if isinstance(subject, dict) and subject.get("delay_ms"):
                time.sleep(int(subject["delay_ms"]) / 1000.0)
            if mode == "fail":
                self._reply(
                    HTTPStatus.INTERNAL_SERVER_ERROR, {"detail": "inference failed"}
                )
                return
            risky = "danger" in json.dumps(subject)
            if openai:
                # OpenAI refuses single questions and answers the others, so
                # `mode: refusal` refuses only the first question.
                answers_list: list[dict[str, Any]] = [
                    {"type": "refusal", "name": q["name"]}
                    if mode == "refusal" and i == 0
                    else _openai_answer(q, risky)
                    for i, q in enumerate(questions_list)
                ]
                self._reply(
                    HTTPStatus.OK,
                    {
                        "model": body.get("model"),
                        "answers": answers_list,
                        "usage": {
                            "input_tokens": 10 * len(questions_list),
                            "output_tokens": 0,
                        },
                    },
                )
                return
            questions = body["questions"]
            answers = {
                key: _answer(question, risky, mode)
                for key, question in questions.items()
            }
            if mode == "missing_answer":
                answers.popitem()
            if mode == "out_of_range":
                for answer in answers.values():
                    if answer["type"] == "noul":
                        answer["noul"] = -9
                    else:
                        answer["confidence"] = 7
            if mode == "wrong_type":
                for answer in answers.values():
                    answer["type"] = "choice" if answer["type"] != "choice" else "score"
            response: dict[str, Any] = {
                "model": body.get("model"),
                "answers": answers,
                "usage": {"input_tokens": 10 * len(questions), "output_tokens": 0},
            }
            if mode == "bare":
                del response["model"]
                del response["usage"]
            if mode == "laya":
                response["model"] = "laya-rl-agent"
                response["usage"].update(
                    {
                        "state_tokens": 512,
                        "state_tokens_dropped": 64,
                        "truncated": True,
                        "truncated_questions": list(questions)[:1],
                    }
                )
                response["routing"] = {"model": "english", "reason": "fixture"}
            if workers_ai:
                response = {
                    "result": response,
                    "success": True,
                    "errors": [],
                    "messages": [],
                }
            self._reply(HTTPStatus.OK, response)

    return SystemOneHandler


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


@fixture(name="system_one", assertions=SystemOneAssertions)
def run() -> FixtureHandle:
    errors: list[str] = []
    requests: list[dict[str, Any]] = []
    state = {"active": 0, "arrivals": 0, "max_active": 0}
    server = ThreadingHTTPServer(
        ("127.0.0.1", 0), _make_handler(errors, requests, state)
    )
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    port = server.server_address[1]

    def _assert_test(
        *, assertions: SystemOneAssertions | dict[str, Any], **_: object
    ) -> None:
        if isinstance(assertions, dict):
            assertions = SystemOneAssertions(**assertions)
        if assertions.count is not None and len(requests) != assertions.count:
            raise AssertionError(
                f"expected {assertions.count} requests, got {len(requests)}"
            )
        for request in requests:
            body = request["body"]
            if request["authorization"] != assertions.authorization:
                raise AssertionError(
                    f"expected authorization {assertions.authorization!r}, "
                    f"got {request['authorization']!r}"
                )
            if assertions.model is not None and body.get("model") != assertions.model:
                raise AssertionError(
                    f"expected model {assertions.model!r}, got {body.get('model')!r}"
                )
            if assertions.questions is not None and _canonical(
                body.get("questions")
            ) != _canonical(assertions.questions):
                raise AssertionError(
                    f"expected questions {assertions.questions!r}, "
                    f"got {body.get('questions')!r}"
                )
        if assertions.states is not None:
            actual = sorted(
                _canonical(request["body"].get("state", request["body"].get("input")))
                for request in requests
            )
            expected = sorted(_canonical(value) for value in assertions.states)
            if actual != expected:
                raise AssertionError(f"expected states {expected!r}, got {actual!r}")
        if (
            assertions.max_concurrent_requests is not None
            and state["max_active"] != assertions.max_concurrent_requests
        ):
            raise AssertionError(
                f"expected at most {assertions.max_concurrent_requests} concurrent "
                f"requests, observed {state['max_active']}"
            )

    def _teardown() -> None:
        server.shutdown()
        worker.join()
        server.server_close()
        if errors:
            raise RuntimeError(errors[0])

    return FixtureHandle(
        env={
            "SYSTEM_ONE_FIXTURE_ENDPOINT": f"http://127.0.0.1:{port}/v1",
            "SYSTEM_ONE_FIXTURE_WORKERS_AI_URL": (
                f"http://127.0.0.1:{port}{WORKERS_AI_PATH}"
            ),
            "SYSTEM_ONE_FIXTURE_OPENAI_URL": f"http://127.0.0.1:{port}{OPENAI_PATH}",
        },
        teardown=_teardown,
        hooks={"assert_test": _assert_test},
    )
