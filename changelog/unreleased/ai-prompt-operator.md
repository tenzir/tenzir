---
title: Add the `ai::prompt` operator
type: feature
authors:
  - mavam
  - codex
created: 2026-05-28T07:18:56.000000Z
---

The new `ai::prompt` operator sends each input event to an OpenAI-compatible
Responses API endpoint and writes an opinionated result record back into the
event:

```tql
from {message: "summarize this"}
ai::prompt model="gpt-4.1-mini"
```

By default the operator serializes `this` as compact JSON, writes the result to
`ai.prompt`, and uses the local Ollama endpoint at
`http://localhost:11434/v1`. Override `endpoint` to use another
OpenAI-compatible service.
