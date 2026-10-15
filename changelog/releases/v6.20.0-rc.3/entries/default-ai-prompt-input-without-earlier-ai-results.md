---
title: Default ai_prompt input without earlier AI results
type: change
authors:
  - mavam
created: 2026-10-09T08:33:27.222274Z
---

By default, `ai_prompt` sent the entire event to the model, including results that earlier AI calls had written to the event. In the following pipeline, the second call sent the summary from the first call along with its token usage and latency, although the classification only needs the alert:

```tql
from {alert: "PowerShell downloaded a script from a newly registered domain."}
ai_prompt model="qwen3.8", system="Summarize this alert.", into=ai.summary
ai_prompt model="qwen3.8", system="Classify this alert.", into=ai.label
```

The default input now leaves out the `ai` field and the `into` field, so the second call sends only `{"alert": "…"}`. To send earlier results anyway, pass them explicitly, for example with `data={alert: alert, summary: ai.summary.text}`, or send the whole event with `data=this`.
