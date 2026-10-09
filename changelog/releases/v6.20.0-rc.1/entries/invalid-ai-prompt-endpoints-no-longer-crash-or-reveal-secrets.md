---
title: Invalid ai_prompt endpoints no longer crash or reveal secrets
type: bugfix
authors:
  - mavam
created: 2026-10-09T08:29:38.545304Z
---

The `ai_prompt` operator no longer crashes the pipeline when the `endpoint` is not a valid URL, for example because its port is out of range. It now reports a regular error and doesn't include the resolved endpoint in the message, so an endpoint passed as a secret stays hidden.
