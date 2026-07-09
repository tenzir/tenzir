---
title: Explicit continuation token for the first page in /serve-multi
type: change
authors:
  - lava
prs:
  - 6373
created: 2026-07-09T13:30:40.461464Z
---

The `/serve-multi` REST endpoint now requires a `continuation_token` on every
entry in `requests`. The first page of an output stream is addressed by the
well-known nil UUID instead of omitting the field:

```json
{
  "requests": [
    {
      "serve_id": "query-1",
      "continuation_token": "00000000-0000-0000-0000-000000000000"
    }
  ]
}
```

Because every page—including the first—now has an explicit token, re-sending
any request returns the same batch of events and the same next token again.
Previously, retrying the initial request after its response was lost failed
with an "unknown continuation token" error and made the first batch
unrecoverable.

The `/serve` endpoint is unchanged: omitting `continuation_token` still
requests the first page, and it also benefits from first-page retries. In
particular, when a pipeline's entire output fits into the first response,
retrying that request now returns the events again instead of an empty
`completed` response.
