---
title: HEC metadata and raw endpoint support in `to_splunk`
type: feature
author: mavam
pr: 6074
created: 2026-04-30T13:02:25.554018Z
---

The `to_splunk` operator gains three new options for richer HEC metadata.

Use `time=` to set the per-event Splunk timestamp from an expression that
evaluates to a Tenzir `time` or a non-negative epoch in seconds:

```tql
from {message: "login succeeded", observed_at: 2026-04-24T08:30:00Z}
to_splunk "https://localhost:8088",
  hec_token=secret("splunk-hec-token"),
  time=observed_at
```

Use `fields=` to attach indexed HEC fields. The expression must evaluate to
a flat record whose values are strings or lists of strings:

```tql
to_splunk "https://localhost:8088",
  hec_token=secret("splunk-hec-token"),
  event={message: message},
  fields={user: user, tags: tags}
```

Use `raw=` to send already-formatted text to the HEC raw endpoint
(`/services/collector/raw`). The `raw` expression must evaluate to a
`string`. Multiple events in one request are separated by newlines, and
request-level metadata such as `host`, `source`, `sourcetype`, `index`, and
`time` is sent as query parameters:

```tql
to_splunk "https://localhost:8088",
  hec_token=secret("splunk-hec-token"),
  raw=line,
  source=source,
  sourcetype="linux_secure"
```

`raw` and `event` are mutually exclusive; `fields` is not supported with
`raw`.
