---
title: Platform control endpoints in tokens
type: feature
authors:
  - tobim
  - codex
created: 2026-06-25T13:38:07.719373Z
---

Tenzir nodes can now connect to the Tenzir Platform with a `tenzir.token` value
that carries the platform control endpoint:

```yaml
tenzir:
  token: tnz_XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX_<encoded-endpoint>
```

The same format works through the `TENZIR_TOKEN` environment variable. Set
`tenzir.platform-control-endpoint` or `TENZIR_PLATFORM_CONTROL_ENDPOINT` only
when you want to override the endpoint embedded in the token.
