---
title: Best-effort platform pipeline telemetry
type: feature
authors:
  - tobim
created: 2026-10-06T07:52:14.570609Z
---

Pipeline runs can now report external connector counters, lifecycle outcomes, and
compile-time and runtime diagnostics to the platform. Reporting also works for
standalone execution with deployment credentials:

```sh
tenzir --platform https://platform.example.com \
  --deployment-id example-deployment --key-file deployment.key \
  --pipeline-id example-pipeline --run 7 \
  'from_stdin | read_lines | discard'
```

Telemetry delivery is best effort and does not enable operator profiling.
