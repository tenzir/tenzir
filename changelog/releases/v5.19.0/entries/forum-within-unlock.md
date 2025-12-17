---
title: "Flip pipeline subprocesses option semantics"
type: change
author: mavam
created: 2025-10-24T12:07:43Z
pr: 5537
---

We renamed the configuration option to `tenzir.pipeline-subprocesses` and
kept the feature opt-in to avoid surprising users upgrading from earlier
releases. Set the option to `true` to enable subprocess execution:

```yaml
tenzir:
  pipeline-subprocesses: true
```
