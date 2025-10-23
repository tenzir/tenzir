---
title: "Flip pipeline subprocesses option semantics"
type: change
authors: mavam
pr: 5537
---

We renamed the configuration option to `tenzir.pipeline-subprocesses` and
kept the feature opt-in to avoid surprising users upgrading from earlier
releases. Set the option to `true` to enable subprocess execution:

```yaml
tenzir:
  pipeline-subprocesses: true
```
