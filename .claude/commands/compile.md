---
description: Compile a target
argument-hint: "[target] [cmake-options...]"
context: fork
model: haiku
---

Execute:

```sh
scripts/build.sh $ARGUMENTS
```

- On success: verify binary exists, report briefly, stop
- On failure: report the first error, stop
- Never edit code, run the binary, or run tests
