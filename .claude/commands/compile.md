---
description: Compile the whole project or a specific target (e.g. `/compile tenzir`)
argument-hint: "[target] [cmake-options...]"
context: fork
model: haiku
allowed-tools: Bash(./scripts/build.sh:*)
---

Run `./scripts/build.sh $ARGUMENTS`.

- On success: answer briefly, report only directly relevant warnings, stop
- On failure: report the first error, stop
- Never edit code, run the binary, or run tests
