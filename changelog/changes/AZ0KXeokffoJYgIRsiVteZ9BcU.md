---
title: "PRs 1196-1233"
type: feature
authors: dominiklohmann
pr: 1196
---

The new `dump` command prints configuration and schema-related information. The
implementation allows for printing all registered concepts and models, via `vast
dump concepts` and `vast dump models`. The flag to `--yaml` to `dump` switches
from JSON to YAML output, such that it confirms to the taxonomy configuration
syntax.
