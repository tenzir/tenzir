---
title: "Add colors to JSON printer"
type: change
author: mavam
created: 2023-07-15T05:07:37Z
pr: 3343
---

We removed the `--pretty` option from the `json` printer. This option is now the
default. To switch to NDJSON, use `-c|--compact-output`.
