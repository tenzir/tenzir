---
title: "Compact NDJSON output"
type: change
authors: dominiklohmann
pr: 5015
---

The output of `write_ndjson` is now more compact and no longer includes
unnecessary whitespace. Additionally, `write_json` no longer prints a trailing
whitespace after each comma.
