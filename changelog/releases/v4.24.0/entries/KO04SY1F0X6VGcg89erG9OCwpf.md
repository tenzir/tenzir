---
title: "Ignore whole line when NDJSON parser fails"
type: bugfix
author: jachris
created: 2024-11-21T11:19:56Z
pr: 4801
---

The `read_ndjson` operator no longer uses an error-prone mechanism to continue
parsing an NDJSON line that contains an error. Instead, the entire line is
skipped.
