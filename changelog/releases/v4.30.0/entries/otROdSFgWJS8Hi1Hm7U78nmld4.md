---
title: "Fix crash in `from \"path/to/file.ndjson\"`"
type: bugfix
author: dominiklohmann
created: 2025-03-13T10:39:21Z
pr: 5050
---

The `from` operator no longer incorrectly attempts to use parsers with a known
file extension that is a suffix of the actual file extension. For example, `from
"file.foojson"` will no longer attempt to use the `json` parser by default,
while `from "file.foo.json"` and `from "file.json"` continue to work as
expected. This fixes an error for `.ndjson` files, which could previously not
decide between the `json` and `ndjson` parsers.
