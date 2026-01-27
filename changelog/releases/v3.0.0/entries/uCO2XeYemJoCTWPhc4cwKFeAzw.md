---
title: "Align output of the Zeek TSV reader with schemas"
type: change
author: dominiklohmann
created: 2023-01-28T18:52:37Z
pr: 2887
---

The bundled Zeek schema no longer includes the `_path` field included in Zeek
JSON. Use `#type == "zeek.foo"` over `_path == "foo"` for querying data ingested
using `vast import zeek-json`.
