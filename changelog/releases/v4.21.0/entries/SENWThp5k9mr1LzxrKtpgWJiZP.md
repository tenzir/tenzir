---
title: "Make the kv-parser consider quotes when looking for separators"
type: feature
author: IyeOnline
created: 2024-09-22T20:02:08Z
pr: 4591
---

The `kv` parser now allows for keys and values to be enclosed in double quotes:
Split matches within quotes will not be considered. Quotes will be trimmed of
keys and values. For example `"key"="nested = value, fun"` will now successfully
parse as `{ "key" : "nested = value, fun" }`.
