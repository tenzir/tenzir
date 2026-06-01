---
title: Parse TQL records with `read_tql`
type: feature
author: mavam
pr: 5707
created: 2026-04-30T13:00:14.039139Z
---

The new `read_tql` operator parses an incoming byte stream of TQL-formatted
records into events. Each top-level record expression becomes one event:

```tql
load_file "events.tql"
read_tql
```

The input format matches the output of `write_tql`, so `read_tql` is useful
for round-tripping data through TQL notation, reading TQL-formatted files,
or processing data piped from other Tenzir pipelines.
