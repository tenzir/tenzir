---
title: "Sinks in HTTP Parsing Pipelines"
type: feature
author: mavam
created: 2025-07-15T16:35:30Z
pr: 5343
---

Parsing pipeline in the `from_http` and `http` operators now support sinks. This
worked already in `from_file` parsing pipelines and now works, as expected, also
in the HTTP parsing pipelines. For example, you can now write:

```tql
from_http "https://cra.circl.lu/opendata/geo-open/mmdb-country-asn/latest.mmdb" {
  context::load "geo-open-country-asn"
}
```
