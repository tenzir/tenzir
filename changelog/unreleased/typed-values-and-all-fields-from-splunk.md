---
title: Typed values and all fields from from_splunk
type: breaking
authors:
  - mavam
created: 2026-10-30T11:50:26.000000Z
---

The `from_splunk` operator now infers numbers and booleans from the strings
that Splunk returns, in addition to IP addresses, subnets, timestamps, and
durations. Filters can therefore compare Splunk fields like any other values:

```tql
from_splunk "https://splunk.example.com:8089",
  search="index=main sourcetype=linux_secure",
  earliest="-15m",
  latest="now",
  headers={Authorization: secret("splunk-rest-authorization")}
where bytes > 1024 and success
```

Previously, `bytes` was the string `"2048"` and `success` the string
`"true"`. Pass `raw=true` to keep every value as a string.

Results now also carry all of their fields. Splunk's export endpoint returns
only the fields that a search references, so a plain search such as
`search index=main` used to yield `_raw`, `_time`, and a few default
fields only. The operator now appends `| fields *` to the search unless a
downstream `select` names the fields that the pipeline needs.
