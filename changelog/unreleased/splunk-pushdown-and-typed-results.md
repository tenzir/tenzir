---
title: Faster Splunk searches with typed results
type: change
authors:
  - mavam
created: 2026-10-30T11:50:26.000000Z
---

The `from_splunk` operator now hands as much of the pipeline as it can to
Splunk, and returns typed values:

```tql
from_splunk "https://splunk.example.com:8089",
  index="main",
  earliest="-1h",
  latest="now",
  headers={Authorization: secret("splunk-rest-authorization")}
where sourcetype == "linux_secure" and user != "root" and bytes > 1024
summarize src_ip, attempts=count()
```

- **Typed values:** the operator now infers numbers and booleans
  from the strings that Splunk returns, so `bytes > 1024` and `where success`
  work as expected. Previously these values stayed strings. Pass `raw=true` to
  keep every value as a string.
- **All fields:** results carry every extracted field, not only the fields
  that the search names. The operator appends `| fields *` unless a downstream
  `select` names the fields that the pipeline needs.
- **Reading indexes:** the new `index` argument reads one or more indexes
  without writing SPL, so that you can filter in TQL. The `search` argument
  also accepts searches as typed in Splunk Web, such as `index=main`.
- **Filters, projections, and limits:** downstream `where`, `select`, and
  `head` go into the search, including negations and IP address filters.
  Splunk compares some values differently, so these filters only preselect
  the events, and Tenzir still applies the complete filter.
- **Summaries:** a `summarize` that directly follows the operator, also through
  `top` and `rare`, runs in Splunk with `stats` for group keys, `count()`,
  `count(field)`, and `min` and `max` of integers. Whenever Splunk's result
  could differ from what `summarize` computes, Tenzir summarizes the events
  itself instead.

Searches with `fieldformat`, macros, or commands with side effects such as
`outputlookup` keep the respective work in Tenzir. The reference page on SPL
optimizations lists the exact rules.
