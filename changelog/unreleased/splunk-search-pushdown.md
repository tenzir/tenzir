---
title: Filters, projections, and limits pushed into Splunk searches
type: feature
authors:
  - mavam
created: 2026-10-30T11:50:26.000000Z
---

The `from_splunk` operator now sends downstream filters, projections, and
limits to Splunk as trailing stages of the search, so that Splunk returns fewer
results:

```tql
from_splunk "https://splunk.example.com:8089",
  search="index=main sourcetype=linux_secure",
  earliest="-15m",
  latest="now",
  headers={Authorization: secret("splunk-rest-authorization")}
where status >= 500 and user.starts_with("adm")
select _time, user, status
```

This runs `search index=main sourcetype=linux_secure | search status>=500
user="adm*" | fields status, user, _time` in Splunk. Splunk's `search`
command matches strings case-insensitively, so Tenzir still applies the complete
filter to the results. Comparisons with numbers, strings, booleans, and `_time`,
`in` lists, `starts_with`, `ends_with`, substring tests, and `!= null`
reach Splunk. Other predicates stay in Tenzir.

A downstream `head` without a filter becomes a `head` stage, and the operator
stops reading results once the limit is reached. Searches with commands that
have side effects, such as `outputlookup` or `collect`, or with macros, never
get a `head` stage.
