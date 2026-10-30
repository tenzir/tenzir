---
title: Reading Splunk indexes without SPL
type: feature
authors:
  - mavam
created: 2026-10-30T12:14:47.000000Z
---

The `from_splunk` operator has a new `index` argument that reads the events
of one or more indexes without writing SPL. Filter the events in TQL, and Tenzir
adds the filters to the search that it sends to Splunk:

```tql
from_splunk "https://splunk.example.com:8089",
  index="main",
  earliest="-1h",
  latest="now",
  headers={Authorization: secret("splunk-rest-authorization")}
where sourcetype == "linux_secure" and user == "root"
select _time, user, src_ip
```

Splunk receives `search index="main" sourcetype="linux_secure" user="root" |
fields src_ip, user, _time, sourcetype`. Pass a list, such as `index=["main", "web_*"]`, to
read several indexes.

The `search` argument now also accepts a search exactly as you would type it in
Splunk Web, such as `index=main sourcetype=linux_secure`. Tenzir prepends the
`search` command unless the search starts with it or with `|`.
