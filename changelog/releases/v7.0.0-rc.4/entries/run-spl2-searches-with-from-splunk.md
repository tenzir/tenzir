---
title: Run SPL2 searches with from_splunk
type: feature
authors:
  - mavam
created: 2026-11-17T08:03:37.705544Z
---

`from_splunk` now runs SPL2 searches with the new `spl2` argument, as an alternative to `search` for SPL and `index`. SPL2 searches need Splunk Enterprise 10.2 or Splunk Cloud Platform 10.2.2510 or later.

As for SPL, `from_splunk` hands Splunk the operators that follow it: `where`, `select`, `drop`, `summarize`, `sort`, and `head` run in Splunk with Splunk's semantics. For example, this pipeline:

```tql
from_splunk "https://splunk.example.com:8089",
  spl2=r#"from main where sourcetype == "linux_secure""#,
  earliest="-1h",
  latest="now",
  headers={Authorization: secret("splunk-rest-authorization")}
where user != "root"
summarize user, attempts=count()
sort -attempts
head 20
```

dispatches the SPL2 search below as the statement `$main` of a module, and exports its results once the search job is done:

```
from main where sourcetype == "linux_secure"
| where ('user' != "root")
| stats count() as tenzir_column_0 by 'user'
| sort 20 -'tenzir_column_0'
| head 20
| appendpipe [stats count() as tenzir_groups_0]
```

Filters that compare `_time` with a time, and the operators after them, stay in the pipeline, since an SPL2 search may redefine `_time`. The `options` argument does not apply to `spl2`. Use `pushdown=false` to keep every operator in the pipeline with TQL semantics.
