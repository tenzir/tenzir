---
title: Fix assertion failure in replace operator when replacing with null
type: bugfix
authors:
  - mavam
  - claude
pr: 5696
created: 2026-01-29T08:51:10.900215Z
---

The `replace` operator no longer triggers an assertion failure when using
`with=null` on data processed by operators like `ocsf::cast`.

```tql
load_file "dns.json"
read_json
ocsf::cast "dns_activity"
replace what="", with=null
```
