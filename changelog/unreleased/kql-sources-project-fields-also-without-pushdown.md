---
title: KQL sources project fields also without pushdown
type: change
authors:
  - mavam
created: 2026-11-15T20:33:09.141213Z
---

With `pushdown=false`, `from_microsoft_defender` and `from_azure_log_analytics` now keep only the fields that the operators after them read, as other sources do. For example:

```tql
from_microsoft_defender "DeviceEvents", azure_auth={…}, pushdown=false
where DeviceName == "host-a"
select DeviceName, Id
```

The query now ends in `| project ['Id'] = column_ifexists('Id', dynamic(null)), ['DeviceName'] = column_ifexists('DeviceName', dynamic(null))`, while the `where` and the `select` still run in Tenzir. Before, the query went out unchanged.
