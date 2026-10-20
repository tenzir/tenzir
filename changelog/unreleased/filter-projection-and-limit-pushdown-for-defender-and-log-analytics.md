---
title: Filter, projection, and limit pushdown for Defender and Log Analytics
type: change
authors:
  - mavam
created: 2026-10-15T16:30:00.000000Z
---

The `from_microsoft_defender` and `from_azure_log_analytics` operators now push conservative prefilters and projection and limit hints from the `where`, `select`, and `head` operators that follow them into your KQL query, so that the service can return fewer rows and columns:

```tql
from_azure_log_analytics "SecurityEvent | where EventID == 4625",
  workspace_id=secret("log-analytics-workspace"),
  azure_auth={
    tenant_id: "...",
    client_id: "...",
    client_secret: secret("azure-client-secret"),
  }
where Account == "CONTOSO\\alice" and Computer.starts_with("DC")
select TimeGenerated, Account, Computer
```

Your KQL query stays as it is. The operators append trailing `where`, `project`, and `take` stages, so that they apply to the rows that your query returns. Since the types of a query's output are unknown before it runs, the appended `where` only preselects candidates: it keeps every row whose KQL type could decode to a matching TQL value, and every TQL filter still runs locally with exact TQL semantics, including case sensitivity and `null` handling. Since Defender may silently return only the first 100,000 rows of a query, a `take 100000` stage precedes the appended `where` there, so that a filter never reveals rows that Defender would have cut off. Log Analytics reports a result beyond its row and size limits as a partial failure, which the operator rejects, so its filters run on the complete result: they yield the complete filtered result or fail as well.

Prefilters cover comparisons of a field with a number, equality of a field with a string or boolean, `in` with a list of literals of a single kind, `!= null`, and case-sensitive `starts_with`, `ends_with`, and substring matching. They also cover case-insensitive string hunts with ASCII search strings, equality and list membership after `to_lower()` or `to_upper()`, and IP equality and subnet hunts. For example, these filters can reduce the events returned by a Defender or Log Analytics source:

```tql
where ProcessCommandLine.search("powershell", ignore_case=true)
  and RemoteIP.ip() in 10.20.0.0/16
select DeviceName, ProcessCommandLine, RemoteIP
```

Use `.ip()` to parse an address stored in a string. The prefilters retain Unicode, structured values, and address spellings that need local evaluation, so they can return extra candidates without changing the final matches. Case-insensitive searches with non-ASCII search strings and native IPv6 subnet ranges starting at or below `::ffff:ffff:ffff` stay local. Regular expressions, times, negation, nested field references, and lists that mix kinds also stay local.

A `head` reaches the service only if no filter precedes it and its limit fits into a signed 64-bit integer. A projected column that your query does not return becomes `null`, as with a local `select`. The `start` and `end` window stays unchanged, and queries that use `render`, `fork`, `facet`, or a `set` statement are sent unchanged.
