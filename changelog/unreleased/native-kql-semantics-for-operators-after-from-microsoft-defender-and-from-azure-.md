---
title: Native KQL semantics for operators after from_microsoft_defender and from_azure_log_analytics
type: change
authors:
  - mavam
created: 2026-11-06T08:41:14.354284Z
---

`from_microsoft_defender` and `from_azure_log_analytics` now hand the service the operators that follow them, as long as KQL can spell them: `where`, `select`, `drop`, `summarize` with `count()`, `count(x)`, `min`, `max`, and `sum`, `sort`, and `head`. The operators append them to your query as `where`, `project`, `project-away`, `summarize`, `sort`, and `take` stages, where they leave the pipeline and run with KQL's semantics:

```tql
from_microsoft_defender "DeviceNetworkEvents",
  azure_auth={
    tenant_id: "...",
    client_id: "...",
    client_secret: secret("defender-client-secret"),
  }
where RemotePort == 443
summarize DeviceName, connections=count()
sort -connections
head 10
```

- **Filters:** strings compare case-sensitively, and a string is never `null` in KQL but may be empty. IP addresses compare as text, and subnets match with `ipv4_is_in_range` or `ipv6_is_in_range`. A comparison of mismatched types either holds for no row or fails the query.
- **Fields:** a field that the query does not return fails the query, as does a type that KQL rejects, such as a sum over strings or a `summarize` by a `dynamic` column.
- **Summaries:** `summarize` keeps a group for `null` keys, and `count(x)` counts the values that are not empty.
- **Sorts:** `sort` puts `null` first in ascending order and last in descending order.

Before, the operators only preselected rows in the service and filtered them again in Tenzir. A query that the service rejects now fails the pipeline. After a query whose result is not a single table, such as a query with several tabular statements, or one with `render`, `fork`, or `facet`, the operators stay in the pipeline.

`from_microsoft_defender` now warns when a result has 100,000 rows, the most that advanced hunting returns, since the service may have truncated it without telling.

Use `pushdown=false` to send your query unchanged and keep every operator in the pipeline with TQL semantics:

```tql
from_azure_log_analytics "SecurityEvent",
  workspace_id=secret("log-analytics-workspace"),
  azure_auth={
    tenant_id: "...",
    client_id: "...",
    client_secret: secret("log-analytics-client-secret"),
  },
  pushdown=false
where Account == "alice"
```
