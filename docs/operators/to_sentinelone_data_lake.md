---
title: to_sentinelone_data_lake
category: Outputs/Events
example: 'to_sentinelone_data_lake "https://…", …'
---

Sends security events to SentinelOne's Data Lake via REST API.

```tql
to_sentinelone_data_lake domain=string, bearer_token=string [session_info=record]
```

## Description

The `to_sentinelone_data_lake` operator sends incoming events to
the [SentinelOne Data Lake REST API](https://support.sentinelone.com/hc/en-us/articles/360004195934-SentinelOne-API-Guide)
as structured data.

The operator accumulates multiple events before sending them as a single batch
request, respecting the API's limits.

If events are OCSF events, the timestamp and severity are automatically extracted
and added to the events meta information.
