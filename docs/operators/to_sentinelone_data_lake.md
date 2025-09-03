---
title: to_sentinelone_data_lake
category: Outputs/Events
example: 'to_sentinelone_data_lake "https://…", …'
---

Sends security events to SentinelOne Singularity Data Lake via REST API.

```tql
to_sentinelone_data_lake url:string, bearer_token=string
                        [session_info=record, timeout=duration]
```

## Description

The `to_sentinelone_data_lake` operator sends incoming events to
the [SentinelOne Data Lake REST API](https://support.sentinelone.com/hc/en-us/articles/360004195934-SentinelOne-API-Guide)
as structured data.

The operator accumulates multiple events before sending them as a single batch
request, respecting the API's limits.

If events are OCSF events, the timestamp and severity are automatically extracted
and added to the events meta information.

### `url: string`

The URL of the SentinelOne Data Lake's console, for example `https://xdr.us1.sentinelone.net`

### `bearer_token = string`

The _token_ to use for authorization.

### `session_info = record (optional)`

Some additional sessionInfo to send with each batch of events, as the
`sessionInfo` field in the request body.

### `timeout = duration (optional)`

The delay after which events are sent, even if if this results in fewer events
sent per message.

Defaults to `5min`.

## Automatic OCSF detection

The operator will automatically detect that an event is an OCSF event and set
the events Data Lake `ts` and `sev` fields based on the OCSF fields `time` and
`severity_id`.

`severity_id` is mapped to the SentinelOne Data Lakes `sev` property according to
this table:

| OCSF `severity_id` | OCSF `severity` | SentinelOne severity | SentinelOne name |
| :----------------: | :-------------: | :------------------: | :--------------: |
|         0          |     Unknown     |          3           |       info       |
|         1          |  Informational  |          1           |      finer       |
|         2          |       Low       |          3           |       fine       |
|         3          |     Medium      |          3           |       info       |
|         4          |      High       |          3           |       warn       |
|         5          |    Critical     |          3           |      error       |
|         6          |      Fatal      |          3           |      fatal       |
|         99         |      Other      |          3           |       info       |
