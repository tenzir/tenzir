---
title: to_sentinelone_data_lake
category: Outputs/Events
example: 'to_sentinelone_data_lake "https://…", …'
---

Sends security events to SentinelOne Singularity Data Lake via REST API.

```tql
to_sentinelone_data_lake url:string, token=string,
                        [session_info=record, timeout=duration]
```

## Description

The `to_sentinelone_data_lake` operator sends incoming events to
the [SentinelOne Data Lake REST API](https://support.sentinelone.com/hc/en-us/articles/360004195934-SentinelOne-API-Guide)
as structured data, using the `addEvents` endpoint.

The operator accumulates multiple events before sending them as a single request,
respecting the API's limits.

If events are OCSF events, the `time` and `severity_id` fields are automatically
extracted and added to the events meta information.

The OCSF `severity_id` is mapped to the SentinelOne Data Lake `sev` property
according to this table:

| OCSF `severity_id` | SentinelOne severity |
| :----------------: | :------------------: |
|    0 (Unknown)     |       3 (info)       |
| 1 (Informational)  |      1 (finer)       |
|      2 (Low)       |       2 (fine)       |
|     3 (Medium)     |       3 (info)       |
|      4 (High)      |       4 (warn)       |
|    5 (Critical)    |      5 (error)       |
|     6 (Fatal)      |      6 (fatal)       |
|     99 (Other)     |       3 (info)       |

### `url: string`

The ingest URL for the Data Lake.

:::info
Please note that using the wrong ingestion endpoint, such as an incorrect region,
may silently fail, as the SentinelOne API responds with 200 OK, even for some
erroneous requests.
:::

### `token = string`

The token to use for authorization.

### `session_info = record (optional)`

Some additional sessionInfo to send with each batch of events, as the
`sessionInfo` field in the request body. If this option is used, it is recommended
that it contains a field `serverHost` to identify the Node.

This can also contain a field `parser`, which names a SentinelOne parser that
will be applied to the data field `message`. This can be used to ingest unstructured
data.

### `timeout = duration (optional)`

The delay after which events are sent, even if this results in fewer events
sent per message.

Defaults to `1min`.

## Examples

### Send events to SentinelOne Data Lake

```tql
to_sentinelone_data_lake "https://ingest.eu1.sentinelone.net",
  token=secret("sentinelone-token")
```

### Send additional session information

```tql
to_sentinelone_data_lake "https://ingest.eu1.sentinelone.net",
  token=secret("sentinelone-token"),
  session_info={
    serverHost: "Node 42",
    serverType: "Tenzir Node",
    region: "Planet Earth",
  }
```

### Send 'unstructured' data

The operator can also be used to send unstructured data to be parsed by SentinelOne.
For this, the operators input must contain a field `message` and a `parser` must
be specified in the `session_info`:

```tql
select message = this.print_ndjson();         // Format the entire event as JSON
to_sentinelone_data_lake "https://ingest.eu1.sentinelone.net",
  token=secret("sentinelone-token"),
  session_info={
    serverHost: "Node 42",
    parser: "json",                            // Have SentinelOne parse the data
  }
```

:::info[Ingest Costs]
SentinelOne charges per ingested _value_ byte in the events. This means that you get
charged for all bytes in `message`, including the keys, structural elements and
whitespace.

If you already have structured data in Tenzir, prefer sending structured data, as
you will only be charged for the values and one byte per key, as opposed to the
full keys and structural characters in `message`.
:::
