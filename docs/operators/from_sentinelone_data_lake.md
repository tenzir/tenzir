---
title: from_sentinelone_data_lake
category: Inputs/Events
example: 'from_sentinelone_data_lake "https://…", …'
---

Retrieves PowerQuery results from SentinelOne Singularity Data Lake.

```tql
from_sentinelone_data_lake url:string, token=string, query=string,
                           [start=time, end=time]
```

## Description

The `from_sentinelone_data_lake` operator queries the [SentinelOne Data Lake
PowerQuery
API](https://support.sentinelone.com/hc/en-us/articles/360004195934-SentinelOne-API-Guide)
to retrieve security events based on a custom query.

The operator sends a single request to the `/api/powerQuery` endpoint with your
query and optional time range filters, then parses the tabular response into
events that can be processed by your pipeline.

### `url: string`

The base URL for your SentinelOne Data Lake instance.

Must be an `https://` URL.

:::info
Ensure you're using the correct regional endpoint for your SentinelOne instance
(e.g., `https://xdr.eu1.sentinelone.net` for EU).
:::

### `token = string`

The API token to use for authorization.

It is recommended to use the `secret()` function to securely reference
credentials:

### `query = string`

The PowerQuery query string to execute against the Data Lake.

PowerQuery is SentinelOne's query language for searching and analyzing data in
the Data Lake. Refer to the [SentinelOne PowerQuery
documentation](https://support.sentinelone.com/hc/en-us/articles/360004195934)
for query syntax details.

### `start = time (optional)`

The start time for the query time range.

When specified, only events with timestamps at or after this time will be
returned.

### `end = time (optional)`

The end time for the query time range.

When specified, only events with timestamps before this time will be returned.

## Examples

### Query threat events from the last 24 hours

```tql
from_sentinelone_data_lake "https://xdr.eu1.sentinelone.net",
  token=secret("sentinelone-token"),
  query="severity > 3"
```

### Query specific fields with time range filters

```tql
from_sentinelone_data_lake "https://xdr.eu1.sentinelone.net",
  token=secret("sentinelone-token"),
  query="severity > 3 | columns id",
  start=now()-10d,
  end=now()-3d
```

## See Also

[`to_sentinelone_data_lake`](/reference/operators/to_sentinelone_data_lake)
