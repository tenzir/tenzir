---
title: to_sentinel_one
category: Outputs/Events
example: 'to_sentinel_one "https://…", …'
---

Sends security events to SentinelOne's Data Lake via REST API.

```tql
to_sentinel_one endpoint=string, api_token=string, event_data=any,
                [activity_type=string, site_id=string, agent_id=string,
                max_request_size=int, batch_timeout=duration]
```

## Description

The `to_sentinel_one` operator makes it possible to ingest security events via 
the [SentinelOne Data Lake REST API](https://support.sentinelone.com/hc/en-us/articles/360004195934-SentinelOne-API-Guide).
Events are sent to the `/web/api/v2.1/activities` endpoint using Bearer token authentication.

The operator accumulates multiple events before sending them as a single batch
request. You can control the maximum request size via the `max_request_size` 
option and the timeout before sending all accumulated events via the 
`batch_timeout` option.

### `endpoint = string`

The SentinelOne management console endpoint URL. This should be the base URL
of your SentinelOne instance, for example:
- `https://your-instance.sentinelone.net`
- `https://your-instance.eu1.sentinelone.net`

### `api_token = string`

The API token for authentication with the SentinelOne REST API. This token
is used in the `Authorization: Bearer <token>` header for all requests.

You can generate API tokens from the SentinelOne management console under 
Settings > Users > API Token.

### `event_data = any`

The event data to send to SentinelOne's Data Lake. This parameter accepts any
Tenzir data type, which is automatically converted to JSON format internally
before being sent to the API. This allows you to send structured records,
strings, or any other Tenzir data type directly without manual conversion.

### `activity_type = string (optional)`

The type of activity being logged. This helps categorize the events within
SentinelOne's Data Lake for better organization and analysis.

Common activity types include:
- `"security_event"`
- `"network_activity"` 
- `"file_activity"`
- `"process_activity"`

Defaults to `"security_event"`.

### `site_id = string (optional)`

The SentinelOne site ID to associate with the events. This helps organize 
events by site within your SentinelOne deployment.

### `agent_id = string (optional)`

The SentinelOne agent ID to associate with the events. This links events
to specific agents in your deployment for correlation and analysis.

### `max_request_size = int (optional)`

The maximum number of bytes in the request payload. Large batches will be
split into multiple requests to stay under this limit.

Defaults to `1M`.

### `batch_timeout = duration (optional)`

The maximum duration to wait for new events before sending the accumulated
batch. This ensures events are delivered in a timely manner even when 
the volume is low.

Defaults to `5s`.

## Examples

### Send DNS logs to SentinelOne Data Lake

```tql
from {log: "31-Mar-2025 01:35:02.187 client 192.168.1.100#4238: query: suspicious-domain.com IN A + (203.0.113.1)"}
to_sentinel_one \
  endpoint="https://your-instance.sentinelone.net",
  api_token=secret("sentinel_one_token"),
  event_data=log,
  activity_type="network_activity",
  site_id="site-12345"
```

### Process Zeek connection logs for SentinelOne

```tql
load_file "conn.log"
read_zeek_tsv
where orig_bytes > 1000000  // Focus on large transfers
to_sentinel_one \
  endpoint="https://your-instance.eu1.sentinelone.net",
  api_token=secret("sentinel_api_token"),
  event_data=this,
  activity_type="network_activity",
  batch_timeout=10s
```

### Send file activity events with agent correlation

```tql
from {
  timestamp: 2025-03-31T10:30:00Z,
  file_path: "/etc/shadow", 
  process: "cat",
  user: "root",
  action: "read"
}
to_sentinel_one \
  endpoint="https://company.sentinelone.net",
  api_token=secret("s1_token"),
  event_data=f"File access: {process} by {user} accessed {file_path}",
  activity_type="file_activity",
  agent_id="agent-abc123",
  site_id="production-site"
```

## See Also

[`to_google_secops`](/reference/operators/to_google_secops),
[`to_splunk`](/reference/operators/to_splunk),
[`to_amazon_security_lake`](/reference/operators/to_amazon_security_lake)