---
title: to_google_secops
category: Outputs/Events
example: 'to_google_secops …'
---
Sends unstructured events to a Google SecOps Chronicle instance.

```tql
to_google_secops customer_id=string, private_key=string, client_email=string,
                 log_type=string, log_text=string, [region=string,
                 timestamp=time, labels=record, namespace=string,
                 max_request_size=int, batch_timeout=duration]
```

## Description

The `to_google_secops` operator makes it possible to ingest events via the
[Google SecOps Chronicle unstructured logs ingestion
API](https://cloud.google.com/chronicle/docs/reference/ingestion-api#unstructuredlogentries).

### `customer_id = string`

The customer UUID to use.

### `private_key = string`

The private key to use for authentication. This corresponds to the `private_key`
in the SecOps collector config.

### `client_email = string`

The user email to use for authentication. This corresponds to the `client_email`
in the SecOps collector config.

### `log_type = string`

The log type of the events.

### `log_text = string`

The log text to send.

### `region = string (optional)`

[Regional
prefix](https://cloud.google.com/chronicle/docs/reference/ingestion-api#regional_endpoints)
for the Ingestion endpoint (`malachiteingestion-pa.googleapis.com`).

### `timestamp = time (optional)`

Optional timestamp field to attach to logs.

### `labels = record (optional)`

A record of labels to attach to the logs. For example, `{node: "Configured
Tenzir Node"}`.

### `namespace = string (optional)`

The namespace to use when ingesting.

Defaults to `tenzir`.

### `max_request_size = int (optional)`

The maximum number of bytes in the request payload.

Defaults to `1M`.

### `batch_timeout = duration (optional)`

The maximum duration to wait for new events before sending the request.

Defaults to `5s`.

## Examples

```tql
from {log: "31-Mar-2025 01:35:02.187 client 0.0.0.0#4238: query: tenzir.com IN A + (255.255.255.255)"}
to_google_secops \
  customer_id="00000000-0000-0000-00000000000000000",
  private_key=secret("my_secops_key"),
  client_email="somebody@example.com",
  log_text=log,
  log_type="BIND_DNS",
  region="europe"
```

## See Also

[`to_google_cloud_logging`](/reference/operators/to_google_cloud_logging)
