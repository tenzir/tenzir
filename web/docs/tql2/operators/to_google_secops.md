# to_google_secops

Sends unstructued events to a Google SecOps Chronicle instance.

```tql
to_google_secops customer_id=string, config=string|record, log_type=string,
                log_text=string, [region=string, timestamp=time,
                labels=record, namespace=string]
```

## Description

The `to_google_secops` operator makes it possible to ingest events via the
[Google SecOps Chronicle unstructuredlogs ingestion
API](https://cloud.google.com/chronicle/docs/reference/ingestion-api#unstructuredlogentries).

### `customer_id = string`

The customer UUID to use.

### `config = string | record`

Path to the JSON collector config or a record with at least the keys
`private_key` and `client_email`.

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

## Examples

```tql
from {log: "31-Mar-2025 01:35:02.187 client 0.0.0.0#4238: query: tenzir.com IN A + (255.255.255.255)"}
to_google_secops customer_id="00000000-0000-0000-00000000000000000", config="../tenzir_ingestion.json", log_text=log, log_type="BIND_DNS", region="europe"
```
