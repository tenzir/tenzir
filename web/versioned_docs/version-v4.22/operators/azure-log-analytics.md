---
sidebar_custom_props:
  operator:
    source: false
    sink: true
---

# azure-log-analytics

Sends events via the [Microsoft Azure Logs Ingestion API][api].

[api]: https://learn.microsoft.com/en-us/azure/azure-monitor/logs/logs-ingestion-api-overview

## Synopsis

```
azure-log-analytics --tenant-id <tenant-id> --client-id <client-id>
                    --client-secret <client-secret>
                    --dce <data-collection-endpoint>
                    --dcr <data-collection-rule-id>
                    --table <table-name>
```

## Description

The `azure-log-analytics` operator makes it possible to upload events to
[supported tables][supported] or to [custom tables][custom] in Microsoft Azure.

[supported]: https://learn.microsoft.com/en-us/azure/azure-monitor/logs/logs-ingestion-api-overview#supported-tables
[custom]: https://learn.microsoft.com/en-us/azure/azure-monitor/logs/create-custom-table?tabs=azure-portal-1%2Cazure-portal-2%2Cazure-portal-3#create-a-custom-table

The operator handles access token retrievals by itself and updates that token
automatically, if needed.

### `--tenant-id <tenant-id>`

The Microsoft Directory (tenant) ID, written as
`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`.

### `--client-id <client-id>`

The Microsoft Application (client) ID, written as
`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`.

### `--client-secret <client-secret>`

The client secret.

### `--dce <data-collection-endpoint>`

The data collection endpoint URL.

### `--dcr <data-collection-rule-id>`

The data collection rule ID, written as `dcr-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx`.

### `--table <table-name>`

The table to upload events to.

## Examples

Upload `custom.mydata` events to a table `Custom-MyData`:

```
export
| where #schema == "custom.mydata"
| azure-log-analytics --tenant-id 00a00a00-0a00-0a00-00aa-000aa0a0a000
  --client-id 000a00a0-0aa0-00a0-0000-00a000a000a0
  --client-secret xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
  --dce https://my-stuff-a0a0.westeurope-1.ingest.monitor.azure.com
  --dcr dcr-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
  --table "Custom-MyData"
```
