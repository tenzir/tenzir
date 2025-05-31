---
title: to_azure_log_analytics
category: Outputs/Events
example: 'to_azure_log_analytics tenant_id="...", workspace_id="..."'
---
Sends events to the [Microsoft Azure Logs Ingestion API][api].

[api]: https://learn.microsoft.com/en-us/azure/azure-monitor/logs/logs-ingestion-api-overview

```tql
to_azure_log_analytics tenant_id=string, client_id=string, client_secret=string,
      dce=string, dcr=string, stream=string, [batch_timeout=duration]
```

## Description

The `to_azure_log_analytics` operator makes it possible to upload events to
[supported tables][supported] or to [custom tables][custom] in Microsoft Azure.

[supported]: https://learn.microsoft.com/en-us/azure/azure-monitor/logs/logs-ingestion-api-overview#supported-tables
[custom]: https://learn.microsoft.com/en-us/azure/azure-monitor/logs/create-custom-table?tabs=azure-portal-1%2Cazure-portal-2%2Cazure-portal-3#create-a-custom-table

The operator handles access token retrievals by itself and updates that token
automatically, if needed.

### `tenant_id = string`

The Microsoft Directory (tenant) ID, written as
`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`.

### `client_id = string`

The Microsoft Application (client) ID, written as
`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`.

### `client_secret = string`

The client secret.

### `dce = string`

The data collection endpoint URL.

### `dcr = string`

The data collection rule ID, written as `dcr-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx`.

### `stream = string`

The stream to upload events to.

### `batch_timeout = duration`

Maximum duration to wait for new events before sending a batch.

Defaults to `5s`.

## Examples

### Upload `custom.mydata` events to the stream `Custom-MyData`

```tql
export
where @name == "custom.mydata"
to_azure_log_analytics tenant_id="00a00a00-0a00-0a00-00aa-000aa0a0a000",
  client_id="000a00a0-0aa0-00a0-0000-00a000a000a0",
  client_secret="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  dce="https://my-stuff-a0a0.westeurope-1.ingest.monitor.azure.com",
  dcr="dcr-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  stream="Custom-MyData"
```
