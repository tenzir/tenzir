# azure_log_analytics

Sends events via the [Microsoft Azure Logs Ingestion API][api].

[api]: https://learn.microsoft.com/en-us/azure/azure-monitor/logs/logs-ingestion-api-overview

```tql
azure_log_analytics tenant_id=str, client_id=str, client_secret=str, dce=str, dcr=str, table=str
```

## Description

The `azure_log_analytics` operator makes it possible to upload events to
[supported tables][supported] or to [custom tables][custom] in Microsoft Azure.

[supported]: https://learn.microsoft.com/en-us/azure/azure-monitor/logs/logs-ingestion-api-overview#supported-tables
[custom]: https://learn.microsoft.com/en-us/azure/azure-monitor/logs/create-custom-table?tabs=azure-portal-1%2Cazure-portal-2%2Cazure-portal-3#create-a-custom-table

The operator handles access token retrievals by itself and updates that token
automatically, if needed.

### `tenant_id = str`

The Microsoft Directory (tenant) ID, written as
`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`.

### `client_id = str`

The Microsoft Application (client) ID, written as
`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`.

### `client_secret = str`

The client secret.

### `dce = str`

The data collection endpoint URL.

### `dcr = str`

The data collection rule ID, written as `dcr-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx`.

### `table = str`

The table to upload events to.

## Examples

Upload `custom.mydata` events to a table `Custom-MyData`:

```tql
export
where meta.name == "custom.mydata"
azure_log_analytics tenant_id="00a00a00-0a00-0a00-00aa-000aa0a0a000",
  client_id="000a00a0-0aa0-00a0-0000-00a000a000a0",
  client_secret="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  dce="https://my-stuff-a0a0.westeurope-1.ingest.monitor.azure.com",
  dcr="dcr-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  table="Custom-MyData"
```
