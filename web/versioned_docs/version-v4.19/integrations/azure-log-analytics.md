# Azure Log Analytics

Azure Monitor is Microsoft's cloud solution for collecting and analyzing logs
and system events. Azure Log Analytics is a part of Monitor and comes with an
[Logs Ingestion
API](https://learn.microsoft.com/en-us/azure/azure-monitor/logs/logs-ingestion-api-overview)
for sending data to tables within a [Log Analytics
workspace](https://learn.microsoft.com/en-us/azure/azure-monitor/logs/log-analytics-workspace-overview)
that which is a unique environment for log data, such as from [Microsoft
Sentinel](https://learn.microsoft.com/en-us/azure/sentinel/overview?tabs=azure-portal)
and [Microsoft Defender for
Cloud](https://learn.microsoft.com/en-us/azure/defender-for-cloud/defender-for-cloud-introduction).
Log Anlaytics tables are either pre-defined [standard
tables](https://learn.microsoft.com/en-us/azure/azure-monitor/logs/logs-ingestion-api-overview#supported-tables)
that follow a given schema, or user-defined [custom
tables](https://learn.microsoft.com/en-us/azure/azure-monitor/logs/create-custom-table#create-a-custom-table).

The diagram below illustrates the key components involved when sending data to a
Log Analytics table:

![Log Ingestion Workflow](azure-log-analytics.excalidraw.svg)

The [Data Collection Endpoint
(DCE)](https://learn.microsoft.com/en-us/azure/azure-monitor/essentials/data-collection-endpoint-overview)
is an authenticated HTTPS API endpoint that accepts HTTP POST requests with
events encoded as JSON arrays in the request body. The [Data Collection Rule
(DCR)](https://learn.microsoft.com/en-us/azure/azure-monitor/essentials/data-collection-rule-overview) offers optional transformation of arriving data and routes the data to a Log Analytics table.

:::tip Azure Monitor Setup Tutorial
The following use cases assume that you have already set up the Azure Monitor
side, for example, by following the [official
tutorial](https://learn.microsoft.com/en-us/azure/azure-monitor/logs/tutorial-logs-ingestion-portal)
that walks through for setting up a sample Entra application to authenticate
against the API, to create a DCE to receive data, to create a custom table in a
Log Analytics workspace and DCR to forward data to that table, and to give the
applciation the proper permissions to access the created DCE and DCR.
:::

## Send logs to custom table

Let's assume that you have the following CSV file that you want to send to a
custom table:

```csv title="users.csv"
user,age
Alice,42
Bob,43
Charlie,44
```

Assuming you have already [created a custom
table](https://learn.microsoft.com/en-us/azure/azure-monitor/logs/create-custom-table#create-a-custom-table)
called `Custom-Users`, you can send this file to the table as follows:

```
from users.csv
| azure-log-analytics
  --tenant-id "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
  --client-id "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
  --client-secret "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
  --dce "https://my-dce.westeurope-1.ingest.monitor.azure.com"
  --dcr "dcr-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
  --table "Custom-Users"
```
