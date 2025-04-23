# to_google_cloud_logging

Sends events to [Google Cloud Logging](https://cloud.google.com/logging).

```tql
to_google_cloud_logging resource=string, name=string, payload=string,
      [service_credentials=string, severity=string, timestamp=time,
      batch_timeout=duration, max_batch_size=int]
```

## Description

Sends events to [Google Cloud Logging](https://cloud.google.com/logging).

### `resource = string`

The type of the [monitored resource](https://cloud.google.com/logging/docs/reference/v2/rest/v2/MonitoredResource).

### `name = string`

The resource name for the associated log entry. Must be in one of the following
formats:

```
projects/[PROJECT_ID]/logs/[LOG_ID]
organizations/[ORGANIZATION_ID]/logs/[LOG_ID]
billingAccounts/[BILLING_ACCOUNT_ID]/logs/[LOG_ID]
folders/[FOLDER_ID]/logs/[LOG_ID]
```

[LOG_ID] must be URL-encoded within `name`. Example:
"organizations/1234567890/logs/cloudresourcemanager.googleapis.com%2Factivity".

[LOG_ID] must be less than 512 characters long and can only include the
following characters: upper and lower case alphanumeric characters,
forward-slash, underscore, hyphen, and period.

### `payload = string`

The log entry payload.

### `service_credentials = string (optional)`

Credentials to use if using a service account.

### `severity = string (optional)`

Severity of the event. Available severity levels are documented [here](https://cloud.google.com/logging/docs/reference/v2/rest/v2/LogEntry#LogSeverity).

Defaults to `default`.

### `timestamp = time (optional)`

Timestamp of the event.

### `batch_timeout = duration (optional)`

Maximum interval between sending the events.

Defaults to `5s`.

### `max_batch_size = int (optional)`

Maximum events to batch before sending.

Defaults to `1k`.

## Example

## Send logs using a service account

```tql
to_google_cloud_logging payload=log_text, 
      name="projects/PROJECT_ID/logs/LOG_ID", 
      resource=resource,
      service_credentials=file_contents("/path/to/credentials.json")
```
