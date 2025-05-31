---
title: write_syslog
category: Printing
example: 'write_syslog'
---

Writes events as syslog.

```tql
write_syslog [facility=int, severity=int, timestamp=time, hostname=string,
              app_name=string, process_id=string, message_id=string,
              structured_data=record, message=string]
```

## Description

Writes events as [RFC 5424](https://datatracker.ietf.org/doc/html/rfc5424)
Syslog messages.

All options to the operator try to get values for the respective fields from the
same-named fields in the input events if unspecified.

### `facility = int (optional)`

Set the facility of the syslog.

Defaults to `1` if `null`.

### `severity = int (optional)`

Set the severity of the syslog.

Defaults to `6` if `null`.

### `timestamp = time (optional)`

Set the timestamp of the syslog.

### `hostname = string (optional)`

Set the hostname of the syslog.

### `app_name = string (optional)`

Set the application name of the syslog.

### `process_id = string (optional)`

Set the process id of the syslog.

### `message_id = string (optional)`

Set the message id of the syslog.

### `structured_data = record (optional)`

Set the structured data of the syslog.

### `message = string (optional)`

Set the message of the syslog.

## Examples

### Create a syslog manually

```tql
from {
  facility: 1,
  severity: 1,
  timestamp: now(),
  hostname: "localhost",
  structured_data: {
    origin: {
      key: "value",
    },
  },
  message: "Tenzir",
}
write_syslog
```

```log
<9>1 2025-03-31T13:28:55.971210Z localhost - - - [origin key="value"] Tenzir
```

## See Also

[`parse_syslog`](/reference/functions/parse_syslog),
[`print_syslog`](/reference/functions/print_syslog),
[`read_syslog`](/reference/operators/./read_syslog)
