# Suricata

[Suricata](https://suricata.io/) is network monitor with a rule matching engine
to detect threats.

Use Tenzir to acquire, process, and store Suricata logs.

## Ingest EVE JSON logs into a node

[EVE JSON](https://docs.suricata.io/en/latest/output/eve/eve-json-output.html)
is the log format in which Suricata generates events.

A typical Suricata configuration looks like this:

```yaml title=suricata.yaml
outputs:
  # Extensible Event Format (nicknamed EVE) event log in JSON format
  - eve-log:
      enabled: yes
      filetype: regular #regular|syslog|unix_dgram|unix_stream|redis
      filename: eve.json
```

The `filetype` setting determines how you'd process the log file.

### Import from a file

By default, Suricata uses the file type `regular`. Ingest into a node as
follows:

```
from /path/to/eve.json read suricata
| import
```

### Import from a Unix domain socket

If your `filetype` setting is `unix_stream`, you need to create a Unix domain
socket first, e.g., like this:

```bash
nc -U -l /tmp/eve.socket
```

Then you can use the same pipeline as above, since Tenzir's
[`file`](../connectors/file.md) automatically detects the file type.
