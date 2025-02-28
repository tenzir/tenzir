# Suricata

[Suricata](https://suricata.io/) is network monitor with a rule matching engine
to detect threats. Use Tenzir to acquire, process, and store Suricata logs.

![Suricata](suricata.svg)

## Examples

### Ingest EVE JSON logs into a node

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

The `filetype` setting determines how you'd process the log file and defaults to
`regular`.

Onboard Suricata EVE JSON logs as follows:

```tql
load_file "/path/to/eve.json"
read_suricata
publish "suricata"
```

If your set `filetype` to `unix_stream`, you need to create a Unix domain socket
first, e.g., like this:

```bash
nc -U -l /tmp/eve.socket
```

Then use the same pipeline as above; Tenzir automatically detects the file type.
