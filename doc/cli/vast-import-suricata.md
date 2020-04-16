The `import suricata` command format consumes [Eve
JSON](https://suricata.readthedocs.io/en/latest/output/eve/eve-json-output.html)
logs from [Suricata](https://suricata-ids.org). Eve JSON is Suricata's unified
format to log all types of activity as single stream of [line-delimited
JSON](https://en.wikipedia.org/wiki/JSON_streaming#Line-delimited_JSON).

For each log entry, VAST parses the field `event_type` to determine the
specific record type and then parses the data according to the known schema.

To add support for additional fields and event types, adapt the
`suricata.schema` file that ships with every installation of VAST.

```bash
vast import suricata < path/to/eve.log
```
