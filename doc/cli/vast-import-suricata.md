The `suricata` import format consumes
[EVE](https://suricata.readthedocs.io/en/latest/output/eve/eve-json-output.html)
JSON logs from [Suricata](https://suricata-ids.org). EVE is output is
Suricata's unified format to log all types of activity as single stream of
[line-delimited JSON](https://en.wikipedia.org/wiki/JSON_streaming#Line-delimited_JSON).

For each log entry, VAST parses the field `event_type` to determine the
specific record type and then parses the data according to the known schema.

```sh
vast import suricata < eve.log
```
