---
description: Line-delimited JSON
---

# JSON

The `json` format in VAST represents [line-delimited
JSON](https://en.wikipedia.org/wiki/JSON_streaming#Line-delimited_JSON) objects
according to a specified schema. That is, one line corresponds to one event. The
object field names correspond to record field names.

JSON can express only a subset [VAST's type
system](../data-model/type-system.md). For example, VAST has
first-class support for IP addresses but they are strings in JSON. To get the
most out of your data and retain domain semantics, [define a schema for your
JSON objects](../../use/import/README.md#provide-a-schema-for-unknown-types).

## Parser

Consider the this example JSON file `data.json`:

```json
{"ts":"2011-08-15T03:36:16.748830Z","uid":"CKmcUPexVMCAAkl6h","id.orig_h":"210.87.254.81","id.orig_p":3,"id.resp_h":"147.32.84.165","id.resp_p":1,"proto":"icmp","conn_state":"OTH","missed_bytes":0,"orig_pkts":1,"orig_ip_bytes":56,"resp_pkts":0,"resp_ip_bytes":0,"tunnel_parents":[]}
{"ts":"2011-08-15T03:37:11.992151Z","uid":"CTluup1eVngpaS6e2i","id.orig_h":"147.32.84.165","id.orig_p":3923,"id.resp_h":"218.108.143.87","id.resp_p":22,"proto":"tcp","duration":3.006088,"orig_bytes":0,"resp_bytes":0,"conn_state":"S0","missed_bytes":0,"history":"S","orig_pkts":4,"orig_ip_bytes":192,"resp_pkts":0,"resp_ip_bytes":0,"tunnel_parents":[]}
{"ts":"2011-08-15T03:37:12.593013Z","uid":"C4KKBn3pbBOEm8XWOk","id.orig_h":"147.32.84.165","id.orig_p":3924,"id.resp_h":"218.108.189.111","id.resp_p":22,"proto":"tcp","duration":3.005948,"orig_bytes":0,"resp_bytes":0,"conn_state":"S0","missed_bytes":0,"history":"S","orig_pkts":4,"orig_ip_bytes":192,"resp_pkts":0,"resp_ip_bytes":0,"tunnel_parents":[]}
```

Import this file by specifying the schema `zeek.conn` that ships with VAST:

```bash
vast import --type=zeek.conn json < data.json
```

Passing a schema type via `--type` is necessary because the NDJSON objects are
just a collection of fields. VAST cannot know how to name the corresponding
record without an external hint. See the section on [mapping events to
schemas](../../use/import/README.md#map-events-to-schemas) for details.

## Printer

Use the `json` format to render a query result as JSON:

```bash
vast export json 'dest_ip in 147.32.84.0/24 || http_user_agent == /Google Update.*/' | jq
```

```json
{
  "timestamp": "2011-08-14T05:38:53.914038",
  "flow_id": 929669869939483,
  "pcap_cnt": null,
  "vlan": null,
  "in_iface": null,
  "src_ip": "147.32.84.165",
  "src_port": 138,
  "dest_ip": "147.32.84.255",
  "dest_port": 138,
  "proto": "UDP",
  "event_type": "flow",
  "community_id": null,
  "flow": {
    "pkts_toserver": 2,
    "pkts_toclient": 0,
    "bytes_toserver": 486,
    "bytes_toclient": 0,
    "start": "2011-08-12T12:53:47.928539",
    "end": "2011-08-12T12:53:47.928552",
    "age": 0,
    "state": "new",
    "reason": "timeout",
    "alerted": false
  },
  "app_proto": "failed"
}
{
  "timestamp": "2011-08-12T13:00:36.378914",
  "flow_id": 269421754201300,
  "pcap_cnt": 22569,
  "vlan": null,
  "in_iface": null,
  "src_ip": "147.32.84.165",
  "src_port": 1027,
  "dest_ip": "74.125.232.202",
  "dest_port": 80,
  "proto": "TCP",
  "event_type": "http",
  "community_id": null,
  "http": {
    "hostname": "cr-tools.clients.google.com",
    "url": "/service/check2?appid=%7B430FD4D0-B729-4F61-AA34-91526481799D%7D&appversion=1.3.21.65&applang=&machine=0&version=1.3.21.65&osversion=5.1&servicepack=Service%20Pack%202",
    "http_port": null,
    "http_user_agent": "Google Update/1.3.21.65;winhttp",
    "http_content_type": null,
    "http_method": "GET",
    "http_refer": null,
    "protocol": "HTTP/1.1",
    "status": null,
    "redirect": null,
    "length": 0
  },
  "tx_id": 0
}
```

### Flatten records

Providing `--flatten` embeds nested records:

```bash
vast export json --flatten '#type == /.*flow/' | jq
```

```json
{
  "timestamp": "2011-08-14T05:38:53.914038",
  "flow_id": 929669869939483,
  "pcap_cnt": null,
  "vlan": null,
  "in_iface": null,
  "src_ip": "147.32.84.165",
  "src_port": 138,
  "dest_ip": "147.32.84.255",
  "dest_port": 138,
  "proto": "UDP",
  "event_type": "flow",
  "community_id": null,
  "flow.pkts_toserver": 2,
  "flow.pkts_toclient": 0,
  "flow.bytes_toserver": 486,
  "flow.bytes_toclient": 0,
  "flow.start": "2011-08-12T12:53:47.928539",
  "flow.end": "2011-08-12T12:53:47.928552",
  "flow.age": 0,
  "flow.state": "new",
  "flow.reason": "timeout",
  "flow.alerted": false,
  "app_proto": "failed"
}
```

Note how the nested `flow` record of the first output is now flattened in the
(single) top-level record.

### Omit null fields

Add `--omit-nulls` to skip fields that are not set, i.e., would render as `null`
in JSON:

```bash
vast export json --omit-null '#type == /.*flow/' | jq
```

```json
{
  "timestamp": "2011-08-12T13:00:36.378914",
  "flow_id": 269421754201300,
  "pcap_cnt": 22569,
  "src_ip": "147.32.84.165",
  "src_port": 1027,
  "dest_ip": "74.125.232.202",
  "dest_port": 80,
  "proto": "TCP",
  "event_type": "http",
  "http": {
    "hostname": "cr-tools.clients.google.com",
    "url": "/service/check2?appid=%7B430FD4D0-B729-4F61-AA34-91526481799D%7D&appversion=1.3.21.65&applang=&machine=0&version=1.3.21.65&osversion=5.1&servicepack=Service%20Pack%202",
    "http_user_agent": "Google Update/1.3.21.65;winhttp",
    "http_method": "GET",
    "protocol": "HTTP/1.1",
    "length": 0
  },
  "tx_id": 0
}
```

Note that `pcap_cnt`, `vlan`, and other fields do not appear in the output,
although have existed in the query result above.

### Omitting empty fields

The options `--omit-empty-records`, `--omit-empty-lists`, and
`--omit-empty-maps` cause empty records, lists, and maps to be hidden from the
output respectively.

For example, consider this JSON object:

```json
{
  "foo": [],
  "bar": [
    null
  ],
  "baz": {
    "qux": {},
    "quux": null
  }
}
```

With `--omit-empty-records`, this same record will display like this:

```json
{
  "foo": [],
  "bar": [
    null
  ],
  "baz": {
    "quux": null
  }
}
```

With `--omit-empty-lists`, this same record will display like this:

```json
{
  "bar": [
    null
  ],
  "baz": {
    "qux": {},
    "quux": null
  }
}
```

With `--omit-empty-records` and `--omit-nulls`, this same record will display
like this:

```json
{
  "bar": []
}
```

::tip Shorthand Syntax
The option `--omit-empty` is short for `--omit-nulls --omit-empty-records
--omit-empty-lists --omit-empty-maps`.
:::

### Render durations as fractional seconds

For use cases that involve arithmetic on time durations after VAST provided the
data, the default representation of duration types as string with an SI suffix
is not convenient, e.g., rendering them as `"42 secs"` or `"1.5d"` would require
additional parsing.

For such cases, printing durations as fractional seconds (like a UNIX timestamp)
can come in handy. Pass `--numeric-durations` to the JSON export to perform this
transformation.
