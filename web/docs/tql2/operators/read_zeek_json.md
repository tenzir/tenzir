# read_zeek_json

Parse an incoming Zeek JSON stream into events.

```tql
read_zeek_json [schema_only=bool, raw=bool]
```

## Description

### `raw = bool (optional)`

Use only the raw types that are native to the parsed format. Fields that have a type
specified in the chosen `schema` will still be parsed according to the schema.

This means that JSON numbers will be parsed as numbers,
but every JSON string remains a string, unless the field is in the `schema`.

### `schema_only = bool (optional)`

When working with an existing schema, this option will ensure that the output
schema has *only* the fields from that schema. If the schema name is obtained via a `selector`
and it does not exist, this has no effect.

This option requires either `schema` or `selector` to be set.

## Examples

```json title="zeek.json"
{"__name":"sensor_10_0_0_2","_write_ts":"2020-02-26T04:00:03.734769Z","ts":"2020-02-26T03:40:03.724911Z","uid":"Cx3bf12iVwo5m7Gkd1","id.orig_h":"193.10.255.99","id.orig_p":6667,"id.resp_h":"141.9.40.50","id.resp_p":21,"proto":"tcp","duration":1196.975041,"orig_bytes":0,"resp_bytes":0,"conn_state":"S1","local_orig":false,"local_resp":true,"missed_bytes":0,"history":"Sh","orig_pkts":194,"orig_ip_bytes":7760,"resp_pkts":191,"resp_ip_bytes":8404}
{"_path":"_0_0_2","_write_ts":"2020-02-11T03:48:57.477193Z","ts":"2020-02-11T03:48:57.477193Z","uid":"Cpk0Nl33Zb5ZWLP1tc","id.orig_h":"185.100.59.59","id.orig_p":6667,"id.resp_h":"141.9.255.157","id.resp_p":8080,"proto":"tcp","note":"LongConnection::found","msg":"185.100.59.59 -> 141.9.255.157:8080/tcp remained alive for longer than 19m55s","sub":"1194.62","src":"185.100.59.59","dst":"141.9.255.157","p":8080,"peer_descr":"worker-02","actions":["Notice::ACTION_LOG"],"suppress_for":3600}
```

```tql title="Pipeline"
load "zeek.json"
read_zeek_json
```

```json title="Output"
{
  "__name": "sensor_10_0_0_2",
  "_write_ts": "2020-02-26T04:00:03.734769",
  "ts": "2020-02-26T03:40:03.724911",
  "uid": "Cx3bf12iVwo5m7Gkd1",
  "id": {
    "orig_h": "193.10.255.99",
    "orig_p": 6667,
    "resp_h": "141.9.40.50",
    "resp_p": 21
  },
  "proto": "tcp",
  "duration": 1196.975041,
  "orig_bytes": 0,
  "resp_bytes": 0,
  "conn_state": "S1",
  "local_orig": false,
  "local_resp": true,
  "missed_bytes": 0,
  "history": "Sh",
  "orig_pkts": 194,
  "orig_ip_bytes": 7760,
  "resp_pkts": 191,
  "resp_ip_bytes": 8404
}
{
  "_write_ts": "2020-02-11T03:48:57.477193",
  "ts": "2020-02-11T03:48:57.477193",
  "uid": "Cpk0Nl33Zb5ZWLP1tc",
  "id": {
    "orig_h": "185.100.59.59",
    "orig_p": 6667,
    "resp_h": "141.9.255.157",
    "resp_p": 8080
  },
  "proto": "tcp",
  "_path": "_0_0_2",
  "note": "LongConnection::found",
  "msg": "185.100.59.59 -> 141.9.255.157:8080/tcp remained alive for longer than 19m55s",
  "sub": "1194.62",
  "src": "185.100.59.59",
  "dst": "141.9.255.157",
  "p": 8080,
  "peer_descr": "worker-02",
  "actions": [
    "Notice::ACTION_LOG"
  ],
  "suppress_for": 3600
}
```
