---
sidebar_custom_props:
  format:
    parser: true
---

# suricata

Reads [Suricata][suricata]'s [EVE JSON][eve-json] output. The parser is an alias
for [`json`](json.md) with the arguments:

- `--selector=event_type:suricata`
- `--ndjson`

## Synopsis

```
suricata [--expand-schema] [--raw] [--unnest-separator <separator>]
```

## Description

The [Suricata][suricata] network security monitor converts network
traffic into a stream of metadata events and provides a rule matching engine to
generate alerts. Suricata emits events in the [EVE JSON][eve-json] format. The
output is a single stream of events where the `event_type` field disambiguates
the event type.

[suricata]: https://suricata.io
[eve-json]: https://suricata.readthedocs.io/en/latest/output/eve/eve-json-output.html

Tenzir's [`json`](json.md) can handle EVE JSON correctly, but for the schema
names to match the value from the `event_type` field, you need to pass the
option `--selector=event_type:suricata`. The `suricata` parser does this by
default.

### Common Options (Parser)

The Suricata parser supports some of the common [schema inference options](formats.md#parser-schema-inference).

## Examples

Here's an `eve.log` sample:

```json
{"timestamp":"2011-08-12T14:52:57.716360+0200","flow_id":1031464864740687,"pcap_cnt":83,"event_type":"alert","src_ip":"147.32.84.165","src_port":1181,"dest_ip":"78.40.125.4","dest_port":6667,"proto":"TCP","alert":{"action":"allowed","gid":1,"signature_id":2017318,"rev":4,"signature":"ET CURRENT_EVENTS SUSPICIOUS IRC - PRIVMSG *.(exe|tar|tgz|zip)  download command","category":"Potentially Bad Traffic","severity":2},"flow":{"pkts_toserver":27,"pkts_toclient":35,"bytes_toserver":2302,"bytes_toclient":4520,"start":"2011-08-12T14:47:24.357711+0200"},"payload":"UFJJVk1TRyAjemFyYXNhNDggOiBzbXNzLmV4ZSAoMzY4KQ0K","payload_printable":"PRIVMSG #zarasa48 : smss.exe (368)\r\n","stream":0,"packet":"AB5J2xnDCAAntbcZCABFAABMGV5AAIAGLlyTIFSlTih9BASdGgvw0QvAxUWHdVAY+rCL4gAAUFJJVk1TRyAjemFyYXNhNDggOiBzbXNzLmV4ZSAoMzY4KQ0K","packet_info":{"linktype":1}}
{"timestamp":"2011-08-12T14:55:22.154618+0200","flow_id":2247896271051770,"pcap_cnt":775,"event_type":"dns","src_ip":"147.32.84.165","src_port":1141,"dest_ip":"147.32.80.9","dest_port":53,"proto":"UDP","dns":{"type":"query","id":553,"rrname":"irc.freenode.net","rrtype":"A","tx_id":0}}
{"timestamp":"2011-08-12T16:59:22.181050+0200","flow_id":472067367468746,"pcap_cnt":25767,"event_type":"fileinfo","src_ip":"74.207.254.18","src_port":80,"dest_ip":"147.32.84.165","dest_port":1046,"proto":"TCP","http":{"hostname":"www.nmap.org","url":"/","http_user_agent":"Mozilla/4.0 (compatible)","http_content_type":"text/html","http_method":"GET","protocol":"HTTP/1.1","status":301,"redirect":"http://nmap.org/","length":301},"app_proto":"http","fileinfo":{"filename":"/","magic":"HTML document, ASCII text","gaps":false,"state":"CLOSED","md5":"70041821acf87389e40ddcb092004184","sha1":"10395ab3566395ca050232d2c1a0dbad69eb5fd2","sha256":"2e4c462b3424afcc04f43429d5f001e4ef9a28143bfeefb9af2254b4df3a7c1a","stored":true,"file_id":1,"size":301,"tx_id":0}}
```

Import the log as follows:

```bash
tenzir 'read suricata | import' < eve.log
```

Instead of writing to a file, Suricata can also log to a UNIX domain socket that
Tenzir can then read from. This saves a filesystem round-trip. This requires the
following settings in your `suricata.yaml`:

```yaml
outputs:
  - eve-log:
    enabled: yes
    filetype: unix_stream
    filename: eve.sock
```

Suricata creates `eve.sock` upon startup. Thereafter, you can read from the
socket via netcat:

```bash
nc -vlkU eve.sock | tenzir 'read suricata | ...'
```

Or natively via this Tenzir pipeline:

```
from file --uds eve.sock read suricata
```
