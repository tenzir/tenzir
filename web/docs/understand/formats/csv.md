---
description: "Comma-separated values"
---

# CSV

The `csv` format represents [comma-separated values
(CSV)](https://en.wikipedia.org/wiki/Comma-separated_values) data in tabular
form. The first line in a CSV file is the header that describes the field names.
The remaining lines contain concrete values. One line corresponds to one event,
minus the header.

## Parser

To read CSV data, consider this piece of data:

```csv
ip,sn,str,rec.a,rec.b
1.2.3.4,10.0.0.0/8,foo bar,-4.2,/foo|bar/
```

As with [JSON](json), CSV still requires explicit [selection of a
schema](../../use/import/README.md#map-events-to-schemas) via `--type` to
understand the field types corresponding the the column headers. Here's the
corresponding schema:

```go title=test.schema
type test = record{
  ip: ip,
  sn: subnet,
  str: string,
  rec: record{
    a: double,
    b: pattern
  }
}
```

You can now ingest the CSV input as follows:

```bash
vast import -s test.schema csv < file.csv
```

## Printer

To render data as CSV, use the `export csv` command:

```bash
vast export csv 1.2.3.4 '#type == /suricata.*/'
```

```csv
type,timestamp,flow_id,pcap_cnt,vlan,in_iface,src_ip,src_port,dest_ip,dest_port,proto,event_type,community_id,http.hostname,http.url,http.http_port,http.http_user_agent,http.http_content_type,http.http_method,http.http_refer,http.protocol,http.status,http.redirect,http.length,tx_id
suricata.http,2011-08-12T13:00:36.378914,269421754201300,22569,,,147.32.84.165,1027,74.125.232.202,80,"TCP","http",,"cr-tools.clients.google.com","/service/check2?appid=%7B430FD4D0-B729-4F61-AA34-91526481799D%7D&appversion=1.3.21.65&applang=&machine=0&version=1.3.21.65&osversion=5.1&servicepack=Service%20Pack%202",,"Google Update/1.3.21.65;winhttp",,"GET",,"HTTP/1.1",,,0,0
type,timestamp,flow_id,pcap_cnt,vlan,in_iface,src_ip,src_port,dest_ip,dest_port,proto,event_type,community_id,flow.pkts_toserver,flow.pkts_toclient,flow.bytes_toserver,flow.bytes_toclient,flow.start,flow.end,flow.age,flow.state,flow.reason,flow.alerted,app_proto
suricata.flow,2011-08-14T05:38:53.914038,929669869939483,,,,147.32.84.165,138,147.32.84.255,138,"UDP","flow",,2,0,486,0,2011-08-12T12:53:47.928539,2011-08-12T12:53:47.928552,0,"new","timeout",F,"failed"
```

As with any variable-schema text output, it is complicated to interleave results
from different schemas. If VAST encouters a schema that differs from the
previously rendered one, it appends a new CSV header.

Nested records have dot-separated field names.
