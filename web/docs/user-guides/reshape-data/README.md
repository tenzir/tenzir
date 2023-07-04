---
sidebar_position: 1
---

# Reshape data

Tenzir comes with numerous [transformation
operators](../../operators/transformations/README.md) that do change the the
shape of their input and produce a new output. Here is a visual overview of
transformations that you can perform over a data frame:

![Reshaping Overview](reshaping.excalidraw.svg)

We'll walk through examples for each depicted operator, using the
[M57](../../user-guides.md) dataset. All examples assume that you have imported
the M57 sample data into a node, as explaiend in the
[quickstart](../../get-started.md#quickstart). We therefore start every pipeline
with [`export`](../../operators/sources/export.md).

## Filter rows with `where`

Use [`where`](../../operators/transformations/where.md) to filter rows in the
input with an [expression](../../language/expressions.md).

Filter by metadata using the `#schema` selector:

```bash
tenzir 'export | where #schema == "suricata.alert"'
```

<details>
<summary>Output</summary>

```json
{
  "timestamp": "2021-11-17T13:52:05.695469",
  "flow_id": 1868285155318879,
  "pcap_cnt": 143,
  "vlan": null,
  "in_iface": null,
  "src_ip": "14.1.112.177",
  "src_port": 38376,
  "dest_ip": "198.71.247.91",
  "dest_port": 123,
  "proto": "UDP",
  "event_type": "alert",
  "community_id": null,
  "alert": {
    "app_proto": null,
    "action": "allowed",
    "gid": 1,
    "signature_id": 2017919,
    "rev": 2,
    "signature": "ET DOS Possible NTP DDoS Inbound Frequent Un-Authed MON_LIST Requests IMPL 0x03",
    "category": "Attempted Denial of Service",
    "severity": 2,
    "source": {
      "ip": null,
      "port": null
    },
    "target": {
      "ip": null,
      "port": null
    },
    "metadata": {
      "created_at": [
        "2014_01_03"
      ],
      "updated_at": [
        "2014_01_03"
      ]
    }
  },
  "flow": {
    "pkts_toserver": 2,
    "pkts_toclient": 0,
    "bytes_toserver": 468,
    "bytes_toclient": 0,
    "start": "2021-11-17T13:52:05.695391",
    "end": null,
    "age": null,
    "state": null,
    "reason": null,
    "alerted": null
  },
  "payload": null,
  "payload_printable": null,
  "stream": null,
  "packet": null,
  "packet_info": {
    "linktype": null
  },
  "app_proto": "failed"
}
```

(Only 1 out of 19 shown.)

</details>

Or by using type and field extractors:

```bash
tenzir 'export | where 10.10.5.0/25 && (orig_bytes > 1 Mi || duration > 30 min)'
```

<details>
<summary>Output</summary>

```json
{
  "ts": "2021-11-19T06:30:30.918301",
  "uid": "C9T8pykxdsT7iSrc9",
  "id": {
    "orig_h": "10.10.5.101",
    "orig_p": 50046,
    "resp_h": "87.120.8.190",
    "resp_p": 9090
  },
  "proto": "tcp",
  "service": null,
  "duration": "5.09m",
  "orig_bytes": 1394538,
  "resp_bytes": 95179,
  "conn_state": "S1",
  "local_orig": null,
  "local_resp": null,
  "missed_bytes": 0,
  "history": "ShADad",
  "orig_pkts": 5046,
  "orig_ip_bytes": 1596390,
  "resp_pkts": 5095,
  "resp_ip_bytes": 298983,
  "tunnel_parents": null,
  "community_id": "1:UPodR2krvvXUGhc/NEL9kejd7FA=",
  "_write_ts": null
}
{
  "ts": "2021-11-19T07:05:44.694927",
  "uid": "ChnTjeQncxZrb0ZWg",
  "id": {
    "orig_h": "10.10.5.101",
    "orig_p": 50127,
    "resp_h": "87.120.8.190",
    "resp_p": 9090
  },
  "proto": "tcp",
  "service": null,
  "duration": "54.81s",
  "orig_bytes": 1550710,
  "resp_bytes": 97122,
  "conn_state": "S1",
  "local_orig": null,
  "local_resp": null,
  "missed_bytes": 0,
  "history": "ShADadww",
  "orig_pkts": 5409,
  "orig_ip_bytes": 1767082,
  "resp_pkts": 5477,
  "resp_ip_bytes": 316206,
  "tunnel_parents": null,
  "community_id": "1:aw0CtkT7YikUZWyqdHwgLhqJXxU=",
  "_write_ts": null
}
{
  "ts": "2021-11-19T06:30:15.910850",
  "uid": "CxuTEOgWv2Z74FCG6",
  "id": {
    "orig_h": "10.10.5.101",
    "orig_p": 50041,
    "resp_h": "87.120.8.190",
    "resp_p": 9090
  },
  "proto": "tcp",
  "service": null,
  "duration": "36.48m",
  "orig_bytes": 565,
  "resp_bytes": 507,
  "conn_state": "S1",
  "local_orig": null,
  "local_resp": null,
  "missed_bytes": 0,
  "history": "ShADad",
  "orig_pkts": 78,
  "orig_ip_bytes": 3697,
  "resp_pkts": 77,
  "resp_ip_bytes": 3591,
  "tunnel_parents": null,
  "community_id": "1:r337wYxbKPDv5Vkjoz3gGuld1bs=",
  "_write_ts": null
}
```

</details>

The above example extracts connections that either have sent more than 1 MiB or
lasted longer than 30 minutes.

:::info Extractors
Tenzir's expression language uses
[extractors](../../language/expressions.md#extractors) to locate fields of
interest.

If you don't know a field name but have concrete value, say an IP address,
you can apply a query over all schemas having fields of the `ip` type by writing
`:ip == 172.17.2.163`. The left-hand side of this predicate is a *type
extractor*, denoted by `:T` for a type `T`. The right-hand side is the IP
address literal `172.17.2.163`. You can go one step further and just write
`172.17.2.163` as query. Tenzir infers the literal type and makes a predicate
out of it, i.e.,. `x`, expands to `:T == x` where `T` is the type of `x`. Under
the hood, the predicate all possible fields with type address and yields a
logical OR.

In the above example, the value `10.10.5.0/25` actually expands to the
expression `:ip in 10.10.5.0/25 || :subnet == 10.10.5.0/25`, meaning, Tenzir
looks for any IP address field and performs a top-k prefix search, or any subnet
field where the value matches exactly.
:::

## Limit the output with `head` and `tail`

Use the [`head`](../../operators/transformations/head.md) and
[`tail`](../../operators/transformations/tail.md) operators to get the first or
last N records of the input.

The first 3 Zeek logs with IPs in 10.10.5.0/25:

```bash
tenzir '
  export
  | where #schema == /zeek.*/ && 10.10.5.0/25
  | head 3
  '
```

<details>
<summary>Output</summary>

```json
{
  "ts": "2021-11-19T04:28:06.186626",
  "cause": "violation",
  "analyzer_kind": "protocol",
  "analyzer_name": "GSSAPI",
  "uid": "CaHAWI2k6vB6BEOh65",
  "fuid": null,
  "id.orig_h": "10.10.5.101",
  "id.orig_p": 49847,
  "id.resp_h": "10.10.5.5",
  "id.resp_p": 49667,
  "id.vlan": null,
  "id.vlan_inner": null,
  "failure_reason": "Binpac exception: binpac exception: out_of_bound: ASN1EncodingMeta:more_len: 129 > 74",
  "failure_data": null
}
{
  "ts": "2021-11-19T04:28:06.186853",
  "cause": "violation",
  "analyzer_kind": "protocol",
  "analyzer_name": "GSSAPI",
  "uid": "CaHAWI2k6vB6BEOh65",
  "fuid": null,
  "id.orig_h": "10.10.5.101",
  "id.orig_p": 49847,
  "id.resp_h": "10.10.5.5",
  "id.resp_p": 49667,
  "id.vlan": null,
  "id.vlan_inner": null,
  "failure_reason": "Binpac exception: binpac exception: out_of_bound: ASN1EncodingMeta:more_len: 129 > 74",
  "failure_data": null
}
{
  "ts": "2021-11-19T04:28:06.187119",
  "cause": "violation",
  "analyzer_kind": "protocol",
  "analyzer_name": "GSSAPI",
  "uid": "CaHAWI2k6vB6BEOh65",
  "fuid": null,
  "id.orig_h": "10.10.5.101",
  "id.orig_p": 49847,
  "id.resp_h": "10.10.5.5",
  "id.resp_p": 49667,
  "id.vlan": null,
  "id.vlan_inner": null,
  "failure_reason": "Binpac exception: binpac exception: out_of_bound: ASN1EncodingMeta:more_len: 129 > 74",
  "failure_data": null
}
```

</details>

:::caution `tail` is blocking
The `tail` operator must wait for its entire input, whereas `head N` terminates
immediately after the first `N` records have arrived. Use `head` for
the majority of use cases and `tail` only when you have to.
:::

## Pick fields with `select` and `drop`

Use the [`select`](../../operators/transformations/select.md) operator to
restrict the output to a list of fields.

```bash
tenzir '
  export
  | where #schema == "suricata.alert"
  | select src_ip, dest_ip, severity, signature
  | head 3
  '
```

<details>
<summary>Output</summary>

```json
{
  "src_ip": "8.218.64.104",
  "dest_ip": "198.71.247.91",
  "alert": {
    "signature": "SURICATA UDPv4 invalid checksum",
    "severity": 3
  }
}
{
  "src_ip": "14.1.112.177",
  "dest_ip": "198.71.247.91",
  "alert": {
    "signature": "ET DOS Possible NTP DDoS Inbound Frequent Un-Authed MON_LIST Requests IMPL 0x03",
    "severity": 2
  }
}
{
  "src_ip": "167.94.138.20",
  "dest_ip": "198.71.247.91",
  "alert": {
    "signature": "SURICATA UDPv4 invalid checksum",
    "severity": 3
  }
}
```

</details>

Note that `select` does not reorder the input fields. Use
[`put`](../../operators/transformations/put.md) for adjusting the field order.

## Sample schemas with `taste`

The [`taste`](../../operators/transformations/taste.md) operator provides a
sample of the first N events of every unique schemas in the dataflow. For
example, to get 5 unique samples:

```bash
tenzir '
  export
  | taste 1
  | head 5
  '
```

<details>
<summary>Output</summary>

```json
{
  "ts": "2021-11-17T13:54:01.721755",
  "cause": "violation",
  "analyzer_kind": "protocol",
  "analyzer_name": "HTTP",
  "uid": "Cqp7rtziLijlnrxYf",
  "fuid": null,
  "id.orig_h": "87.251.64.137",
  "id.orig_p": 64078,
  "id.resp_h": "198.71.247.91",
  "id.resp_p": 80,
  "id.vlan": null,
  "id.vlan_inner": null,
  "failure_reason": "not a http request line",
  "failure_data": null
}
{
  "ts": "2021-11-17T13:33:53.748229",
  "ts_delta": "1.18m",
  "peer": "zeek",
  "gaps": 0,
  "acks": 2,
  "percent_lost": 0.0,
  "_write_ts": null
}
{
  "ts": "2021-11-17T13:32:46.565337",
  "uid": "C5luJD1ATrGDOcouW2",
  "id": {
    "orig_h": "89.248.165.145",
    "orig_p": 43831,
    "resp_h": "198.71.247.91",
    "resp_p": 52806
  },
  "proto": "tcp",
  "service": null,
  "duration": null,
  "orig_bytes": null,
  "resp_bytes": null,
  "conn_state": "S0",
  "local_orig": null,
  "local_resp": null,
  "missed_bytes": 0,
  "history": "S",
  "orig_pkts": 1,
  "orig_ip_bytes": 40,
  "resp_pkts": 0,
  "resp_ip_bytes": 0,
  "tunnel_parents": null,
  "community_id": "1:c/CLmyk4xRElyzleEMhJ4Baf4Gk=",
  "_write_ts": null
}
{
  "ts": "2021-11-18T08:05:09.134638",
  "uid": "Cwk5in34AvxJ8MurDh",
  "id": {
    "orig_h": "10.2.9.133",
    "orig_p": 49768,
    "resp_h": "10.2.9.9",
    "resp_p": 135
  },
  "rtt": "254.0us",
  "named_pipe": "135",
  "endpoint": "epmapper",
  "operation": "ept_map",
  "_write_ts": null
}
{
  "ts": "2021-11-18T08:00:21.486539",
  "uids": [
    "C4fKs01p1bdzLWvtQa"
  ],
  "client_addr": "192.168.1.102",
  "server_addr": "192.168.1.1",
  "mac": "00:0b:db:63:58:a6",
  "host_name": "m57-jo",
  "client_fqdn": "m57-jo.",
  "domain": "m57.biz",
  "requested_addr": null,
  "assigned_addr": "192.168.1.102",
  "lease_time": "59.4m",
  "client_message": null,
  "server_message": null,
  "msg_types": [
    "REQUEST",
    "ACK"
  ],
  "duration": "163.82ms",
  "trans_id": null,
  "_write_ts": null
}
```

</details>

## Add fields with `put` and `extend`

The [`extend`](../../operators/transformations/extend.md) operator appends new
fields to the input. The [`put`](../../operators/transformations/put.md)
operator does the same but drops all non-referenced fields.

Here is an example that generates host pairs plus service for Zeek connection
records. Think of the output is a the edges in graph, with the last column being
the edge type.

```bash
tenzir '
  export 
  | where #schema == "zeek.conn" && 10.10.5.0/25
  | put id.orig_h, id.resp_h, service
  | head
  | write tsv to stdout
  ' | column -t
```

<details>
<summary>Output</summary>

```
id.orig_h    id.resp_h     service
10.10.5.101  77.75.230.91  http
10.10.5.101  10.10.5.5     dns
10.10.5.101  10.10.5.5     dns
10.10.5.101  20.189.173.1  -
10.10.5.101  20.189.173.1  ssl
10.10.5.101  52.109.8.21   ssl
10.10.5.101  10.10.5.5     dns
10.10.5.101  10.10.5.5     dns
10.10.5.101  20.54.88.152  ssl
10.10.5.101  13.107.42.16  ssl
```

</details>

## Give schemas and fields new names with `rename`

The [`rename`](../../operators/transformations/rename.md) operator changes field
or schema names.

For example, rename the schema name and only print that afterwards:

```bash
tenzir '
  export
  | where #schema == "zeek.conn"
  | rename flow=:zeek.conn
  | put schema=#schema
  | head 1
  '
```

<details>
<summary>Output</summary>

```json
{
  "schema": "flow"
}
```

</details>

Rename a field:

```bash
tenzir '
  export
  | where #schema == "zeek.conn"
  | rename src=id.orig_h, dst=id.resp_h
  | put src, dst
  | head
  '
```

<details>
<summary>Output</summary>

```json
{"src": "89.248.165.145", "dst": "198.71.247.91"}
{"src": "128.14.134.170", "dst": "198.71.247.91"}
{"src": "60.205.181.213", "dst": "198.71.247.91"}
{"src": "31.44.185.120", "dst": "198.71.247.91"}
{"src": "91.223.67.180", "dst": "198.71.247.91"}
{"src": "185.73.126.70", "dst": "198.71.247.91"}
{"src": "183.136.225.42", "dst": "198.71.247.91"}
{"src": "71.6.135.131", "dst": "198.71.247.91"}
{"src": "172.104.138.223", "dst": "198.71.247.91"}
{"src": "185.94.111.1", "dst": "198.71.247.91"}
```

</details>

## Aggreate records with `summarize`

Use [`summarize`](../../operators/transformations/summarize.md) to group and
aggregate data.

```bash
tenzir '
  export
  | #schema == "suricata.alert"
  | summarize count=count(src_ip) by severity
  '
```

<details>
<summary>Output</summary>

```json
{
  "alert.severity": 1,
  "count": 134644
}
{
  "alert.severity": 2,
  "count": 26780
}
{
  "alert.severity": 3,
  "count": 179713
}
```

</details>

Suricata alerts with lower severity are more critical, with severity 1 being the
highest. Let's group by alert signature containing the substring `SHELLCODE`:

```bash
tenzir '
  export
  | where severity == 1
  | summarize count=count(src_ip) by signature
  | where /.*SHELLCODE.*/
  '
```

<details>
<summary>Output</summary>

```json
{
  "alert.signature": "ET SHELLCODE Possible Call with No Offset TCP Shellcode",
  "count": 2
}
{
  "alert.signature": "ET SHELLCODE Possible %41%41%41%41 Heap Spray Attempt",
  "count": 32
}
```

</details>

## Reorder records with `sort`

Use [`sort`](../../operators/transformations/sort.md) to arrange the output
records according to the order of a specific field.

```bash
tenzir '
  export
  | #schema == "suricata.alert"
  | summarize count=count(src_ip) by severity
  | sort count desc
  '
```

<details>
<summary>Output</summary>

```json
{
  "alert.severity": 3,
  "count": 179713
}
{
  "alert.severity": 1,
  "count": 134644
}
{
  "alert.severity": 2,
  "count": 26780
}
```

</details>

## Deduplicate with `unique`

Use [`unique`](../../operators/transformations/unique.md) to remove adjacent
duplicates. This operator comes in handy after a
[`sort`](../../operators/transformations/sort.md) that arranges the input so
that duplicates lay next to each other:

```bash
tenzir '
  export
  | where #schema == "zeek.kerberos"
  | put client
  | sort client
  | unique
  | head
  | write ssv to stdout
  '
```

<details>
<summary>Output</summary>

```
client
/NM
Administrator/EAGLEFREAKS
DEKSTOP-D9UMVWL$/SIMONSAYSGO.NET
DEKSTOP-VVCWQF5$/POLICYBARONS.COM
DESKTOP-1-PC$/MAXSUGER.COM
DESKTOP-1O7QAEA$/VICTORYPUNK.COM
DESKTOP-2P2S7WR$/VICTORYPUNK.COM
DESKTOP-30CQ14B$/FIRGREENTECH.COM
DESKTOP-3KI6Y6G$/JIGGEDYJACK.COM
DESKTOP-41SH6EJ$/DUCKKISSMIXER.COM
```

</details>

To compute a unique list of values per group, use the `distinct` aggregation
function in [`summarize`](../../operators/transformations/summarize.md):

```bash
tenzir '
  export
  | where #schema == "zeek.conn"
  | summarize sources=distinct(id.orig_h) by id.resp_h
  | rename destination=id.resp_h
  | head 3
  '
```

<details>
<summary>Output</summary>

```json
{
  "destination": "192.168.201.13",
  "sources": [
    "10.12.14.101",
    "10.12.17.101"
  ]
}
{
  "destination": "192.168.62.104",
  "sources": [
    "10.12.14.101",
    "10.12.17.101"
  ]
}
{
  "destination": "10.0.177.137",
  "sources": [
    "10.7.5.133"
  ]
}
```

</details>

## Profile the pipeline with `measure`

Use [`measure`](../../operators/transformations/measure.md) to profile the input
and replace it with runtime statistics.

For example, one way to compute a histogram over the entire persisted dataset is
to perform a full scan, replace the input with statistics, and then aggregate
them by schema:

```bash
tenzir '
  export
  | measure
  | summarize events=sum(events) by schema
  | sort events desc
  | write tsv to stdout
  ' | column -t
```

<details>
<summary>Output</summary>

```
schema               events
zeek.conn            583838
zeek.dns             90013
zeek.http            75290
zeek.telemetry       72853
zeek.ssl             42389
zeek.files           21922
suricata.alert       21749
zeek.dce_rpc         19585
zeek.analyzer        14755
zeek.notice          5871
zeek.weird           4617
zeek.reporter        3528
zeek.ocsp            2874
zeek.kerberos        2708
zeek.x509            2379
zeek.smtp            1967
zeek.smb_mapping     1584
zeek.stats           1409
zeek.ntp             1224
zeek.smb_files       1140
zeek.dpd             926
zeek.tunnel          606
zeek.sip             565
zeek.loaded_scripts  512
zeek.capture_loss    476
zeek.ntlm            429
zeek.pe              315
zeek.dhcp            267
zeek.snmp            132
zeek.traceroute      9
zeek.ftp             4
zeek.packet_filter   1
zeek.radius          1
```

</details>

The above pipeline performs a full scan over the data at the node. Tenzir's
pipeline optimizer pushes down predicate to avoid scans when possible. Consider
this pipeline:

```bash
tenzir '
  export
  | where *.id.orig_h in 10.0.0.0/8
  | write parquet to file local.parquet
  '
```

The optimizer coalesces the `export` and `where` operators such that
[expression](../../language/expressions.md) `*.id.orig_h in 10.0.0.0/8` gets
pushed down to the index and storage layer.
